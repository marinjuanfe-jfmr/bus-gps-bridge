import json
import math
import os
import re
import ssl
import threading
import time
import uuid as _uuid
from datetime import datetime, timezone, timedelta

import paho.mqtt.client as mqtt
import requests
from flask import Flask, jsonify, request

app = Flask(__name__)

# ── Configuración base ─────────────────────────────────────────────────────────
HOME_LAT        = float(os.environ.get("HOME_LAT", "4.646992"))
HOME_LON        = float(os.environ.get("HOME_LON", "-74.108089"))
N8N_WEBHOOK_URL = os.environ.get("N8N_WEBHOOK_URL", "")

COLOMBIA_TZ = timezone(timedelta(hours=-5))

# ── Rutas con waypoints ────────────────────────────────────────────────────────
# Cada waypoint se activa cuando el bus entra a su radio (en metros).
# Los waypoints se activan en ORDEN SECUENCIAL — no se puede saltar al siguiente
# sin haber pasado el anterior.
#
#   action   → lo que se envía a N8N (maneja EARLY, NEAR, CRITICAL, DONE)
#   radius_m → distancia al waypoint para considerarlo "alcanzado"

ROUTES = {
    "morning": [
        {
            "name":     "A_mañana",
            "lat":      4.642746,
            "lon":      -74.113247,
            "radius_m": 350,
            "action":   "EARLY",
        },
        {
            "name":     "B_común",
            "lat":      4.643184,
            "lon":      -74.110882,
            "radius_m": 350,
            "action":   "NEAR",
        },
        {
            "name":     "Casa",
            "lat":      HOME_LAT,
            "lon":      HOME_LON,
            "radius_m": 500,
            "action":   "CRITICAL",
        },
    ],
    "afternoon": [
        {
            "name":     "A_tarde",
            "lat":      4.638135,
            "lon":      -74.114025,
            "radius_m": 350,
            "action":   "EARLY",
        },
        {
            "name":     "B_común",
            "lat":      4.643184,
            "lon":      -74.110882,
            "radius_m": 350,
            "action":   "NEAR",
        },
        {
            "name":     "Casa",
            "lat":      HOME_LAT,
            "lon":      HOME_LON,
            "radius_m": 500,
            "action":   "CRITICAL",
        },
    ],
}


def detect_route():
    """Determina la ruta según la hora colombiana: antes de las 13:00 → mañana."""
    hour = datetime.now(COLOMBIA_TZ).hour
    return "morning" if hour < 13 else "afternoon"


# ── Estado global ──────────────────────────────────────────────────────────────
state = {
    "active":          False,
    "share_url":       None,
    "route":           "morning",
    "waypoint_index":  0,        # índice del próximo waypoint a alcanzar
    "alert_state":     "FAR",    # FAR → EARLY → NEAR → CRITICAL → DONE
    "min_dist_home":   float("inf"),  # mínima distancia a casa (para detectar DONE)
    "mqtt_client":     None,
    "creds":           {},
    "lock":            threading.Lock(),
}

# ── Helpers ────────────────────────────────────────────────────────────────────

def haversine(lat1, lon1, lat2, lon2):
    R = 6_371_000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


PATTERNS = {
    "host":     r'const\s+hostmqtt\s*=\s*["\']([^"\']+)["\']',
    "port":     r'const\s+portmqtt\s*=\s*(\d+)',
    "user":     r'const\s+usermqtt\s*=\s*["\']([^"\']+)["\']',
    "password": r'const\s+passmqtt\s*=\s*["\']([^"\']+)["\']',
    "topic":    r'const\s+topicmqtt\s*=\s*["\']([^"\']+)["\']',
}

def extract_mqtt_credentials(share_url):
    resp = requests.get(share_url, timeout=15)
    resp.raise_for_status()
    html = resp.text
    creds = {}
    for key, pattern in PATTERNS.items():
        m = re.search(pattern, html)
        if not m:
            raise ValueError(f"No se encontró '{key}' en la página. ¿El enlace expiró?")
        creds[key] = m.group(1)
    creds["port"] = int(creds["port"])
    return creds


def notify_n8n(action, distance_m, waypoint_name=""):
    if not N8N_WEBHOOK_URL:
        print(f"[WARN] N8N_WEBHOOK_URL no configurado — acción: {action}, dist: {distance_m}m")
        return
    try:
        requests.post(
            N8N_WEBHOOK_URL,
            json={
                "action":       action,
                "distance_m":   round(distance_m),
                "waypoint":     waypoint_name,
            },
            timeout=10,
        )
        print(f"[N8N] Notificación enviada: {action} ({distance_m:.0f}m) @ {waypoint_name}")
    except Exception as exc:
        print(f"[ERROR] Fallo al notificar N8N: {exc}")


# ── SinricPro (Alexa via Portal API) ─────────────────────────────────────────
SINRIC_EMAIL   = os.environ.get("SINRIC_EMAIL", "")
SINRIC_PASSWORD = os.environ.get("SINRIC_PASSWORD", "")
SINRIC_DEVICES = {
    "EARLY":    os.environ.get("SINRIC_DEVICE_EARLY", ""),
    "NEAR":     os.environ.get("SINRIC_DEVICE_NEAR", ""),
    "CRITICAL": os.environ.get("SINRIC_DEVICE_CRITICAL", ""),
}

_sinric_jwt       = None
_sinric_jwt_time  = 0
_sinric_jwt_lock  = threading.Lock()
JWT_TTL_SECS      = 6 * 24 * 3600   # refrescar tras 6 días (JWT dura 7)


def _sinric_login():
    """Login en portal SinricPro → obtiene JWT. Devuelve token o None."""
    global _sinric_jwt, _sinric_jwt_time
    if not (SINRIC_EMAIL and SINRIC_PASSWORD):
        print("[SINRIC] Sin credenciales (SINRIC_EMAIL / SINRIC_PASSWORD)")
        return None
    try:
        # SinricPro usa HTTP Basic Auth; el JWT viene en el header Authorization
        # de la respuesta (no en el body)
        resp = requests.post(
            "https://portal.sinric.pro/api/v1/auth",
            auth=(SINRIC_EMAIL, SINRIC_PASSWORD),
            timeout=15,
        )
        resp.raise_for_status()
        # JWT en header de respuesta: "Bearer <token>"
        auth_header = resp.headers.get("authorization", "")
        if auth_header.startswith("Bearer "):
            token = auth_header[len("Bearer "):]
        else:
            # fallback: buscar en el body
            body = resp.json()
            token = (
                body.get("token")
                or body.get("accessToken")
                or (body.get("data") or {}).get("token")
            )
        if token:
            with _sinric_jwt_lock:
                _sinric_jwt      = token
                _sinric_jwt_time = time.time()
            print("[SINRIC] Login OK — JWT obtenido ✓")
            return token
        print(f"[SINRIC] Login: JWT no encontrado. Status={resp.status_code}")
    except Exception as exc:
        print(f"[SINRIC] Login fallido: {exc}")
    return None


def _invalidate_sinric_jwt():
    global _sinric_jwt, _sinric_jwt_time
    with _sinric_jwt_lock:
        _sinric_jwt      = None
        _sinric_jwt_time = 0


def _get_sinric_jwt():
    """Devuelve JWT válido; hace login si falta o está por expirar."""
    with _sinric_jwt_lock:
        jwt = _sinric_jwt
        age = time.time() - _sinric_jwt_time
    if jwt and age < JWT_TTL_SECS:
        return jwt
    return _sinric_login()


def sinric_trigger(device_id, detected=True):
    """Dispara evento de movimiento en SinricPro via portal API → rutina Alexa."""
    if not device_id:
        return
    jwt = _get_sinric_jwt()
    if not jwt:
        print(f"[SINRIC] Sin JWT — no se puede disparar dispositivo {device_id}")
        return

    value = '{"state":"detected"}' if detected else '{"state":"not detected"}'
    params = {
        "clientId":  "portal",
        "messageId": str(_uuid.uuid4()),
        "type":      "event",
        "action":    "motion",
        "createdAt": int(time.time()),
        "value":     value,
    }
    headers = {"Authorization": f"Bearer {jwt}"}

    try:
        resp = requests.get(
            f"https://portal.sinric.pro/api/v1/devices/{device_id}/action",
            params=params,
            headers=headers,
            timeout=10,
        )
        if resp.status_code == 401:
            # JWT expirado — invalidar caché, refrescar y reintentar una vez
            print("[SINRIC] 401 — JWT expirado, refrescando...")
            _invalidate_sinric_jwt()
            jwt = _sinric_login()
            if jwt:
                headers = {"Authorization": f"Bearer {jwt}"}
                params["messageId"] = str(_uuid.uuid4())
                resp = requests.get(
                    f"https://portal.sinric.pro/api/v1/devices/{device_id}/action",
                    params=params,
                    headers=headers,
                    timeout=10,
                )
        state_str = "DETECTED" if detected else "CLEAR"
        print(f"[SINRIC] Portal {state_str} → {resp.status_code} {resp.text[:120]}")
    except Exception as exc:
        print(f"[SINRIC] Portal API error: {exc}")


# ── Lógica MQTT ────────────────────────────────────────────────────────────────

def on_connect(client, userdata, flags, reason_code, properties=None):
    if reason_code == 0:
        creds = state.get("creds", {})
        topic = creds.get("topic", "")
        client.subscribe(topic, qos=1)
        print(f"[MQTT] Conectado, suscrito a {topic}")
    else:
        print(f"[MQTT] Conexión fallida: {reason_code}")


def on_message(client, userdata, msg):
    with state["lock"]:
        if not state["active"]:
            return
        route       = state["route"]
        wp_idx      = state["waypoint_index"]
        alert_st    = state["alert_state"]

    # ── Parsear posición GPS ───────────────────────────────────────────────
    try:
        raw = msg.payload.decode("utf-8").strip()
        start = next((i for i, c in enumerate(raw) if c in "{["), 0)
        raw = raw[start:]
        decoder = json.JSONDecoder()
        data, _ = decoder.raw_decode(raw)
        if isinstance(data, list):
            data = data[0]
        lat = float(data.get("latitud") or data.get("lat"))
        lon = float(data.get("longitud") or data.get("lng") or data.get("lon"))
    except Exception as exc:
        print(f"[MQTT] Error parseando mensaje: {exc}")
        return

    route_wps = ROUTES[route]

    # ── Máquina de estados por waypoints ──────────────────────────────────

    if alert_st == "CRITICAL":
        # Bus llegó a casa — esperar que se aleje para emitir DONE
        dist_home = haversine(lat, lon, HOME_LAT, HOME_LON)
        with state["lock"]:
            state["min_dist_home"] = min(state["min_dist_home"], dist_home)
            min_d = state["min_dist_home"]
        print(f"[GPS] lat={lat:.5f} lon={lon:.5f} dist_casa={dist_home:.0f}m (mín={min_d:.0f}m) estado=CRITICAL")
        if dist_home > min_d + 300:
            _transition("DONE", dist_home, "")
            stop_monitoring()

    elif wp_idx < len(route_wps):
        wp = route_wps[wp_idx]
        dist_to_wp = haversine(lat, lon, wp["lat"], wp["lon"])
        print(
            f"[GPS] lat={lat:.5f} lon={lon:.5f} "
            f"→ {wp['name']}: {dist_to_wp:.0f}m (umbral {wp['radius_m']}m) "
            f"estado={alert_st}"
        )
        if dist_to_wp <= wp["radius_m"]:
            _transition(wp["action"], dist_to_wp, wp["name"])
            with state["lock"]:
                state["waypoint_index"] += 1
                if wp["action"] == "CRITICAL":
                    state["min_dist_home"] = dist_to_wp


def _transition(new_state, dist, waypoint_name):
    with state["lock"]:
        state["alert_state"] = new_state
    print(f"[ALERT] → {new_state} ({dist:.0f}m) @ {waypoint_name}")
    notify_n8n(new_state, dist, waypoint_name)

    # ── Alexa via SinricPro ────────────────────────────────────────────────
    device_id = SINRIC_DEVICES.get(new_state, "")
    if device_id:
        def _alexa_trigger():
            sinric_trigger(device_id, detected=True)
            time.sleep(5)
            sinric_trigger(device_id, detected=False)  # reset para próxima vez
        threading.Thread(target=_alexa_trigger, daemon=True).start()


def on_disconnect(client, userdata, flags, reason_code, properties=None):
    if state["active"]:
        print(f"[MQTT] Desconexión inesperada ({reason_code}), reconectando...")


# ── Control de sesión ──────────────────────────────────────────────────────────

def start_monitoring(share_url, route):
    stop_monitoring()

    creds = extract_mqtt_credentials(share_url)
    state["creds"] = creds

    c = mqtt.Client(transport="websockets")
    c.username_pw_set(creds["user"], creds["password"])
    c.tls_set(cert_reqs=ssl.CERT_NONE)
    c.tls_insecure_set(True)
    c.on_connect    = on_connect
    c.on_message    = on_message
    c.on_disconnect = on_disconnect

    with state["lock"]:
        state["active"]         = True
        state["share_url"]      = share_url
        state["route"]          = route
        state["waypoint_index"] = 0
        state["alert_state"]    = "FAR"
        state["min_dist_home"]  = float("inf")
        state["mqtt_client"]    = c

    def run():
        c.connect(creds["host"], creds["port"], keepalive=60)
        c.loop_forever()

    t = threading.Thread(target=run, daemon=True)
    t.start()

    wp0 = ROUTES[route][0]
    print(f"[SESSION] Monitoreo iniciado | ruta={route} | primer waypoint={wp0['name']} | url={share_url}")


def stop_monitoring():
    with state["lock"]:
        c = state.get("mqtt_client")
        state["active"]         = False
        state["mqtt_client"]    = None
        state["alert_state"]    = "FAR"
        state["waypoint_index"] = 0
        state["min_dist_home"]  = float("inf")
        state["share_url"]      = None

    if c:
        try:
            c.disconnect()
        except Exception:
            pass
    print("[SESSION] Monitoreo detenido")


# ── Endpoints Flask ────────────────────────────────────────────────────────────

@app.route("/health", methods=["GET"])
def health():
    return "OK", 200


@app.route("/status", methods=["GET"])
def status():
    with state["lock"]:
        wp_idx = state["waypoint_index"]
        route  = state["route"]
    route_wps   = ROUTES.get(route, [])
    next_wp     = route_wps[wp_idx]["name"] if wp_idx < len(route_wps) else "—"
    with _sinric_jwt_lock:
        sinric_ok = _sinric_jwt is not None
    return jsonify({
        "active":         state["active"],
        "alert_state":    state["alert_state"],
        "route":          route,
        "next_waypoint":  next_wp,
        "waypoint_index": wp_idx,
        "url":            state["share_url"],
        "alexa_jwt_ok":   sinric_ok,
    })


@app.route("/start", methods=["POST"])
def api_start():
    data  = request.get_json(silent=True) or {}
    url   = data.get("url", "").strip()
    route = data.get("route", "").strip() or detect_route()

    if not url:
        return jsonify({"error": "Falta el campo 'url'"}), 400
    if "colfenixgps.co/share/" not in url:
        return jsonify({"error": "URL no parece un enlace de ColfenixGPS"}), 400
    if route not in ROUTES:
        return jsonify({"error": f"Ruta inválida. Usa 'morning' o 'afternoon'"}), 400

    try:
        start_monitoring(url, route)
        wp0 = ROUTES[route][0]
        return jsonify({
            "status":           "started",
            "route":            route,
            "first_waypoint":   wp0["name"],
            "url":              url,
        })
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/stop", methods=["POST"])
def api_stop():
    stop_monitoring()
    return jsonify({"status": "stopped"})


@app.route("/test-alexa/<action>", methods=["GET"])
def test_alexa(action):
    """Prueba solo Alexa: /test-alexa/EARLY  /test-alexa/NEAR  /test-alexa/CRITICAL"""
    device_id = SINRIC_DEVICES.get(action.upper(), "")
    if not device_id:
        return jsonify({"error": "Acción inválida. Usa EARLY, NEAR o CRITICAL"}), 400
    def _trigger():
        sinric_trigger(device_id, detected=True)
        time.sleep(5)
        sinric_trigger(device_id, detected=False)
    threading.Thread(target=_trigger, daemon=True).start()
    return jsonify({"status": "disparado", "action": action.upper(), "device": device_id})


@app.route("/test-alert/<action>", methods=["GET"])
def test_alert(action):
    """Prueba completa (Telegram + Alexa): /test-alert/EARLY  /NEAR  /CRITICAL  /DONE"""
    valid = {"EARLY", "NEAR", "CRITICAL", "DONE"}
    action = action.upper()
    if action not in valid:
        return jsonify({"error": f"Acción inválida. Usa: {', '.join(valid)}"}), 400
    # Usa exactamente el mismo código que cuando el bus pasa por un waypoint real
    threading.Thread(
        target=_transition,
        args=(action, 300, "test"),
        daemon=True,
    ).start()
    return jsonify({"status": "disparado", "action": action, "note": "Telegram + Alexa en camino"})


# ── Arranque ───────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
