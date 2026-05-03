import hashlib
import hmac as _hmac
import json
import math
import os
import re
import ssl
import threading
import time
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


# ── SinricPro (Alexa via WebSocket) ───────────────────────────────────────────
SINRIC_APP_KEY    = os.environ.get("SINRIC_APP_KEY", "")
SINRIC_APP_SECRET = os.environ.get("SINRIC_APP_SECRET", "")
SINRIC_DEVICES    = {
    "EARLY":    os.environ.get("SINRIC_DEVICE_EARLY", ""),
    "NEAR":     os.environ.get("SINRIC_DEVICE_NEAR", ""),
    "CRITICAL": os.environ.get("SINRIC_DEVICE_CRITICAL", ""),
}

_sinric_ws   = None
_sinric_lock = threading.Lock()


def _sinric_sign(payload_dict):
    payload_str = json.dumps(payload_dict, separators=(",", ":"), sort_keys=True)
    return _hmac.new(
        SINRIC_APP_SECRET.encode(),
        payload_str.encode(),
        hashlib.sha256,
    ).hexdigest()


def sinric_motion(device_id, motion=True):
    """Envía evento de movimiento a SinricPro → dispara rutina de Alexa."""
    global _sinric_ws
    if not (SINRIC_APP_KEY and SINRIC_APP_SECRET and device_id):
        return
    payload = {
        "action":       "motion",
        "clientId":     SINRIC_APP_KEY,
        "createdAt":    int(time.time()),
        "deviceId":     device_id,
        "reachability": True,
        "type":         "event",
        "value":        {"motion": motion},
    }
    message = json.dumps({
        "payloadVersion":   2,
        "signatureVersion": 1,
        "signature":        {"HMAC": _sinric_sign(payload)},
        "payload":          payload,
    })
    with _sinric_lock:
        ws = _sinric_ws
    if ws:
        try:
            ws.send(message)
            print(f"[SINRIC] Enviado: {message[:300]}")
        except Exception as exc:
            print(f"[SINRIC] Error enviando evento: {exc}")
    else:
        print("[SINRIC] WebSocket no conectado aún")


def _sinric_connect_loop():
    """Hilo daemon que mantiene la conexión WebSocket con SinricPro."""
    import websocket as _wslib

    if not (SINRIC_APP_KEY and SINRIC_APP_SECRET):
        print("[SINRIC] Credenciales no configuradas — Alexa desactivado")
        return

    all_ids = ",".join(v for v in SINRIC_DEVICES.values() if v)
    print(f"[SINRIC] Iniciando conexión | devices={all_ids}")

    while True:
        global _sinric_ws

        def on_open(ws):
            global _sinric_ws
            with _sinric_lock:
                _sinric_ws = ws
            print("[SINRIC] WebSocket conectado ✓")

        def on_message(ws, msg):
            print(f"[SINRIC] Mensaje recibido: {msg[:200]}")
            try:
                data = json.loads(msg)
                if "timestamp" in data:
                    ws.send(json.dumps({"timestamp": data["timestamp"]}))
                    print("[SINRIC] Heartbeat respondido")
            except Exception:
                pass

        def on_error(ws, err):
            print(f"[SINRIC] Error WebSocket: {err}")

        def on_close(ws, code, msg):
            global _sinric_ws
            with _sinric_lock:
                _sinric_ws = None
            print(f"[SINRIC] Desconectado ({code})")

        try:
            ws = _wslib.WebSocketApp(
                "wss://ws.sinric.pro",
                header={
                    "appkey":     SINRIC_APP_KEY,
                    "deviceids":  all_ids,
                    "platform":   "Python",
                    "sdkversion": "2.0.0",
                },
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )
            ws.run_forever(ping_interval=60, ping_timeout=10)
        except Exception as exc:
            print(f"[SINRIC] Excepción: {exc}")

        with _sinric_lock:
            _sinric_ws = None
        print("[SINRIC] Reconectando en 15s...")
        time.sleep(15)


# Iniciar conexión con SinricPro al arrancar el servidor
threading.Thread(target=_sinric_connect_loop, daemon=True).start()


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
            sinric_motion(device_id, motion=True)
            time.sleep(5)
            sinric_motion(device_id, motion=False)  # reset para próxima vez
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
    sinric_ok   = _sinric_ws is not None
    return jsonify({
        "active":          state["active"],
        "alert_state":     state["alert_state"],
        "route":           route,
        "next_waypoint":   next_wp,
        "waypoint_index":  wp_idx,
        "url":             state["share_url"],
        "alexa_connected": sinric_ok,
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
    """Prueba manual: /test-alexa/EARLY  /test-alexa/NEAR  /test-alexa/CRITICAL"""
    device_id = SINRIC_DEVICES.get(action.upper(), "")
    if not device_id:
        return jsonify({"error": f"Acción inválida. Usa EARLY, NEAR o CRITICAL"}), 400
    def _trigger():
        sinric_motion(device_id, motion=True)
        time.sleep(5)
        sinric_motion(device_id, motion=False)
    threading.Thread(target=_trigger, daemon=True).start()
    return jsonify({"status": "disparado", "action": action.upper(), "device": device_id})


# ── Arranque ───────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
