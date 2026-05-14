# Pattaya Vibration Monitor — Web Dashboard

Static web dashboard that subscribes to a VTall-S203L vibration sensor via
MQTT-over-WebSocket. The UI mirrors the live Pattaya Smart Sanitary system.

**Architecture**

```
[VTall-S203L sensor] ── Modbus RTU ──► [Raspberry Pi 5: publisher.py] ──MQTT──► [MQTT broker] ──WSS──► [this dashboard]
```

## Local preview

```bash
python3 -m http.server 8080
```

Then open <http://localhost:8080/> and click the gear icon (⚙) on the right
rail to configure the broker URL and sensor ID.

## Default broker

`wss://broker.hivemq.com:8884/mqtt` — HiveMQ public broker, no auth.
Convenient for demos but **not for production**. Replace with your own
broker (Mosquitto on Pi + websocket listener, or a private cloud broker).

## Topics

- `pattaya/sensors/<sensor_id>/reading` — JSON reading, retained
- `pattaya/sensors/<sensor_id>/status` — `online` / `offline` (LWT)

## Files

- `index.html` · `styles.css` · `app.js` — the dashboard
- MQTT client: [mqtt.js](https://github.com/mqttjs/MQTT.js) via unpkg CDN

## Deploy

Push to `main` of any GitHub repo with Pages enabled, or any static host
(Netlify, Vercel, Cloudflare Pages, Surge). All files are static — no
build step.
