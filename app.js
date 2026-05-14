// =========================================================
// Pattaya Vibration Dashboard — MQTT subscriber (WebSocket).
// Subscribes to pattaya/sensors/<id>/reading + .../status
// =========================================================

// ---------- Config (persisted in localStorage) ----------
const DEFAULTS = {
    broker:   "wss://broker.hivemq.com:8884/mqtt",
    sensorId: "wwtp-nong-yai",
    username: "",
    password: "",
};
const STORE_KEY = "pattaya.vib.cfg.v1";

function loadCfg() {
    try {
        const raw = localStorage.getItem(STORE_KEY);
        if (!raw) return { ...DEFAULTS };
        return { ...DEFAULTS, ...JSON.parse(raw) };
    } catch { return { ...DEFAULTS }; }
}
function saveCfg(cfg) {
    try { localStorage.setItem(STORE_KEY, JSON.stringify(cfg)); } catch {}
}

let cfg = loadCfg();

// ---------- Thresholds (ISO 10816-3 Class II inspired) ----------
const TH_VEL  = { warn: 2.8,  danger: 7.1  };
const TH_ACC  = { warn: 5.0,  danger: 10.0 };
const TH_DISP = { warn: 50,   danger: 100  };
const TH_TEMP = { warn: 60,   danger: 80   };

function classify(value, th) {
    if (value == null || isNaN(value)) return "offline";
    if (value >= th.danger) return "danger";
    if (value >= th.warn)   return "warn";
    return "ok";
}
const STATUS_LABEL = { ok:"ปกติ", warn:"ค่อนข้างวิกฤต", danger:"วิกฤต", offline:"ออฟไลน์" };
const STATUS_RANK  = { ok:0, warn:1, danger:2, offline:-1 };

const $ = id => document.getElementById(id);

function setBadge(id, status) {
    const el = $(id); if (!el) return;
    el.className = "badge " + status;
    el.textContent = STATUS_LABEL[status];
}
function setCardStatus(cardId, status) {
    const c = $(cardId); if (!c) return;
    c.classList.remove("ok","warn","danger");
    if (status !== "offline") c.classList.add(status);
}
function fmt(v, digits=2) { return (v == null || isNaN(v)) ? "—" : Number(v).toFixed(digits); }
function fmtTime(iso) {
    if (!iso) return "—";
    try {
        const d = new Date(iso);
        return d.toLocaleTimeString("th-TH", {hour12:false}) + " · " +
               d.toLocaleDateString("th-TH", {year:"numeric",month:"short",day:"numeric"});
    } catch { return iso; }
}
function escapeHtml(s) {
    return String(s).replace(/[<>&"']/g, c => ({"<":"&lt;",">":"&gt;","&":"&amp;","\"":"&quot;","'":"&apos;"}[c]));
}
function brokerHost(url) {
    try { return new URL(url).host; } catch { return url; }
}

// ---------- MQTT client ----------
let client = null;
let lastReading = null;
let sensorStatus = "unknown";   // "online" | "offline" | "unknown"
const history = [];              // up to 120 points (~2 min at 1 Hz)

function setConn(state, text) {
    const pill = $("conn-pill");
    pill.className = "status-pill " + state;
    $("conn-text").textContent = text;
}

function connect() {
    if (client) {
        try { client.end(true); } catch {}
        client = null;
    }
    setConn("warn", "กำลังเชื่อมต่อ MQTT…");
    $("broker-host").textContent = brokerHost(cfg.broker);
    $("sensor-id-display").textContent = cfg.sensorId;
    $("topic-display").textContent = `pattaya/sensors/${cfg.sensorId}/reading`;

    const opts = {
        clientId: "pattaya-dash-" + Math.random().toString(16).slice(2, 10),
        clean: true,
        reconnectPeriod: 3000,
        connectTimeout: 10000,
    };
    if (cfg.username) opts.username = cfg.username;
    if (cfg.password) opts.password = cfg.password;

    try {
        client = mqtt.connect(cfg.broker, opts);
    } catch (e) {
        setConn("err", "URL ไม่ถูกต้อง: " + e.message);
        return;
    }

    const tReading = `pattaya/sensors/${cfg.sensorId}/reading`;
    const tStatus  = `pattaya/sensors/${cfg.sensorId}/status`;

    client.on("connect", () => {
        setConn("ok", "ออนไลน์ (MQTT)");
        client.subscribe([tReading, tStatus], { qos: 0 }, err => {
            if (err) setConn("err", "Subscribe ไม่สำเร็จ: " + err.message);
        });
    });
    client.on("reconnect", () => setConn("warn", "กำลังเชื่อมต่อใหม่…"));
    client.on("close",     () => setConn("warn", "การเชื่อมต่อปิด"));
    client.on("offline",   () => setConn("err",  "ออฟไลน์"));
    client.on("error",     e  => setConn("err",  "ข้อผิดพลาด: " + e.message));

    client.on("message", (topic, msg) => {
        if (topic === tReading) {
            try { handleReading(JSON.parse(msg.toString())); }
            catch (e) { console.warn("bad reading payload", e); }
        } else if (topic === tStatus) {
            handleStatus(msg.toString());
        }
    });
}

function handleStatus(state) {
    sensorStatus = state;
    if (state === "offline") {
        setConn("err", "เซนเซอร์ออฟไลน์ (LWT)");
        ["card-vel","card-acc","card-disp","card-temp"].forEach(c => setCardStatus(c, "offline"));
        ["badge-vel","badge-acc","badge-disp","badge-temp"].forEach(b => setBadge(b, "offline"));
    } else if (state === "online" && client && client.connected) {
        setConn("ok", "ออนไลน์ (MQTT)");
    }
}

function handleReading(d) {
    lastReading = d;
    if (d?.timestamp) $("updated-at").textContent = fmtTime(d.timestamp);

    if (!d || !d.ok) {
        const msg = d?.error ? "ผิดพลาด: " + d.error : "ไม่มีข้อมูล";
        setConn("err", msg);
        ["card-vel","card-acc","card-disp","card-temp"].forEach(c => setCardStatus(c, "offline"));
        ["badge-vel","badge-acc","badge-disp","badge-temp"].forEach(b => setBadge(b, "offline"));
        return;
    }

    // velocity
    const vx = d.velocity?.x, vy = d.velocity?.y, vz = d.velocity?.z;
    const vMax = Math.max(vx||0, vy||0, vz||0);
    $("vel-x").textContent = fmt(vx);
    $("vel-y").textContent = fmt(vy);
    $("vel-z").textContent = fmt(vz);
    $("vel-max").innerHTML = `${fmt(vMax)}<span class="stat-unit">มม./วินาที</span>`;
    const vStat = classify(vMax, TH_VEL);
    setBadge("badge-vel", vStat); setCardStatus("card-vel", vStat);

    // acceleration
    const ax = d.acceleration?.x, ay = d.acceleration?.y, az = d.acceleration?.z;
    const aMax = Math.max(ax||0, ay||0, az||0);
    $("acc-x").textContent = fmt(ax);
    $("acc-y").textContent = fmt(ay);
    $("acc-z").textContent = fmt(az);
    $("acc-max").innerHTML = `${fmt(aMax)}<span class="stat-unit">ม./วินาที²</span>`;
    const aStat = classify(aMax, TH_ACC);
    setBadge("badge-acc", aStat); setCardStatus("card-acc", aStat);

    // displacement
    const dx = d.displacement?.x, dy = d.displacement?.y, dz = d.displacement?.z;
    const dMax = Math.max(dx||0, dy||0, dz||0);
    $("disp-x").textContent = fmt(dx, 0);
    $("disp-y").textContent = fmt(dy, 0);
    $("disp-z").textContent = fmt(dz, 0);
    $("disp-max").innerHTML = `${fmt(dMax, 0)}<span class="stat-unit">µm</span>`;
    const dStat = classify(dMax, TH_DISP);
    setBadge("badge-disp", dStat); setCardStatus("card-disp", dStat);

    // temperature
    const t = d.temperature;
    $("temp-val").innerHTML = `${fmt(t, 1)}<span class="stat-unit">°C</span>`;
    const tStat = classify(t, TH_TEMP);
    setBadge("badge-temp", tStat); setCardStatus("card-temp", tStat);

    // peaks + kurtosis
    const peakVel = Math.max(d.max_velocity?.x||0, d.max_velocity?.y||0, d.max_velocity?.z||0);
    const peakAcc = Math.max(d.max_acceleration?.x||0, d.max_acceleration?.y||0, d.max_acceleration?.z||0);
    $("peak-vel").textContent = fmt(peakVel) + " mm/s";
    $("peak-acc").textContent = fmt(peakAcc) + " m/s²";
    const kVel = Math.max(d.velocity_kurtosis?.x||0, d.velocity_kurtosis?.y||0, d.velocity_kurtosis?.z||0);
    const kAcc = Math.max(d.acceleration_kurtosis?.x||0, d.acceleration_kurtosis?.y||0, d.acceleration_kurtosis?.z||0);
    $("kurt-vel").textContent = fmt(kVel);
    $("kurt-acc").textContent = fmt(kAcc);

    if (d.version != null) $("sensor-fw").textContent = "v" + d.version;
    if (d.serial_number != null) $("sensor-sn").textContent = d.serial_number;

    // overall
    const overall = ["vel","acc","disp","temp"]
        .map(k => $("badge-" + k).className.split(" ").pop())
        .reduce((acc, s) => STATUS_RANK[s] > STATUS_RANK[acc] ? s : acc, "ok");
    $("overall-badge").innerHTML = `<span class="badge ${overall}">สถานะรวม: ${STATUS_LABEL[overall]}</span>`;

    // faults
    renderFaults(d.decoded || {});

    // history buffer
    history.push({
        t: d.timestamp, vx: vx||0, vy: vy||0, vz: vz||0,
    });
    while (history.length > 120) history.shift();
    renderChart();
}

function renderFaults(decoded) {
    const make = (items, danger=false) => {
        if (!items || !items.length) return `<li class="empty">ไม่พบความผิดปกติ</li>`;
        return items.map(s => `<li${danger ? ' class="danger"' : ''}>${escapeHtml(s)}</li>`).join("");
    };
    $("fault-x").innerHTML  = make(decoded.fault_x, true);
    $("fault-y").innerHTML  = make(decoded.fault_y, true);
    $("fault-z").innerHTML  = make(decoded.fault_z, true);
    $("warn-temp").innerHTML = make(decoded.temp_alarm);
    $("warn-x").innerHTML   = make(decoded.warn_x);
    $("warn-y").innerHTML   = make(decoded.warn_y);
    $("warn-z").innerHTML   = make(decoded.warn_z);
}

function renderChart() {
    const svg = $("chart");
    const W = 1200, H = 280;
    const PAD_L = 44, PAD_R = 12, PAD_T = 16, PAD_B = 30;
    const innerW = W - PAD_L - PAD_R;
    const innerH = H - PAD_T - PAD_B;

    if (!history.length) {
        svg.innerHTML = `<text x="${W/2}" y="${H/2}" text-anchor="middle" fill="#94a3b8" font-size="13">รอข้อมูลจาก MQTT…</text>`;
        return;
    }

    const allVals = history.flatMap(p => [p.vx, p.vy, p.vz]).filter(v => v != null);
    let maxY = Math.max(...allVals, TH_VEL.warn);
    maxY = Math.ceil(maxY * 1.15);
    const N = history.length;

    const x = i => PAD_L + (i / Math.max(1, N - 1)) * innerW;
    const y = v => PAD_T + innerH - (v / maxY) * innerH;

    let parts = "";
    const ticks = [0, maxY*0.25, maxY*0.5, maxY*0.75, maxY];
    ticks.forEach(v => {
        const yy = y(v);
        parts += `<line class="grid-line" x1="${PAD_L}" x2="${W - PAD_R}" y1="${yy}" y2="${yy}"/>`;
        parts += `<text class="axis-text" x="${PAD_L - 6}" y="${yy + 4}" text-anchor="end">${v.toFixed(1)}</text>`;
    });
    parts += `<text class="axis-label-y" x="6" y="${PAD_T - 2}">mm/s</text>`;
    if (TH_VEL.warn < maxY)   parts += `<line class="threshold-warn"   x1="${PAD_L}" x2="${W - PAD_R}" y1="${y(TH_VEL.warn)}"   y2="${y(TH_VEL.warn)}"/>`;
    if (TH_VEL.danger < maxY) parts += `<line class="threshold-danger" x1="${PAD_L}" x2="${W - PAD_R}" y1="${y(TH_VEL.danger)}" y2="${y(TH_VEL.danger)}"/>`;

    const path = (key, cls) => {
        const pts = history.map((p, i) => `${i === 0 ? "M" : "L"}${x(i).toFixed(1)},${y(p[key]||0).toFixed(1)}`).join(" ");
        return `<path class="${cls}" d="${pts}"/>`;
    };
    parts += path("vx", "line-x");
    parts += path("vy", "line-y");
    parts += path("vz", "line-z");

    const xTicks = 5;
    for (let i = 0; i < xTicks; i++) {
        const idx = Math.floor((N - 1) * (i / (xTicks - 1)));
        const p = history[idx];
        if (!p?.t) continue;
        const t = new Date(p.t);
        const label = t.toLocaleTimeString("th-TH", {hour12:false, hour:"2-digit", minute:"2-digit", second:"2-digit"});
        parts += `<text class="axis-text" x="${x(idx)}" y="${H - 10}" text-anchor="middle">${label}</text>`;
    }
    svg.innerHTML = parts;
}

// ---------- Settings modal ----------
const modal = $("settings-modal");
function openSettings() {
    $("cfg-broker").value = cfg.broker;
    $("cfg-sensor").value = cfg.sensorId;
    $("cfg-username").value = cfg.username || "";
    $("cfg-password").value = cfg.password || "";
    modal.hidden = false;
}
function closeSettings() { modal.hidden = true; }

$("btn-settings").addEventListener("click", openSettings);
$("conn-pill").addEventListener("click", openSettings);
$("settings-close").addEventListener("click", closeSettings);
$("settings-cancel").addEventListener("click", closeSettings);
modal.addEventListener("click", e => { if (e.target === modal) closeSettings(); });

$("settings-save").addEventListener("click", () => {
    const next = {
        broker:   $("cfg-broker").value.trim() || DEFAULTS.broker,
        sensorId: $("cfg-sensor").value.trim() || DEFAULTS.sensorId,
        username: $("cfg-username").value.trim(),
        password: $("cfg-password").value,
    };
    if (!/^wss?:\/\//i.test(next.broker)) {
        alert("Broker URL ต้องขึ้นต้นด้วย wss:// หรือ ws://");
        return;
    }
    cfg = next;
    saveCfg(cfg);
    history.length = 0;
    closeSettings();
    connect();
});

document.querySelectorAll(".brokers-quick button").forEach(b => {
    b.addEventListener("click", () => {
        if (b.dataset.preset === "hivemq")     $("cfg-broker").value = "wss://broker.hivemq.com:8884/mqtt";
        if (b.dataset.preset === "mosquitto")  $("cfg-broker").value = "wss://test.mosquitto.org:8081/";
        if (b.dataset.preset === "local")      $("cfg-broker").value = "ws://" + location.hostname + ":9001";
    });
});

// ---------- Sidebar interactions (matching live app) ----------
document.querySelectorAll(".nav-toggle").forEach(btn => {
    btn.addEventListener("click", () => {
        const expanded = btn.getAttribute("aria-expanded") === "true";
        btn.setAttribute("aria-expanded", String(!expanded));
        const children = btn.parentElement.querySelector(".nav-children");
        if (children) children.hidden = expanded;
        btn.querySelector(".nav-caret").textContent = expanded ? "⌄" : "˄";
    });
});
$("btn-collapse").addEventListener("click", () => {
    $("sidebar").classList.toggle("collapsed");
});

// ---------- Boot ----------
connect();
