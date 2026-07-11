"use strict";

// ais-tracker dashboard: a Leaflet map fed by the WebSocket position feed, with
// a search box, a vessel detail sidebar, toggleable overlays, and an alert pane.
// No framework, no build step — just the REST + WS API the tracker binary serves.

const map = L.map("map", { worldCopyJump: true }).setView([20, 0], 3);
L.tileLayer("https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png", {
  attribution: '&copy; OpenStreetMap &copy; CARTO',
  subdomains: "abcd",
  maxZoom: 18,
}).addTo(map);

// markers holds one Leaflet marker per MMSI currently on screen.
const markers = new Map();
const vesselLayer = L.layerGroup().addTo(map);

const statusEl = document.getElementById("status");

async function api(path) {
  const res = await fetch(path);
  if (!res.ok) throw new Error(`${path}: ${res.status}`);
  const body = await res.json();
  return body.data;
}

function fmtTime(s) {
  if (!s) return "—";
  return new Date(s).toLocaleString();
}

function el(tag, cls, text) {
  const e = document.createElement(tag);
  if (cls) e.className = cls;
  if (text != null) e.textContent = text;
  return e;
}

function vesselIcon(cog) {
  const rot = cog == null ? 0 : cog;
  return L.divIcon({
    className: "vessel-marker",
    html: `<div style="transform:rotate(${rot}deg);width:0;height:0;border-left:5px solid transparent;border-right:5px solid transparent;border-bottom:12px solid #4aa8ff;"></div>`,
    iconSize: [10, 12],
    iconAnchor: [5, 6],
  });
}

let socket;

function connect() {
  const proto = location.protocol === "https:" ? "wss:" : "ws:";
  socket = new WebSocket(`${proto}//${location.host}/ws/positions`);

  socket.onopen = () => {
    setStatus("live", "live");
    sendViewport();
  };
  socket.onclose = () => {
    setStatus("disconnected", "down");
    setTimeout(connect, 2000);
  };
  socket.onerror = () => socket.close();
  socket.onmessage = (ev) => {
    try {
      updateVessel(JSON.parse(ev.data));
    } catch (_) { /* ignore malformed frame */ }
  };
}

function setStatus(text, cls) {
  statusEl.textContent = text;
  statusEl.className = "status " + (cls || "");
}

// sendViewport subscribes the socket to the current map bounds, so the server
// only pushes fixes we can actually see.
function sendViewport() {
  if (!socket || socket.readyState !== WebSocket.OPEN) return;
  const b = map.getBounds();
  socket.send(JSON.stringify({
    type: "subscribe",
    bbox: [b.getWest(), b.getSouth(), b.getEast(), b.getNorth()],
  }));
}

map.on("moveend", sendViewport);

function updateVessel(p) {
  let m = markers.get(p.mmsi);
  if (m) {
    m.setLatLng([p.lat, p.lon]);
    m.setIcon(vesselIcon(p.cog));
  } else {
    m = L.marker([p.lat, p.lon], { icon: vesselIcon(p.cog) });
    m.on("click", () => showVessel(p.mmsi));
    m.addTo(vesselLayer);
    markers.set(p.mmsi, m);
  }
}

const searchForm = document.getElementById("search-form");
const searchInput = document.getElementById("search-input");
const searchResults = document.getElementById("search-results");

searchForm.addEventListener("submit", (e) => {
  e.preventDefault();
  runSearch(searchInput.value.trim());
});

async function runSearch(q) {
  if (!q) { searchResults.classList.add("hidden"); return; }
  const results = await api(`/api/vessels?search=${encodeURIComponent(q)}&limit=25`);
  searchResults.innerHTML = "";
  if (!results || results.length === 0) {
    searchResults.appendChild(el("li", "muted", "No matches."));
  } else {
    for (const r of results) {
      const li = el("li");
      li.appendChild(el("div", null, r.Name || `MMSI ${r.MMSI}`));
      li.appendChild(el("div", "sub", `${r.MMSI}  ${r.CallSign || ""}  ${r.FlagCountry || ""}`));
      li.addEventListener("click", () => {
        searchResults.classList.add("hidden");
        showVessel(r.MMSI);
      });
      searchResults.appendChild(li);
    }
  }
  searchResults.classList.remove("hidden");
}

const sidebar = document.getElementById("sidebar");
const sidebarBody = document.getElementById("sidebar-body");
document.getElementById("sidebar-close").addEventListener("click", () => sidebar.classList.add("hidden"));

async function showVessel(mmsi) {
  sidebar.classList.remove("hidden");
  sidebarBody.innerHTML = "";
  sidebarBody.appendChild(el("p", "muted", "Loading…"));

  let v;
  try {
    v = await api(`/api/vessels/${mmsi}`);
  } catch (err) {
    sidebarBody.innerHTML = "";
    sidebarBody.appendChild(el("p", "muted", "Not found."));
    return;
  }

  sidebarBody.innerHTML = "";
  sidebarBody.appendChild(el("h3", null, v.name || `MMSI ${v.mmsi}`));

  const kv = el("dl", "kv");
  addKV(kv, "MMSI", v.mmsi);
  addKV(kv, "IMO", v.imo || "—");
  addKV(kv, "Call sign", v.call_sign || "—");
  addKV(kv, "Flag", v.flag_country || "—");
  addKV(kv, "First seen", fmtTime(v.first_seen_at));
  addKV(kv, "Last seen", fmtTime(v.last_seen_at));
  sidebarBody.appendChild(kv);

  if (v.last_position) {
    const p = v.last_position;
    map.panTo([p.lat, p.lon]);
    addSection(sidebarBody, "Last position");
    const pkv = el("dl", "kv");
    addKV(pkv, "Lat / lon", `${p.lat.toFixed(4)}, ${p.lon.toFixed(4)}`);
    addKV(pkv, "SOG / COG", `${fmtNum(p.sog)} kn / ${fmtNum(p.cog)}°`);
    addKV(pkv, "Reported", fmtTime(p.reported_at));
    sidebarBody.appendChild(pkv);
  }

  addSection(sidebarBody, "Sanctions");
  if (v.sanctions && v.sanctions.length) {
    sidebarBody.appendChild(el("span", "tag sanction", "listed"));
    const ul = el("ul", "list");
    for (const s of v.sanctions) {
      ul.appendChild(el("li", null, `${s.program} · ${s.reference} (score ${s.match_score.toFixed(2)})`));
    }
    sidebarBody.appendChild(ul);
  } else {
    sidebarBody.appendChild(el("span", "tag ok", "clear"));
  }

  if (v.operators && v.operators.length) {
    addSection(sidebarBody, "Ownership");
    const ul = el("ul", "list");
    for (const o of v.operators) ul.appendChild(el("li", null, o.canonical));
    sidebarBody.appendChild(ul);
  }

  if (v.anomaly) {
    addSection(sidebarBody, "Anomaly");
    sidebarBody.appendChild(el("p", null, `score ${v.anomaly.score.toFixed(2)} (${v.anomaly.method})`));
  }

  // Similar vessels, best effort — absent embeddings simply yield nothing.
  try {
    const sim = await api(`/api/vessels/${mmsi}/similar?limit=5`);
    if (sim && sim.length) {
      addSection(sidebarBody, "Moves like");
      const ul = el("ul", "list");
      for (const s of sim) {
        const li = el("li");
        const a = el("a", null, s.Name || `MMSI ${s.MMSI}`);
        a.href = "#";
        a.addEventListener("click", (e) => { e.preventDefault(); showVessel(s.MMSI); });
        li.appendChild(a);
        li.appendChild(el("span", "sub", `  ${(s.Similarity * 100).toFixed(0)}%`));
        ul.appendChild(li);
      }
      sidebarBody.appendChild(ul);
    }
  } catch (_) { /* similarity is optional */ }
}

function fmtNum(n) { return n == null ? "—" : n.toFixed(1); }
function addKV(dl, k, v) { dl.appendChild(el("dt", null, k)); dl.appendChild(el("dd", null, String(v))); }
function addSection(parent, title) { parent.appendChild(el("div", "section-title", title)); }

const portLayer = L.layerGroup();
const geofenceLayer = L.layerGroup();

document.getElementById("toggle-ports").addEventListener("change", async (e) => {
  if (e.target.checked) {
    const ports = await api("/api/ports?limit=500");
    for (const p of ports) {
      L.circleMarker([p.lat, p.lon], { radius: 3, color: "#f0a020", weight: 1 })
        .bindTooltip(`${p.name} (${p.country})`)
        .addTo(portLayer);
    }
    portLayer.addTo(map);
  } else {
    portLayer.clearLayers();
    map.removeLayer(portLayer);
  }
});

document.getElementById("toggle-geofences").addEventListener("change", async (e) => {
  if (e.target.checked) {
    const fences = await api("/api/geofences");
    for (const f of fences) {
      if (!f.polygon) continue;
      L.geoJSON(f.polygon, { style: { color: "#4aa8ff", weight: 1, fillOpacity: 0.08 } })
        .bindTooltip(f.name)
        .addTo(geofenceLayer);
    }
    geofenceLayer.addTo(map);
  } else {
    geofenceLayer.clearLayers();
    map.removeLayer(geofenceLayer);
  }
});

const alertsList = document.getElementById("alerts-list");

async function refreshAlerts() {
  try {
    const since = new Date(Date.now() - 24 * 3600 * 1000).toISOString();
    const alerts = await api(`/api/alerts?since=${encodeURIComponent(since)}&limit=50`);
    alertsList.innerHTML = "";
    if (!alerts || alerts.length === 0) {
      alertsList.appendChild(el("li", "muted", "No alerts in the last 24h."));
      return;
    }
    for (const a of alerts) {
      const li = el("li");
      li.appendChild(el("span", "type " + a.type, a.type));
      const label = el("span");
      label.textContent = `MMSI ${a.mmsi}`;
      label.style.cursor = "pointer";
      label.addEventListener("click", () => showVessel(a.mmsi));
      li.appendChild(label);
      li.appendChild(el("span", "when", fmtTime(a.at)));
      alertsList.appendChild(li);
    }
  } catch (_) { /* keep last render on transient failure */ }
}

connect();
refreshAlerts();
setInterval(refreshAlerts, 15000);
