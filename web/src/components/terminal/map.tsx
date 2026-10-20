import { component$, noSerialize, useContext, useSignal, useVisibleTask$ } from "@builder.io/qwik";
import type * as Leaflet from "leaflet";
import "leaflet/dist/leaflet.css";
import { getGeofences, getPorts, type LiveFix } from "~/lib/api";
import { lat, lon } from "~/lib/format";
import { flagged, TerminalCtx, type MapHandle } from "~/lib/state";

// Standard OSM tiles need no key; the monochrome look (and the dark theme's
// inversion) comes from a CSS filter on the tile pane, see --tiles.
const tileUrl = "https://tile.openstreetmap.org/{z}/{x}/{y}.png";

// Fixes not refreshed within this window drop off the map.
const staleMs = 60 * 60 * 1000;
const tableRows = 40;

export const LiveMap = component$(() => {
  const t = useContext(TerminalCtx);
  const host = useSignal<HTMLDivElement>();
  const cursor = useSignal("");

  // Leaflet and the socket live entirely in the browser; the rest of the app
  // reaches them through the MapHandle stored on the terminal context.
  // eslint-disable-next-line qwik/no-use-visible-task
  useVisibleTask$(async ({ cleanup }) => {
    const L = (await import("leaflet")).default;
    const map = L.map(host.value!, { worldCopyJump: true, zoomControl: false }).setView([20, 0], 3);
    L.control.zoom({ position: "topright" }).addTo(map);
    L.tileLayer(tileUrl, {
      attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
      maxZoom: 18,
    }).addTo(map);

    const fixes = new Map<number, LiveFix>();
    const markers = new Map<number, Leaflet.Marker>();
    const vessels = L.layerGroup().addTo(map);
    const pick = L.marker([0, 0], {
      icon: L.divIcon({ className: "t-pick", iconSize: [26, 26] }),
      interactive: false,
      keyboard: false,
    });
    let flags = new Set<number>();
    let selected: number | null = null;
    let frames = 0;
    // The writer flushes in batches, so the rate is a 10s rolling mean.
    const recent: number[] = [];

    const icon = (cog: number | null, flag: boolean) =>
      L.divIcon({
        className: flag ? "t-vessel is-flagged" : "t-vessel",
        iconSize: [12, 14],
        iconAnchor: [6, 7],
        html: `<svg width="12" height="14" viewBox="-6 -7 12 14" aria-hidden="true"><path d="M0,-7 L5,6 L-5,6 Z" transform="rotate(${cog ?? 0})"/></svg>`,
      });

    const place = (p: LiveFix) => {
      const m = markers.get(p.mmsi);
      if (m) {
        m.setLatLng([p.lat, p.lon]);
        m.setIcon(icon(p.cog, flags.has(p.mmsi)));
        return;
      }
      const created = L.marker([p.lat, p.lon], { icon: icon(p.cog, flags.has(p.mmsi)), title: String(p.mmsi) });
      created.on("click", () => (t.selected = p.mmsi));
      created.addTo(vessels);
      markers.set(p.mmsi, created);
    };

    let socket: WebSocket | null = null;
    let retry: ReturnType<typeof setTimeout> | undefined;
    let closed = false;

    const subscribe = () => {
      const b = map.getBounds();
      const c = map.getCenter();
      t.bbox = [b.getWest(), b.getSouth(), b.getEast(), b.getNorth()];
      t.center = { lat: c.lat, lon: c.lng };
      if (socket?.readyState === WebSocket.OPEN) {
        socket.send(JSON.stringify({ type: "subscribe", bbox: t.bbox }));
      }
    };

    const connect = () => {
      const proto = location.protocol === "https:" ? "wss:" : "ws:";
      socket = new WebSocket(`${proto}//${location.host}/ws/positions`);
      t.link = "connecting";
      socket.onopen = () => {
        t.link = "live";
        subscribe();
      };
      socket.onclose = () => {
        if (closed) return;
        t.link = "down";
        retry = setTimeout(connect, 2000);
      };
      socket.onerror = () => socket?.close();
      socket.onmessage = (ev) => {
        let p: LiveFix;
        try {
          p = JSON.parse(ev.data);
        } catch {
          return;
        }
        frames++;
        fixes.set(p.mmsi, p);
        place(p);
        if (p.mmsi === selected) {
          pick.setLatLng([p.lat, p.lon]).addTo(map);
          if (t.follow) map.panTo([p.lat, p.lon], { animate: true });
        }
      };
    };

    map.on("moveend", subscribe);
    map.on("mousemove", (e: Leaflet.LeafletMouseEvent) => {
      cursor.value = `${lat(e.latlng.lat)} / ${lon(L.Util.wrapNum(e.latlng.lng, [-180, 180], true))}`;
    });
    map.on("mouseout", () => (cursor.value = ""));

    // Once a second: frame rate, the in-view table, and stale-fix pruning.
    const tick = setInterval(() => {
      recent.push(frames);
      if (recent.length > 10) recent.shift();
      t.rate = Math.round(recent.reduce((a, b) => a + b, 0) / recent.length);
      frames = 0;
      const now = Date.now();
      const bounds = map.getBounds();
      const inView: LiveFix[] = [];
      for (const [mmsi, p] of fixes) {
        if (now - new Date(p.at).getTime() > staleMs) {
          fixes.delete(mmsi);
          markers.get(mmsi)?.remove();
          markers.delete(mmsi);
          continue;
        }
        if (bounds.contains([p.lat, p.lon])) inView.push(p);
      }
      inView.sort((a, b) => (a.at < b.at ? 1 : -1));
      t.inView = inView.length;
      t.rows = inView.slice(0, tableRows).map(({ mmsi, sog, cog, at }) => ({ mmsi, sog, cog, at }));
    }, 1000);

    const ports = L.layerGroup();
    const fences = L.layerGroup();
    let portsLoaded = false;
    let fencesLoaded = false;

    const handle: MapHandle = {
      select(mmsi) {
        selected = mmsi;
        const p = mmsi == null ? undefined : fixes.get(mmsi);
        if (p) pick.setLatLng([p.lat, p.lon]).addTo(map);
        else pick.remove();
      },
      focus(la, lo) {
        if (selected != null && !fixes.has(selected)) pick.setLatLng([la, lo]).addTo(map);
        map.setView([la, lo], Math.max(map.getZoom(), 8));
      },
      flags(set) {
        const prev = flags;
        flags = set;
        for (const [mmsi, m] of markers) {
          if (prev.has(mmsi) !== set.has(mmsi)) m.setIcon(icon(fixes.get(mmsi)?.cog ?? null, set.has(mmsi)));
        }
      },
      async layers(showPorts, showFences) {
        if (showPorts) {
          if (!portsLoaded) {
            portsLoaded = true;
            try {
              for (const p of await getPorts()) {
                L.circleMarker([p.lat, p.lon], { radius: 3, weight: 1, className: "t-port" })
                  .bindTooltip(`${p.name} ${p.country}`)
                  .addTo(ports);
              }
            } catch {
              portsLoaded = false;
            }
          }
          ports.addTo(map);
        } else {
          ports.remove();
        }
        if (showFences) {
          if (!fencesLoaded) {
            fencesLoaded = true;
            try {
              for (const f of await getGeofences()) {
                if (!f.polygon) continue;
                L.geoJSON(f.polygon, { style: { className: "t-fence", weight: 1 } })
                  .bindTooltip(f.name)
                  .addTo(fences);
              }
            } catch {
              fencesLoaded = false;
            }
          }
          fences.addTo(map);
        } else {
          fences.remove();
        }
      },
    };
    handle.flags(flagged(t));
    handle.select(t.selected);
    t.map = noSerialize(handle);
    connect();

    cleanup(() => {
      closed = true;
      clearTimeout(retry);
      clearInterval(tick);
      socket?.close();
      map.remove();
      t.map = undefined;
    });
  });

  // eslint-disable-next-line qwik/no-use-visible-task
  useVisibleTask$(({ track }) => {
    const m = track(() => t.map);
    const s = track(() => t.selected);
    m?.select(s);
  });

  // eslint-disable-next-line qwik/no-use-visible-task
  useVisibleTask$(({ track }) => {
    const m = track(() => t.map);
    const p = track(() => t.ports);
    const f = track(() => t.fences);
    m?.layers(p, f);
  });

  // eslint-disable-next-line qwik/no-use-visible-task
  useVisibleTask$(({ track }) => {
    const m = track(() => t.map);
    track(() => t.dark);
    track(() => t.sts);
    m?.flags(flagged(t));
  });

  return (
    <section class="t-cell t-map" aria-label="Live map">
      <div ref={host} class="t-map-canvas" />
      <div class="t-scan" aria-hidden="true" />
      <div class="t-layers">
        <button type="button" aria-pressed={t.ports} onClick$={() => (t.ports = !t.ports)}>
          [ PORTS ]
        </button>
        <button type="button" aria-pressed={t.fences} onClick$={() => (t.fences = !t.fences)}>
          [ FENCES ]
        </button>
      </div>
      {cursor.value && <div class="t-coords">{cursor.value}</div>}
    </section>
  );
});
