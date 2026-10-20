import { Resource, component$, useContext, useResource$ } from "@builder.io/qwik";
import { getSimilar, getTrack, getVessel, type Position, type SimilarResult, type VesselDetail } from "~/lib/api";
import { cog, dash, lat, lon, num, shipType, utcTime } from "~/lib/format";
import { TerminalCtx } from "~/lib/state";

interface Loaded {
  v: VesselDetail;
  track: Position[];
  similar: SimilarResult[];
}

/** SOG over the last 24h as a single 2px line on a 300x60 box. */
function sparkline(track: Position[]): { d: string; min: number; max: number } | null {
  const pts = track.filter((p) => p.sog != null);
  if (pts.length < 2) return null;
  const t0 = new Date(pts[0].reported_at).getTime();
  const span = Math.max(1, new Date(pts[pts.length - 1].reported_at).getTime() - t0);
  const sogs = pts.map((p) => p.sog as number);
  const min = Math.min(...sogs);
  const max = Math.max(...sogs);
  const top = Math.max(max, 1);
  const d = pts
    .map((p, i) => {
      const x = ((new Date(p.reported_at).getTime() - t0) / span) * 300;
      const y = 56 - ((p.sog as number) / top) * 50;
      return `${i ? "L" : "M"}${x.toFixed(1)} ${y.toFixed(1)}`;
    })
    .join(" ");
  return { d, min, max };
}

export const Record = component$(() => {
  const t = useContext(TerminalCtx);

  const rec = useResource$<Loaded | null>(async ({ track, cleanup }) => {
    const mmsi = track(() => t.selected);
    if (mmsi == null) return null;
    const ctl = new AbortController();
    cleanup(() => ctl.abort());
    const [v, tr, sim] = await Promise.allSettled([
      getVessel(mmsi, ctl.signal),
      getTrack(mmsi, ctl.signal),
      getSimilar(mmsi, ctl.signal),
    ]);
    if (v.status === "rejected") throw new Error(String(mmsi));
    const vessel = v.value;
    t.names = { ...t.names, [String(mmsi)]: vessel.name || "" };
    if (vessel.last_position) t.map?.focus(vessel.last_position.lat, vessel.last_position.lon);
    // Track ascending by time for the sparkline; similarity is optional.
    const trackRows = tr.status === "fulfilled" ? [...tr.value].sort((a, b) => Date.parse(a.reported_at) - Date.parse(b.reported_at)) : [];
    return { v: vessel, track: trackRows, similar: sim.status === "fulfilled" ? sim.value : [] };
  });

  return (
    <section class="t-cell t-record" aria-label="Vessel record">
      <h2 class="t-title">
        <span>[ RECORD{t.selected != null ? ` / ${t.selected}` : ""} ]</span>
      </h2>
      <Resource
        value={rec}
        onPending={() => <p class="empty">LOADING RECORD...</p>}
        onRejected={(e) => <p class="empty">NO RECORD FOR MMSI {e.message}.</p>}
        onResolved={(r) => {
          if (!r) {
            return (
              <p class="empty">
                NO VESSEL SELECTED.
                <br />
                CLICK A MARKER, AN EVENT, OR PRESS / TO FIND ONE.
              </p>
            );
          }
          const { v } = r;
          const p = v.last_position;
          const spark = sparkline(r.track);
          const sanction = v.sanctions[0];
          return (
            <>
              <div class="name">
                <div class="macro">{v.name || "UNNAMED"}</div>
                <div class="muted" style={{ marginTop: "6px" }}>
                  {shipType(v.ship_type)} / FLAG {v.flag_country || dash} / IMO {v.imo ?? dash}
                </div>
              </div>
              <dl>
                <div>
                  <dt>SOG</dt>
                  <dd class="big">{p ? `${num(p.sog)} KN` : dash}</dd>
                </div>
                <div>
                  <dt>COG</dt>
                  <dd class="big">{cog(p?.cog)}</dd>
                </div>
                <div>
                  <dt>LAT</dt>
                  <dd>{p ? lat(p.lat) : dash}</dd>
                </div>
                <div>
                  <dt>LON</dt>
                  <dd>{p ? lon(p.lon) : dash}</dd>
                </div>
                <div>
                  <dt>CALLSIGN</dt>
                  <dd>{v.call_sign || dash}</dd>
                </div>
                <div>
                  <dt>LAST FIX</dt>
                  <dd>{p ? `${utcTime(p.reported_at)}Z` : dash}</dd>
                </div>
                <div class="wide">
                  <dt>OPERATOR CHAIN</dt>
                  <dd>{v.operators.length ? v.operators.map((o) => o.canonical).join(" /// ") : dash}</dd>
                </div>
                <div>
                  <dt>SDN MATCH</dt>
                  <dd class={{ hot: !!sanction }}>{sanction ? `${sanction.program} ${sanction.reference}` : "NONE"}</dd>
                </div>
                <div>
                  <dt>ANOMALY</dt>
                  <dd>{v.anomaly ? num(v.anomaly.score, 2) : dash}</dd>
                </div>
              </dl>
              <div class="block">
                <div class="muted">
                  SOG 24H
                  {spark
                    ? ` / MIN ${num(spark.min)} / MAX ${num(spark.max)}`
                    : ` / ${r.track.length} ${r.track.length === 1 ? "FIX" : "FIXES"}, TOO FEW FOR A TREND`}
                </div>
                {spark && (
                  <svg class="spark" viewBox="0 0 300 60" preserveAspectRatio="none" role="img" aria-label="Speed over ground, last 24 hours">
                    <path class="base" d="M0 59.5H300" />
                    <path class="series" d={spark.d} vector-effect="non-scaling-stroke" />
                  </svg>
                )}
              </div>
              {r.similar.length > 0 && (
                <div class="block">
                  <div class="muted">MOVES LIKE</div>
                  <ul class="similar">
                    {r.similar.map((s) => (
                      <li key={s.MMSI}>
                        <button type="button" onClick$={() => (t.selected = s.MMSI)}>
                          <span>{s.Name || s.MMSI}</span>
                          <span class="muted">{num(s.Similarity, 2)}</span>
                        </button>
                      </li>
                    ))}
                  </ul>
                </div>
              )}
            </>
          );
        }}
      />
      {t.selected != null && (
        <div class="t-actions">
          <button type="button" aria-pressed={t.follow} onClick$={() => (t.follow = !t.follow)}>
            {t.follow ? "FOLLOWING" : "FOLLOW"}
          </button>
          <button
            type="button"
            onClick$={() => {
              t.selected = null;
              t.follow = false;
            }}
          >
            CLEAR
          </button>
        </div>
      )}
    </section>
  );
});
