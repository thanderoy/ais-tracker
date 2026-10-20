import { component$, useContext } from "@builder.io/qwik";
import type { Alert } from "~/lib/api";
import { num, utcTime } from "~/lib/format";
import { nameOf } from "~/lib/names";
import { TerminalCtx, type Terminal } from "~/lib/state";

interface Line {
  tag: string;
  text: string;
  hot: boolean;
}

function describe(t: Terminal, a: Alert): Line {
  const d = a.detail;
  const name = nameOf(t, a.mmsi);
  switch (a.type) {
    case "geofence":
      return {
        tag: d.event_type === "enter" ? "FENCE>" : "FENCE<",
        text: `${name} / FENCE #${d.geofence_id}`,
        hot: false,
      };
    case "ais_gap":
      return d.resolved_at
        ? { tag: "GAP~", text: `${name} / BACK ${String(d.resolution ?? "").replace(/_/g, " ")}`, hot: false }
        : { tag: "! GAP", text: `${name} / DARK ${d.gap_hours}H`, hot: true };
    case "sts":
      return {
        tag: "!! STS",
        text: `${name} <> ${nameOf(t, Number(d.mmsi_b))} / ${num(d.min_distance as number | null, 0)}M`,
        hot: d.ended_at == null,
      };
    case "sanctions":
      return { tag: "!! SDN", text: `${name} / ${d.program} ${d.reference}`, hot: true };
  }
}

/** The 24h alert feed: geofence crossings, AIS gaps, STS transfers, sanctions hits. */
export const Stream = component$(() => {
  const t = useContext(TerminalCtx);

  return (
    <section class="t-cell t-stream" aria-label="Event stream">
      <h2 class="t-title">
        <span>[ EVENT STREAM ]</span>
        <small>24H</small>
      </h2>
      {t.alertsError && t.alerts.length === 0 && <p class="empty">FEED UNAVAILABLE. RETRYING.</p>}
      {!t.alertsError && t.alerts.length === 0 && <p class="empty">NO EVENTS IN THE LAST 24H.</p>}
      <ol>
        {t.alerts.map((a, i) => {
          const line = describe(t, a);
          return (
            <li key={`${a.type}-${a.mmsi}-${a.at}-${i}`} class={{ "is-hot": line.hot }}>
              <button type="button" onClick$={() => (t.selected = a.mmsi)}>
                <span class="muted">{utcTime(a.at)}</span>
                <span>
                  <span class={{ hot: line.hot }}>{line.tag}</span> {line.text}
                </span>
              </button>
            </li>
          );
        })}
      </ol>
      <div class="t-foot">
        {t.dbReady === null ? "DB CHECKING" : t.dbReady ? "DB READY" : "DB NOT READY"}
        {" / "}POLL 15S
      </div>
    </section>
  );
});
