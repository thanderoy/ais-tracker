import { component$, useContext } from "@builder.io/qwik";
import { lat, lon, pad2 } from "~/lib/format";
import { TerminalCtx } from "~/lib/state";

/** Sector heading plus the four headline counters. */
export const Readout = component$(() => {
  const t = useContext(TerminalCtx);
  const b = t.bbox;
  const sector = b
    ? `BBOX ${b[0].toFixed(1)}  ${b[1].toFixed(1)}  ${b[2].toFixed(1)}  ${b[3].toFixed(1)}`
    : "BBOX PENDING";

  return (
    <section class="t-row" aria-label="Readout">
      <div class="t-cell t-sector">
        <div class="muted">LIVE VIEW / {sector}</div>
        <h1 class="macro">
          {lat(t.center.lat).replace(/\.\d+/, "")} {lon(t.center.lon).replace(/\.\d+/, "")}
        </h1>
      </div>
      <dl class="t-counters">
        <div>
          <dt>IN VIEW</dt>
          <dd class="macro">{t.inView}</dd>
        </div>
        <div>
          <dt>MSG / S</dt>
          <dd class="macro">{t.rate}</dd>
        </div>
        <div>
          <dt>DARK</dt>
          <dd class={["macro", { hot: t.dark.length > 0 }]}>{pad2(t.dark.length)}</dd>
        </div>
        <div class={{ "is-alert": t.stsOpen > 0 }}>
          <dt>STS OPEN</dt>
          <dd class="macro">{pad2(t.stsOpen)}</dd>
        </div>
      </dl>
    </section>
  );
});
