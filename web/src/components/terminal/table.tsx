import { component$, useContext, useVisibleTask$ } from "@builder.io/qwik";
import { age, cog, dash, num } from "~/lib/format";
import { resolveNames } from "~/lib/names";
import { TerminalCtx } from "~/lib/state";

/** The most recently reporting vessels inside the map viewport. */
export const InView = component$(() => {
  const t = useContext(TerminalCtx);

  // eslint-disable-next-line qwik/no-use-visible-task
  useVisibleTask$(({ track }) => {
    const rows = track(() => t.rows);
    resolveNames(
      t,
      rows.map((r) => r.mmsi),
    );
  });

  const now = Date.now();
  const dark = new Set(t.dark);
  const sts = new Set(t.sts);

  return (
    <section class="t-cell t-table" aria-label="Vessels in view">
      {t.rows.length === 0 ? (
        <p class="empty">NO VESSELS REPORTING IN VIEW. PAN OR ZOOM OUT.</p>
      ) : (
        <table>
          <thead>
            <tr>
              <th scope="col">MMSI</th>
              <th scope="col">NAME</th>
              <th scope="col" class="num">
                SOG
              </th>
              <th scope="col" class="num">
                COG
              </th>
              <th scope="col">LAST</th>
              <th scope="col">FLAGS</th>
            </tr>
          </thead>
          <tbody>
            {t.rows.map((r) => {
              const flags = [dark.has(r.mmsi) && "DARK", sts.has(r.mmsi) && "STS"].filter(Boolean).join(" ");
              return (
                <tr key={r.mmsi} class={{ "is-selected": r.mmsi === t.selected }}>
                  <td>
                    <button type="button" onClick$={() => (t.selected = r.mmsi)} aria-label={`Select ${r.mmsi}`}>
                      {r.mmsi}
                    </button>
                  </td>
                  <td>
                    <span>{t.names[String(r.mmsi)] || dash}</span>
                  </td>
                  <td class="num">
                    <span>{num(r.sog)}</span>
                  </td>
                  <td class="num">
                    <span>{cog(r.cog)}</span>
                  </td>
                  <td>
                    <span>{age(r.at, now)}</span>
                  </td>
                  <td>
                    <span class={{ hot: !!flags }}>{flags}</span>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      )}
    </section>
  );
});
