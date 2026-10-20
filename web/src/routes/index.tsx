import { component$, useContextProvider, useStore, useVisibleTask$ } from "@builder.io/qwik";
import type { DocumentHead } from "@builder.io/qwik-city";
import { Header } from "~/components/terminal/header";
import { LiveMap } from "~/components/terminal/map";
import { Readout } from "~/components/terminal/readout";
import { Record } from "~/components/terminal/record";
import { Stream } from "~/components/terminal/stream";
import { InView } from "~/components/terminal/table";
import { getAlerts, getOpenGaps, getSTS, ready } from "~/lib/api";
import { resolveNames } from "~/lib/names";
import { TerminalCtx, type Terminal } from "~/lib/state";

const pollMs = 15_000;

export default component$(() => {
  const t = useStore<Terminal>({
    theme: "dark",
    link: "connecting",
    selected: null,
    follow: false,
    center: { lat: 20, lon: 0 },
    bbox: null,
    inView: 0,
    rate: 0,
    rows: [],
    alerts: [],
    alertsError: false,
    dark: [],
    sts: [],
    stsOpen: 0,
    ports: false,
    fences: false,
    dbReady: null,
    names: {},
    map: undefined,
  });
  useContextProvider(TerminalCtx, t);

  // The page is pre-rendered, so the real theme (set by the boot script in
  // <head>) is only known once it runs in the browser.
  // eslint-disable-next-line qwik/no-use-visible-task
  useVisibleTask$(
    () => {
      t.theme = document.documentElement.dataset.theme === "light" ? "light" : "dark";
    },
    { strategy: "document-ready" },
  );

  // eslint-disable-next-line qwik/no-use-visible-task
  useVisibleTask$(
    ({ cleanup }) => {
      const refresh = async () => {
        const [alerts, gaps, sts, ok] = await Promise.allSettled([getAlerts(), getOpenGaps(), getSTS(), ready()]);
        if (alerts.status === "fulfilled") {
          t.alerts = alerts.value;
          t.alertsError = false;
        } else {
          t.alertsError = true;
        }
        if (gaps.status === "fulfilled") t.dark = gaps.value.map((g) => g.mmsi);
        if (sts.status === "fulfilled") {
          const open = sts.value.filter((e) => e.ended_at == null);
          t.stsOpen = open.length;
          t.sts = open.flatMap((e) => [e.mmsi_a, e.mmsi_b]);
        }
        t.dbReady = ok.status === "fulfilled" ? ok.value : false;

        const mentioned = t.alerts.flatMap((a) =>
          a.type === "sts" ? [a.mmsi, Number(a.detail.mmsi_b)] : [a.mmsi],
        );
        await resolveNames(t, [...new Set(mentioned)], 24);
      };
      refresh();
      const id = setInterval(refresh, pollMs);
      cleanup(() => clearInterval(id));
    },
    { strategy: "document-ready" },
  );

  return (
    <div class="t-root">
      <Header />
      <Readout />
      <div class="t-row" style={{ flex: 1 }}>
        <Stream />
        <LiveMap />
        <Record />
      </div>
      <InView />
      <footer class="t-cell t-footer">
        <span>AIS-TRACKER</span>
        <span>{t.link === "live" ? "FEED LIVE" : "FEED OFFLINE"}</span>
        <span class="end">/// END OF SCREEN</span>
      </footer>
    </div>
  );
});

export const head: DocumentHead = {
  title: "ais-tracker",
  meta: [{ name: "description", content: "Live AIS vessel tracking terminal" }],
};
