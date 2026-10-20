import { $, component$, sync$, useContext, useOnDocument, useSignal, useVisibleTask$ } from "@builder.io/qwik";
import { searchVessels, type SearchResult } from "~/lib/api";
import { utcTime } from "~/lib/format";
import { TerminalCtx } from "~/lib/state";

export const Header = component$(() => {
  const t = useContext(TerminalCtx);
  const query = useSignal("");
  const results = useSignal<SearchResult[] | null>(null);
  const failed = useSignal(false);
  const input = useSignal<HTMLInputElement>();
  const clock = useSignal("--:--:--");

  // eslint-disable-next-line qwik/no-use-visible-task
  useVisibleTask$(({ cleanup }) => {
    const tick = () => (clock.value = utcTime(new Date()));
    tick();
    const id = setInterval(tick, 1000);
    cleanup(() => clearInterval(id));
  });

  // "/" focuses the command line from anywhere. The default has to be blocked
  // synchronously or the slash lands in the input.
  useOnDocument(
    "keydown",
    sync$((e: KeyboardEvent) => {
      const typing = (e.target as HTMLElement | null)?.closest("input, textarea");
      if (e.key === "/" && !typing) e.preventDefault();
    }),
  );
  useOnDocument(
    "keydown",
    $((e: KeyboardEvent) => {
      const typing = (e.target as HTMLElement | null)?.closest("input, textarea");
      if (e.key === "/" && !typing) {
        input.value?.focus();
      } else if (e.key === "Escape") {
        results.value = null;
      }
    }),
  );

  const run = $(async () => {
    const q = query.value.trim();
    if (!q) {
      results.value = null;
      return;
    }
    // A bare 9-digit MMSI jumps straight to the record.
    if (/^\d{9}$/.test(q)) {
      t.selected = Number(q);
      results.value = null;
      query.value = "";
      return;
    }
    try {
      failed.value = false;
      results.value = await searchVessels(q);
    } catch {
      failed.value = true;
      results.value = [];
    }
  });

  const pick = $((mmsi: number) => {
    t.selected = mmsi;
    results.value = null;
    query.value = "";
  });

  const toggleTheme = $(() => {
    const next = t.theme === "dark" ? "light" : "dark";
    t.theme = next;
    document.documentElement.dataset.theme = next;
    try {
      localStorage.setItem("theme", next);
    } catch {
      // Storage can be unavailable; the switch still applies for this session.
    }
  });

  return (
    <header class="t-row">
      <div class="t-cell t-brand">[ AIS-TRACKER ] // TERM 01</div>
      <form class="t-cell t-command" role="search" preventdefault:submit onSubmit$={() => run()}>
        <span aria-hidden="true" class="hot">
          &gt;
        </span>
        <label for="cmd" class="sr-only">
          Find vessel by name, MMSI, or call sign
        </label>
        <input
          id="cmd"
          ref={input}
          type="text"
          autocomplete="off"
          spellcheck={false}
          placeholder="FIND NAME / MMSI / CALLSIGN"
          bind:value={query}
        />
        <kbd>/</kbd>
        {results.value && (
          <ul class="t-results" aria-label="Search results">
            {results.value.length === 0 && (
              <li class="empty">{failed.value ? "SEARCH FAILED. TRY AGAIN." : "NO MATCHES."}</li>
            )}
            {results.value.map((r) => (
              <li key={r.MMSI}>
                <button type="button" onClick$={() => pick(r.MMSI)}>
                  <span>{r.MMSI}</span>
                  <span>{r.Name || "UNNAMED"}</span>
                  <span class="muted">
                    {r.CallSign} {r.FlagCountry}
                  </span>
                </button>
              </li>
            ))}
          </ul>
        )}
      </form>
      <div class="t-cell t-status" aria-live="polite">
        {t.link === "live" && <span class="t-link-live">■ WS LINK LIVE</span>}
        {t.link === "down" && <span class="t-link-down">■ WS LINK DOWN</span>}
        {t.link === "connecting" && <span class="muted">□ WS LINKING</span>}
        <span>{clock.value}Z</span>
      </div>
      <button
        type="button"
        class="t-cell t-theme"
        onClick$={toggleTheme}
        aria-label={t.theme === "dark" ? "Switch to light theme" : "Switch to dark theme"}
      >
        [ {t.theme === "dark" ? "LIGHT" : "DARK"} ]
      </button>
    </header>
  );
});
