import { component$ } from "@builder.io/qwik";
import { QwikCityProvider, RouterOutlet, useDocumentHead } from "@builder.io/qwik-city";

import "@fontsource/jetbrains-mono/400.css";
import "@fontsource/jetbrains-mono/500.css";
import "@fontsource/jetbrains-mono/700.css";
import "@fontsource/archivo-black/400.css";
import "./global.css";

// Runs before first paint so a stored or OS-preferred theme never flashes the
// wrong palette.
const themeBoot = `(function(){try{var t=localStorage.getItem("theme");if(t!=="light"&&t!=="dark"){t=matchMedia("(prefers-color-scheme: light)").matches?"light":"dark"}document.documentElement.dataset.theme=t}catch(e){document.documentElement.dataset.theme="dark"}})()`;

const Head = component$(() => {
  const head = useDocumentHead();
  return (
    <>
      <title>{head.title}</title>
      <meta name="viewport" content="width=device-width, initial-scale=1.0" />
      <link rel="icon" type="image/svg+xml" href="/favicon.svg" />
      {head.meta.map((m) => (
        <meta key={m.key} {...m} />
      ))}
    </>
  );
});

export default component$(() => {
  return (
    <QwikCityProvider>
      <head>
        <meta charset="utf-8" />
        <script dangerouslySetInnerHTML={themeBoot} />
        <Head />
      </head>
      <body>
        <RouterOutlet />
      </body>
    </QwikCityProvider>
  );
});
