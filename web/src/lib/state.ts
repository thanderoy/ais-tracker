import { createContextId, type NoSerialize } from "@builder.io/qwik";
import type { Alert } from "./api";

export type Theme = "dark" | "light";
export type Link = "connecting" | "live" | "down";

/** A vessel currently inside the map viewport, as last seen on the feed. */
export interface Row {
  mmsi: number;
  sog: number | null;
  cog: number | null;
  at: string;
}

/** Imperative handle onto the Leaflet map, set once it mounts in the browser. */
export interface MapHandle {
  select(mmsi: number | null): void;
  focus(lat: number, lon: number): void;
  flags(mmsis: Set<number>): void;
  layers(ports: boolean, fences: boolean): void;
}

export interface Terminal {
  theme: Theme;
  link: Link;
  selected: number | null;
  follow: boolean;
  /** Center and bounds of the viewport the socket is subscribed to. */
  center: { lat: number; lon: number };
  bbox: [number, number, number, number] | null;
  inView: number;
  rate: number;
  rows: Row[];
  alerts: Alert[];
  alertsError: boolean;
  dark: number[];
  /** MMSIs on either side of an open STS transfer. */
  sts: number[];
  stsOpen: number;
  ports: boolean;
  fences: boolean;
  dbReady: boolean | null;
  /** MMSI -> vessel name, filled lazily from /api/vessels/{mmsi}. */
  names: Record<string, string>;
  map: NoSerialize<MapHandle>;
}

export const TerminalCtx = createContextId<Terminal>("ais.terminal");

/** The set of MMSIs currently flagged (dark or in an open STS), for markers and rows. */
export function flagged(t: Terminal): Set<number> {
  return new Set([...t.dark, ...t.sts]);
}
