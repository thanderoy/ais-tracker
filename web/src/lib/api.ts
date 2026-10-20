// Typed client for the tracker's REST API. Every response is wrapped as
// {"data": ...}; search and similar results are untagged Go structs, so their
// fields arrive PascalCase.

export interface Position {
  lon: number;
  lat: number;
  sog: number | null;
  cog: number | null;
  heading: number | null;
  nav_status: number | null;
  reported_at: string;
}

export interface VesselDetail {
  mmsi: number;
  name: string;
  imo: number | null;
  call_sign: string;
  ship_type: number | null;
  length_m: number | null;
  beam_m: number | null;
  flag_country: string;
  first_seen_at: string;
  last_seen_at: string;
  last_position: Position | null;
  sanctions: { program: string; reference: string; match_score: number; matched_at: string }[];
  operators: { id: number; canonical: string }[];
  anomaly: { score: number; method: string; computed_at: string } | null;
}

export interface SearchResult {
  MMSI: number;
  Name: string;
  CallSign: string;
  FlagCountry: string;
  Rank: number;
}

export interface SimilarResult {
  MMSI: number;
  Name: string;
  Similarity: number;
}

export interface Alert {
  type: "geofence" | "ais_gap" | "sts" | "sanctions";
  mmsi: number;
  at: string;
  detail: Record<string, unknown>;
}

export interface Gap {
  id: number;
  mmsi: number;
  gap_hours: number;
  resolved_at: string | null;
}

export interface STSEvent {
  id: number;
  mmsi_a: number;
  mmsi_b: number;
  ended_at: string | null;
}

export interface Port {
  id: number;
  name: string;
  country: string;
  lon: number;
  lat: number;
}

export interface Geofence {
  id: number;
  name: string;
  active: boolean;
  polygon: GeoJSON.GeoJsonObject | null;
}

/** One frame of the /ws/positions feed. */
export interface LiveFix {
  mmsi: number;
  lon: number;
  lat: number;
  sog: number | null;
  cog: number | null;
  at: string;
}

export async function api<T>(path: string, signal?: AbortSignal): Promise<T> {
  const res = await fetch(path, { signal });
  if (!res.ok) throw new Error(`${path}: ${res.status}`);
  const body = (await res.json()) as { data: T };
  return body.data;
}

/** List endpoints encode an empty result as null; callers always get an array. */
async function list<T>(path: string, signal?: AbortSignal): Promise<T[]> {
  return (await api<T[] | null>(path, signal)) ?? [];
}

const day = 24 * 3600 * 1000;

export const getAlerts = () =>
  list<Alert>(`/api/alerts?since=${encodeURIComponent(new Date(Date.now() - day).toISOString())}&limit=80`);
export const getOpenGaps = () => list<Gap>("/api/ais-gaps?resolved=false&limit=500");
export const getSTS = () => list<STSEvent>("/api/sts-events?limit=200");
export async function getVessel(mmsi: number, signal?: AbortSignal): Promise<VesselDetail> {
  const v = await api<VesselDetail>(`/api/vessels/${mmsi}`, signal);
  return { ...v, sanctions: v.sanctions ?? [], operators: v.operators ?? [] };
}
export const searchVessels = (q: string) => list<SearchResult>(`/api/vessels?search=${encodeURIComponent(q)}&limit=20`);
export const getSimilar = (mmsi: number, signal?: AbortSignal) =>
  list<SimilarResult>(`/api/vessels/${mmsi}/similar?limit=5`, signal);
export const getTrack = (mmsi: number, signal?: AbortSignal) =>
  list<Position>(`/api/vessels/${mmsi}/positions?from=${encodeURIComponent(new Date(Date.now() - day).toISOString())}`, signal);
export const getPorts = () => list<Port>("/api/ports?limit=500");
export const getGeofences = () => list<Geofence>("/api/geofences");

export async function ready(): Promise<boolean> {
  try {
    return (await fetch("/readyz")).ok;
  } catch {
    return false;
  }
}
