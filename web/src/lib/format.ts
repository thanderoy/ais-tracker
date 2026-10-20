export const dash = "-";

export function num(n: number | null | undefined, digits = 1): string {
  return n == null ? dash : n.toFixed(digits);
}

export function pad2(n: number): string {
  return n < 10 ? `0${n}` : String(n);
}

/** HH:MM:SS in UTC, the terminal's only clock. */
export function utcTime(iso: string | Date): string {
  const d = typeof iso === "string" ? new Date(iso) : iso;
  return `${pad2(d.getUTCHours())}:${pad2(d.getUTCMinutes())}:${pad2(d.getUTCSeconds())}`;
}

/** Degrees and decimal minutes, e.g. 01°58.21′N. */
export function dm(value: number, pos: string, neg: string, degWidth: number): string {
  const hemi = value >= 0 ? pos : neg;
  const abs = Math.abs(value);
  const deg = Math.floor(abs);
  const min = (abs - deg) * 60;
  return `${String(deg).padStart(degWidth, "0")}°${min.toFixed(2).padStart(5, "0")}′${hemi}`;
}

export const lat = (v: number) => dm(v, "N", "S", 2);
export const lon = (v: number) => dm(v, "E", "W", 3);

/** Course over ground; AIS encodes "not available" as 360. */
export function cog(n: number | null | undefined): string {
  return n == null || n >= 360 ? dash : `${n.toFixed(0)}°`;
}

/** Elapsed time as H:MM:SS, or MM:SS under an hour. */
export function age(iso: string, now: number): string {
  const s = Math.max(0, Math.round((now - new Date(iso).getTime()) / 1000));
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const r = s % 60;
  return h > 0 ? `${h}:${pad2(m)}:${pad2(r)}` : `${pad2(m)}:${pad2(r)}`;
}

/** Coarse AIS ship-type bucket (ITU-R M.1371 type codes). */
export function shipType(code: number | null): string {
  if (code == null) return dash;
  if (code === 30) return "FISHING";
  if (code === 31 || code === 32) return "TOWING";
  if (code === 36) return "SAILING";
  if (code === 37) return "PLEASURE";
  if (code >= 40 && code < 50) return "HSC";
  if (code === 50) return "PILOT";
  if (code === 51) return "SAR";
  if (code === 52) return "TUG";
  if (code >= 60 && code < 70) return "PASSENGER";
  if (code >= 70 && code < 80) return "CARGO";
  if (code >= 80 && code < 90) return "TANKER";
  return "OTHER";
}
