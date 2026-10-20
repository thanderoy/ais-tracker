import { getVessel } from "./api";
import type { Terminal } from "./state";

const inflight = new Set<number>();
const missing = new Set<number>();

/**
 * Resolves vessel names the terminal doesn't know yet. The API has no batch
 * lookup, so this caps concurrent detail requests and remembers misses.
 */
export async function resolveNames(t: Terminal, mmsis: number[], max = 8): Promise<void> {
  const todo = mmsis
    .filter((m) => !(String(m) in t.names) && !inflight.has(m) && !missing.has(m))
    .slice(0, max);
  await Promise.all(
    todo.map(async (m) => {
      inflight.add(m);
      try {
        const v = await getVessel(m);
        t.names = { ...t.names, [String(m)]: v.name || "" };
      } catch {
        missing.add(m);
      } finally {
        inflight.delete(m);
      }
    }),
  );
}

export function nameOf(t: Terminal, mmsi: number): string {
  return t.names[String(mmsi)] || String(mmsi);
}
