-- 000001 baseline — intentionally empty.
-- The schema proper begins at migration 000002 (issue P1-2). This no-op exists
-- so golang-migrate has a versioned starting point that round-trips cleanly.
DO $$ BEGIN END $$;
