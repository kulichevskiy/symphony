-- 002_config_globals_knobs: operational knobs move into the DB (Config v2 7/9).
-- A sparse JSON object of operator-set knob overrides (poll interval, caps,
-- timeouts); unset keys fall back to code defaults. Editable in the UI and
-- hot-reloaded by the daemon at tick boundaries.
ALTER TABLE config_globals ADD COLUMN knobs TEXT NOT NULL DEFAULT '{}';
