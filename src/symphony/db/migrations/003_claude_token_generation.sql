-- 003_claude_token_generation: stamp each run with the minting of the shared
-- agent token it was dispatched on (SYM-233).
--
-- There is exactly one Claude connection behind every run, so "does this run
-- still hold the current token" is answered by comparing a counter, not by
-- comparing secrets. `oauth_connections.generation` advances every time the
-- stored credential is replaced (operator reconnect, central refresh,
-- write-back); `runs.claude_token_generation` remembers the value a run was
-- handed. NULL means the run authenticated ambiently — there was no UI
-- connection to take a token from, which is not the same as generation 0.
ALTER TABLE oauth_connections ADD COLUMN generation INTEGER NOT NULL DEFAULT 1;
ALTER TABLE runs ADD COLUMN claude_token_generation INTEGER;

-- The counter has to outlive the row it describes. Disconnect DELETEs the
-- `oauth_connections` row, so a counter living only there would restart at 1 on
-- the next reconnect — and a run still in flight, stamped with a pre-disconnect
-- value, would eventually collide with a re-climbed generation and be mistaken
-- for a holder of the current token. This side table is the sequence; the
-- column above is the denormalized current value, kept there so a run's token
-- and its generation can be read in one shot.
CREATE TABLE oauth_credential_generations (
    provider   TEXT PRIMARY KEY,
    generation INTEGER NOT NULL
);

-- Seed from whatever is already connected, matching the column default.
INSERT INTO oauth_credential_generations (provider, generation)
    SELECT provider, 1 FROM oauth_connections;
