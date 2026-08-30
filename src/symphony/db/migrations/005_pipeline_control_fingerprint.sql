-- 005_pipeline_control_fingerprint: scope a Skip to the input it approved (SYM-245).
--
-- Skip is only valid for the two validation stages (review, acceptance), and it
-- approves *what was validated* — not the stage in perpetuity. `fingerprint`
-- records that stage's input at skip time (the PR head SHA where one applies),
-- so pushing new commits invalidates the skip and validation is required again.
-- NULL on every non-skip row, and on a skip whose stage has no meaningful input.
ALTER TABLE pipeline_controls ADD COLUMN fingerprint TEXT;
