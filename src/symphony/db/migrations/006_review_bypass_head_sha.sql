-- 006_review_bypass_head_sha: scope a review Skip to the head it approved (SYM-245).
--
-- `$skip-review` records `review_bypassed=1` on `issue_prs`, but nothing recorded
-- *which* head that bypass approved, so a later push left the bypass in effect
-- forever (AC: "Skip is tied to the current input fingerprint and expires when
-- that input changes"). `review_bypassed_head_sha` is the PR head SHA at skip
-- time; '' means the head could not be read at skip time (an unscoped skip,
-- kept in effect rather than losing the operator's decision — mirrors
-- `_pr_head_sha_or_none`'s fallback).
ALTER TABLE issue_prs ADD COLUMN review_bypassed_head_sha TEXT NOT NULL DEFAULT '';
