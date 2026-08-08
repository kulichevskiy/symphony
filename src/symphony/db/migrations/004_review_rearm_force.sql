ALTER TABLE review_rearm_retries
ADD COLUMN force_retrigger INTEGER NOT NULL DEFAULT 0;
