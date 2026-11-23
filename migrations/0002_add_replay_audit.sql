-- Replay audit. A replay is an operator action that returns a dead job
-- to retry_wait, so the durable row must retain who replayed it, when, and how
-- many times it has been replayed.
ALTER TABLE jobs
    ADD COLUMN replay_count integer NOT NULL DEFAULT 0,
    ADD COLUMN replayed_by  text,
    ADD COLUMN replayed_at  timestamptz;

ALTER TABLE jobs
    ADD CONSTRAINT jobs_replay_count_valid CHECK (replay_count >= 0);
