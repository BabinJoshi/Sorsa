-- NOTE:
-- This is a separate ingestion-focused DDL copy.
-- Original file intentionally left untouched:
--   Mindshare DDL/ddl mindshare.sql

CREATE SCHEMA IF NOT EXISTS mindshare;

-- mindshare.mindshare_user definition
--
-- Drop table
--
-- DROP TABLE mindshare.mindshare_user;
CREATE TABLE IF NOT EXISTS mindshare.mindshare_user (
    x_id INT8 NOT NULL PRIMARY KEY,
    x_username STRING(255) NOT NULL,
    display_name STRING(255) NOT NULL,
    score DECIMAL(10, 2) NOT NULL,
    avatar_url STRING(1000) NOT NULL,
    adjustment_config JSONB NOT NULL,
    followers_count INT4 NOT NULL,
    verified BOOL NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp(),
    updated_at TIMESTAMPTZ NULL DEFAULT current_timestamp(),
    last_score_fetched_at TIMESTAMPTZ NULL DEFAULT current_timestamp()
);

CREATE INDEX IF NOT EXISTS idx_mindshare_user_x_username
    ON mindshare.mindshare_user (x_username);

-- mindshare.mindshare_post definition
--
-- Drop table
--
-- DROP TABLE mindshare.mindshare_post;
CREATE TABLE IF NOT EXISTS mindshare.mindshare_post (
    -- Core IDs
    post_id INT8 NOT NULL PRIMARY KEY,
    user_x_id INT8 NOT NULL,

    -- Source flags
    source_m BOOL NOT NULL DEFAULT false,
    source_n BOOL NOT NULL DEFAULT false,
    source_u BOOL NOT NULL DEFAULT false,

    -- Keyword set for cross-project dedupe
    project_keywords TEXT[] NULL,

    -- Post content
    full_text TEXT NOT NULL,

    -- Relationships
    retweeted_post_id INT8 NULL,
    replied_post_id INT8 NULL,
    quoted_post_id INT8 NULL,
    root_post_id INT8 NULL,

    -- Computed type flags
    is_retweet BOOL GENERATED ALWAYS AS (retweeted_post_id IS NOT NULL) STORED NOT NULL,
    is_reply BOOL GENERATED ALWAYS AS (replied_post_id IS NOT NULL) STORED NOT NULL,
    is_quote BOOL GENERATED ALWAYS AS (quoted_post_id IS NOT NULL) STORED NOT NULL,
    is_post BOOL GENERATED ALWAYS AS (
        retweeted_post_id IS NULL
        AND replied_post_id IS NULL
        AND quoted_post_id IS NULL
    ) STORED NOT NULL,

    -- Engagement metrics
    view_count INT4 NOT NULL DEFAULT 0,
    reply_count INT4 NOT NULL DEFAULT 0,
    retweet_count INT4 NOT NULL DEFAULT 0,
    quote_count INT4 NOT NULL DEFAULT 0,
    favorite_count INT4 NOT NULL DEFAULT 0,

    -- Raw entities
    entities JSONB NULL,

    -- Timestamps
    post_created_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp(),
    updated_at TIMESTAMPTZ NULL DEFAULT current_timestamp(),

    -- Ingestion metadata
    last_ingested_run_id UUID NULL,
    last_seen_at TIMESTAMPTZ NULL DEFAULT current_timestamp()
);

CREATE INDEX IF NOT EXISTS idx_mindshare_post_created_at
    ON mindshare.mindshare_post (post_created_at DESC);
CREATE INDEX IF NOT EXISTS idx_mindshare_post_user_x_id
    ON mindshare.mindshare_post (user_x_id);
CREATE INVERTED INDEX IF NOT EXISTS idx_mindshare_post_project_keywords
    ON mindshare.mindshare_post (project_keywords);

-- mindshare.ingestion_run definition
--
-- Run-level tracking table for daily ingestion execution.
--
-- Drop table
--
-- DROP TABLE mindshare.ingestion_run;
CREATE TABLE IF NOT EXISTS mindshare.ingestion_run (
    run_id UUID PRIMARY KEY,
    project_keyword TEXT NOT NULL,
    since_ts TIMESTAMPTZ NOT NULL,
    until_ts TIMESTAMPTZ NOT NULL,
    run_status TEXT NOT NULL,
    error_summary TEXT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp(),
    finished_at TIMESTAMPTZ NULL
);

CREATE INDEX IF NOT EXISTS idx_ingestion_run_status
    ON mindshare.ingestion_run (run_status, started_at DESC);

