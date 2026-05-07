-- mindshare.mindshare_user definition

-- Drop table

-- DROP TABLE mindshare.mindshare_user;

CREATE TABLE mindshare.mindshare_user (
    x_id INT8 NOT NULL,
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


--
--CREATE INDEX ix_mindshare_mindshare_user_x_username ON mindshare.mindshare_user USING btree (x_username);


-- mindshare.mindshare_post definition

-- Drop table

-- DROP TABLE mindshare.mindshare_post;


CREATE TABLE mindshare.mindshare_post (

    -- Core IDs (INT8 per our previous discussion)
    post_id         INT8            NOT NULL,
    user_x_id       INT8            NOT NULL,

    -- Source flags: which pipeline ingested this post
    source_m        BOOL            NOT NULL DEFAULT false,  -- mindshare
    source_n        BOOL            NOT NULL DEFAULT false,  -- nucleus
    source_u        BOOL            NOT NULL DEFAULT false,  -- user

    -- Keywords as array instead of single text
    project_keywords TEXT[]         NULL,

    -- Post content
    full_text       TEXT            NOT NULL,

    -- Relationships
    retweeted_post_id   INT8        NULL,
    replied_post_id     INT8        NULL,
    quoted_post_id      INT8        NULL,
    root_post_id        INT8        NULL,

    -- Computed type flags (CockroachDB supports STORED generated columns)
    is_retweet  BOOL GENERATED ALWAYS AS (retweeted_post_id IS NOT NULL) STORED NOT NULL,
    is_reply    BOOL GENERATED ALWAYS AS (replied_post_id IS NOT NULL) STORED NOT NULL,
    is_quote    BOOL GENERATED ALWAYS AS (quoted_post_id IS NOT NULL) STORED NOT NULL,
    is_post     BOOL GENERATED ALWAYS AS (
                    retweeted_post_id IS NULL
                    AND replied_post_id IS NULL
                    AND quoted_post_id IS NULL
                ) STORED NOT NULL,

    -- Engagement metrics
    view_count      INT4            NOT NULL DEFAULT 0,
    reply_count     INT4            NOT NULL DEFAULT 0,
    retweet_count   INT4            NOT NULL DEFAULT 0,
    quote_count     INT4            NOT NULL DEFAULT 0,
    favorite_count  INT4            NOT NULL DEFAULT 0,

    -- Scoring (only mindshare + nucleus have these; null for user posts)
    sentiment_score DECIMAL(3, 2)   NULL,
    sentiment_label VARCHAR(20)     NULL,

    -- Rich content
    entities        JSONB           NULL,

    -- Timestamps
    post_created_at TIMESTAMPTZ     NOT NULL,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT current_timestamp(),
    updated_at      TIMESTAMPTZ     NULL     DEFAULT current_timestamp()

);

-- mindshare.mindshare_project definition

-- Drop table

-- DROP TABLE mindshare.mindshare_project;



CREATE TABLE mindshare.mindshare_project (
    project_name STRING(100) NOT NULL,
    description STRING NOT NULL,
    start_ts INT8 NULL,
    end_ts INT8 NULL,
    valid_keywords JSONB NOT NULL,
    status BOOL NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp(),
    updated_at TIMESTAMPTZ NULL DEFAULT current_timestamp()
);




-- mindshare.post_content_signal definition

-- Drop table

-- DROP TABLE mindshare.post_content_signal;


CREATE TABLE mindshare.post_content_signal (

    post_id             INT8            NOT NULL,
    project_keywords    TEXT[]          NOT NULL,
    post_created_at     TIMESTAMPTZ     NOT NULL,

    -- Signal scores
    relevance                   DECIMAL(5, 2)   NULL,
    context_depth               DECIMAL(5, 2)   NULL,
    meme_communication_value    DECIMAL(5, 2)   NULL,
    visual_information_density  DECIMAL(5, 2)   NULL,
    human_signal                DECIMAL(5, 2)   NULL,
    project_focus               DECIMAL(5, 2)   NULL,
    mention_farming_risk        DECIMAL(5, 2)   NULL,
    ai_generated_probability    DECIMAL(5, 2)   NULL,
    sentiment                   DECIMAL(4, 2)   NULL,

    reason      TEXT            NULL,

    created_at  TIMESTAMPTZ     NOT NULL DEFAULT current_timestamp(),
    updated_at  TIMESTAMPTZ     NULL     DEFAULT current_timestamp()

);




-- Drop table

-- DROP TABLE mindshare.project_post_cap;




CREATE TABLE mindshare.project_post_cap (

    id                  INT8            NOT NULL DEFAULT unique_rowid(),
    project_keyword     TEXT            NOT NULL,
    leaderboard_type    TEXT            NOT NULL,
    post_cap            INT4            NOT NULL DEFAULT 5,
    cap_period          TEXT            NOT NULL DEFAULT 'week',
    cap_start_date      TIMESTAMPTZ     NULL,
    project_start_date  TIMESTAMPTZ     NULL

);


--drop  table  mindshare.post_content_score

CREATE TABLE mindshare.post_content_score (

    post_id         INT8            NOT NULL,
    -- Keywords as array instead of single text
    project_keywords TEXT[]         NULL,
    content_score   DECIMAL(5, 2)   null);
    


