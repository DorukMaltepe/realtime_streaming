CREATE TABLE wiki_stats (
    window_start timestamptz NOT NULL,
    wiki         text        NOT NULL,
    edits        bigint      NOT NULL,
    bot_edits    bigint      NOT NULL,
    PRIMARY KEY (window_start, wiki)
);

CREATE TABLE anomalies (
    window_start timestamptz      NOT NULL,
    wiki         text             NOT NULL,
    edits        bigint           NOT NULL,
    expected     double precision NOT NULL,
    z_score      double precision NOT NULL,
    PRIMARY KEY (window_start, wiki)
);
