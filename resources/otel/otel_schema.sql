CREATE TEXT SEARCH DICTIONARY IF NOT EXISTS otel_body_dict (
    template  = 'segmentation',
    case      = 'lower',
    break     = 'alpha',
    frequency = true,
    position  = true,
    norm      = true
);

CREATE TABLE IF NOT EXISTS otel_logs (
    "timestamp"         TIMESTAMP_NS NOT NULL,
    observed_timestamp  TIMESTAMP_NS,
    trace_id            VARCHAR,
    span_id             VARCHAR,
    trace_flags         INTEGER,
    severity_text       VARCHAR,
    severity_number     SMALLINT,
    service_name        VARCHAR,
    event_name          VARCHAR,
    body                VARCHAR,
    resource_schema_url VARCHAR,
    scope_schema_url    VARCHAR,
    scope_name          VARCHAR,
    scope_version       VARCHAR,
    resource_attributes JSON,
    scope_attributes    JSON,
    log_attributes      JSON
) WITH (storage = 'search', optimize_top_k = 'bm25(1.2, 0.75)');

CREATE INDEX IF NOT EXISTS otel_logs_idx ON otel_logs USING inverted (
    "timestamp",
    body            otel_body_dict,
    severity_text,
    severity_number,
    service_name,
    event_name,
    trace_id
);

CREATE TABLE IF NOT EXISTS otel_traces (
    "timestamp"         TIMESTAMP_NS NOT NULL,
    end_timestamp       TIMESTAMP_NS,
    trace_id            VARCHAR NOT NULL,
    span_id             VARCHAR NOT NULL,
    parent_span_id      VARCHAR,
    trace_state         VARCHAR,
    span_name           VARCHAR,
    span_kind           VARCHAR,
    service_name        VARCHAR,
    duration_ns         BIGINT,
    status_code         VARCHAR,
    status_message      VARCHAR,
    resource_schema_url VARCHAR,
    scope_schema_url    VARCHAR,
    scope_name          VARCHAR,
    scope_version       VARCHAR,
    resource_attributes JSON,
    scope_attributes    JSON,
    span_attributes     JSON,
    events              JSON,
    links               JSON,
    event_names         VARCHAR[],
    link_trace_ids      VARCHAR[]
) WITH (storage = 'search');

CREATE INDEX IF NOT EXISTS otel_traces_idx ON otel_traces USING inverted (
    "timestamp",
    trace_id,
    span_id,
    span_name,
    span_kind,
    service_name,
    status_code,
    duration_ns,
    event_names,
    link_trace_ids
);

CREATE TABLE IF NOT EXISTS otel_metrics_gauge (
    "timestamp"         TIMESTAMP_NS NOT NULL,
    start_timestamp     TIMESTAMP_NS,
    service_name        VARCHAR,
    metric_name         VARCHAR NOT NULL,
    metric_description  VARCHAR,
    metric_unit         VARCHAR,
    resource_schema_url VARCHAR,
    scope_schema_url    VARCHAR,
    scope_name          VARCHAR,
    scope_version       VARCHAR,
    resource_attributes JSON,
    scope_attributes    JSON,
    attributes          JSON,
    flags               INTEGER,
    value               DOUBLE PRECISION,
    exemplars           JSON
) WITH (storage = 'search');

CREATE INDEX IF NOT EXISTS otel_metrics_gauge_idx ON otel_metrics_gauge
    USING inverted ("timestamp", metric_name, service_name);

CREATE TABLE IF NOT EXISTS otel_metrics_sum (
    "timestamp"             TIMESTAMP_NS NOT NULL,
    start_timestamp         TIMESTAMP_NS,
    service_name            VARCHAR,
    metric_name             VARCHAR NOT NULL,
    metric_description      VARCHAR,
    metric_unit             VARCHAR,
    resource_schema_url     VARCHAR,
    scope_schema_url        VARCHAR,
    scope_name              VARCHAR,
    scope_version           VARCHAR,
    resource_attributes     JSON,
    scope_attributes        JSON,
    attributes              JSON,
    flags                   INTEGER,
    value                   DOUBLE PRECISION,
    aggregation_temporality VARCHAR,
    is_monotonic            BOOLEAN,
    exemplars               JSON
) WITH (storage = 'search');

CREATE INDEX IF NOT EXISTS otel_metrics_sum_idx ON otel_metrics_sum
    USING inverted ("timestamp", metric_name, service_name);

CREATE TABLE IF NOT EXISTS otel_metrics_histogram (
    "timestamp"             TIMESTAMP_NS NOT NULL,
    start_timestamp         TIMESTAMP_NS,
    service_name            VARCHAR,
    metric_name             VARCHAR NOT NULL,
    metric_description      VARCHAR,
    metric_unit             VARCHAR,
    resource_schema_url     VARCHAR,
    scope_schema_url        VARCHAR,
    scope_name              VARCHAR,
    scope_version           VARCHAR,
    resource_attributes     JSON,
    scope_attributes        JSON,
    attributes              JSON,
    flags                   INTEGER,
    count                   BIGINT,
    sum                     DOUBLE PRECISION,
    bucket_counts           BIGINT[],
    explicit_bounds         DOUBLE PRECISION[],
    bucket_values           DOUBLE PRECISION[],
    min                     DOUBLE PRECISION,
    max                     DOUBLE PRECISION,
    aggregation_temporality VARCHAR,
    exemplars               JSON
) WITH (storage = 'search');

CREATE INDEX IF NOT EXISTS otel_metrics_histogram_idx ON otel_metrics_histogram
    USING inverted ("timestamp", metric_name, service_name);

CREATE TABLE IF NOT EXISTS otel_metrics_exponential_histogram (
    "timestamp"             TIMESTAMP_NS NOT NULL,
    start_timestamp         TIMESTAMP_NS,
    service_name            VARCHAR,
    metric_name             VARCHAR NOT NULL,
    metric_description      VARCHAR,
    metric_unit             VARCHAR,
    resource_schema_url     VARCHAR,
    scope_schema_url        VARCHAR,
    scope_name              VARCHAR,
    scope_version           VARCHAR,
    resource_attributes     JSON,
    scope_attributes        JSON,
    attributes              JSON,
    flags                   INTEGER,
    count                   BIGINT,
    sum                     DOUBLE PRECISION,
    scale                   INTEGER,
    zero_count              BIGINT,
    positive_offset         INTEGER,
    positive_bucket_counts  BIGINT[],
    negative_offset         INTEGER,
    negative_bucket_counts  BIGINT[],
    min                     DOUBLE PRECISION,
    max                     DOUBLE PRECISION,
    aggregation_temporality VARCHAR,
    exemplars               JSON
) WITH (storage = 'search');

CREATE INDEX IF NOT EXISTS otel_metrics_exponential_histogram_idx
    ON otel_metrics_exponential_histogram
    USING inverted ("timestamp", metric_name, service_name);

CREATE TABLE IF NOT EXISTS otel_metrics_summary (
    "timestamp"         TIMESTAMP_NS NOT NULL,
    start_timestamp     TIMESTAMP_NS,
    service_name        VARCHAR,
    metric_name         VARCHAR NOT NULL,
    metric_description  VARCHAR,
    metric_unit         VARCHAR,
    resource_schema_url VARCHAR,
    scope_schema_url    VARCHAR,
    scope_name          VARCHAR,
    scope_version       VARCHAR,
    resource_attributes JSON,
    scope_attributes    JSON,
    attributes          JSON,
    flags               INTEGER,
    count               BIGINT,
    sum                 DOUBLE PRECISION,
    quantiles           DOUBLE PRECISION[],
    values              DOUBLE PRECISION[]
) WITH (storage = 'search');

CREATE INDEX IF NOT EXISTS otel_metrics_summary_idx ON otel_metrics_summary
    USING inverted ("timestamp", metric_name, service_name);
