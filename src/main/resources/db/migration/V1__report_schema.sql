-- Flyway is restricted to bd_report; no other service schema is touched.
CREATE TABLE bd_report.processed_event (
    event_id uuid PRIMARY KEY,
    request_id bigint NOT NULL,
    request_version bigint NOT NULL,
    event_type varchar(40) NOT NULL,
    occurred_at timestamptz NOT NULL,
    processed_at timestamptz NOT NULL,
    payload text NOT NULL,
    payload_hash varchar(64) NOT NULL,
    source_topic varchar(249) NOT NULL,
    source_partition integer NOT NULL,
    source_offset bigint NOT NULL,
    UNIQUE (request_id, request_version)
);
CREATE INDEX processed_event_occurred_idx ON bd_report.processed_event(occurred_at);

CREATE TABLE bd_report.request_projection (
    request_id bigint PRIMARY KEY,
    request_version bigint NOT NULL CHECK (request_version > 0),
    procedure_id varchar(100) NOT NULL,
    procedure_name varchar(250) NOT NULL,
    status varchar(30) NOT NULL CHECK (status IN ('INGRESADO','ADMITIDO','EN_GESTION','EN_TERRENO','RESUELTO','RECHAZADO')),
    created_at timestamptz NOT NULL,
    last_occurred_at timestamptz NOT NULL,
    resolved_at timestamptz,
    resolution_seconds bigint CHECK (resolution_seconds >= 0),
    CHECK (last_occurred_at >= created_at),
    CHECK ((status = 'RESUELTO' AND resolved_at IS NOT NULL AND resolution_seconds IS NOT NULL)
        OR (status <> 'RESUELTO' AND resolved_at IS NULL AND resolution_seconds IS NULL))
);
CREATE INDEX projection_created_status_idx ON bd_report.request_projection(created_at, status);
CREATE INDEX projection_created_procedure_idx ON bd_report.request_projection(created_at, procedure_id);
CREATE INDEX projection_resolved_idx ON bd_report.request_projection(resolved_at) WHERE resolved_at IS NOT NULL;

CREATE TABLE bd_report.analytics_outbox (
    id uuid PRIMARY KEY,
    source_event_id uuid NOT NULL UNIQUE REFERENCES bd_report.processed_event(event_id),
    request_id bigint NOT NULL,
    request_version bigint NOT NULL,
    payload text NOT NULL,
    created_at timestamptz NOT NULL,
    available_at timestamptz NOT NULL,
    published_at timestamptz,
    attempts integer NOT NULL DEFAULT 0,
    last_error varchar(250)
);
CREATE INDEX outbox_ready_idx ON bd_report.analytics_outbox(available_at, created_at) WHERE published_at IS NULL;
CREATE INDEX outbox_request_order_idx ON bd_report.analytics_outbox(request_id, request_version) WHERE published_at IS NULL;

CREATE TABLE bd_report.failed_event (
    id bigserial PRIMARY KEY,
    source_topic varchar(249) NOT NULL,
    source_partition integer NOT NULL,
    source_offset bigint NOT NULL,
    record_key text,
    event_id varchar(64),
    request_id varchar(64),
    payload text,
    failure_type varchar(250) NOT NULL,
    failure_reason varchar(500) NOT NULL,
    failed_at timestamptz NOT NULL,
    UNIQUE(source_topic, source_partition, source_offset)
);
CREATE INDEX failed_event_time_idx ON bd_report.failed_event(failed_at);
CREATE INDEX failed_event_id_idx ON bd_report.failed_event(event_id);
