create table payout
(
    id                  uuid         not null primary key,
    idempotency_key     varchar(100) not null,
    request_fingerprint varchar(64)  not null,
    bank_code           varchar(11)  not null,
    account_number      varchar(34)  not null,
    account_holder      varchar(70)  not null,
    amount_minor        bigint       not null,
    currency            varchar(3)   not null,
    status              varchar(16)  not null,
    bank_reference      varchar(64),
    failure_reason      varchar(500),
    created_at          timestamptz  not null,
    updated_at          timestamptz  not null,
    version             bigint       not null,
    constraint uk_payout_idempotency_key unique (idempotency_key),
    constraint ck_payout_amount_positive check (amount_minor > 0),
    constraint ck_payout_status check (status in ('REQUESTED', 'SENDING', 'SENT', 'SETTLED', 'REJECTED', 'FAILED', 'UNKNOWN'))
);

create index ix_payout_created_at on payout (created_at);

-- Written in the same transaction as the payout; the relay publishes rows where published_at is null.
create table outbox_message
(
    id           uuid         not null primary key,
    topic        varchar(200) not null,
    message_key  varchar(200) not null,
    payload      text         not null,
    created_at   timestamptz  not null,
    published_at timestamptz
);

create index ix_outbox_message_unpublished on outbox_message (created_at, id) where published_at is null;

-- One row per (consumer, message id). The primary key is the deduplication guarantee.
create table processed_message
(
    consumer     varchar(100) not null,
    message_id   varchar(100) not null,
    processed_at timestamptz  not null,
    primary key (consumer, message_id)
);
