------------------------------------------------------------------------------
-- ledger_tables.sql
--
-- Collaboration Ledger Protocol Runtime
-- Minimal executable semantic substrate
--
-- This file installs ONLY base tables + constraints.
-- No procedures.
-- No views.
--
-- Safe to run on clean database.
------------------------------------------------------------------------------

drop schema if exists api_ledger cascade;
create schema api_ledger;

set search_path = api_ledger;

------------------------------------------------------------------------------
-- STREAM
------------------------------------------------------------------------------

create table api_stream (
    stream_id      bigserial primary key,
    stream_code    text not null unique,
    stream_title   text not null,
    created_ts     timestamptz not null default now(),

    constraint api_stream_code_ck
        check (stream_code = upper(stream_code))
);

------------------------------------------------------------------------------
-- PARTICIPANT
------------------------------------------------------------------------------

create table api_participant (
    participant_id   bigserial primary key,
    participant_code text not null unique,
    display_name     text not null,
    created_ts       timestamptz not null default now(),

    constraint api_participant_code_ck
        check (participant_code = upper(participant_code))
);

------------------------------------------------------------------------------
-- SEMANTIC OBJECT
------------------------------------------------------------------------------

create table api_object (
    object_id     bigserial primary key,
    stream_id     bigint not null references api_stream(stream_id),
    object_kind   text not null,
    object_key    text not null,
    object_name   text not null,
    created_ts    timestamptz not null default now(),

    constraint api_object_kind_ck
        check (object_kind = upper(object_kind)),

    constraint api_object_stream_uq
        unique (stream_id, object_kind, object_key)
);

create index api_object_stream_ix
    on api_object (stream_id);

------------------------------------------------------------------------------
-- RESPONSIBILITY THREAD
------------------------------------------------------------------------------

create table api_thread (
    thread_id                bigserial primary key,
    stream_id                bigint not null references api_stream(stream_id),
    object_id                bigint not null references api_object(object_id),

    opened_by_act_id         bigint,
    opened_by_participant_id bigint references api_participant(participant_id),
    opened_ts                timestamptz not null default now(),

    closed_by_act_id         bigint,
    closed_ts                timestamptz,
    closure_type             text,

    constraint api_thread_closure_ck
        check (
            closure_type in ('FULFILL','RESTART')
            or closure_type is null
        ),

    constraint api_thread_closed_pair_ck
        check (
            (closed_by_act_id is null and closed_ts is null and closure_type is null)
            or
            (closed_by_act_id is not null and closed_ts is not null and closure_type is not null)
        )
);

create index api_thread_object_ix
    on api_thread (object_id);

------------------------------------------------------------------------------
-- LEDGER ACT
------------------------------------------------------------------------------

create table api_act (
    act_id         bigserial primary key,
    stream_id      bigint not null references api_stream(stream_id),
    participant_id bigint not null references api_participant(participant_id),
    object_id      bigint not null references api_object(object_id),
    act_seq        bigint not null,
    act_type       text not null,
    payload_json   jsonb,
    thread_id      bigint,
    created_ts     timestamptz not null default now(),

    constraint api_act_stream_seq_uq
        unique (stream_id, act_seq),

    constraint api_act_type_ck
        check (act_type in (
            'COMMIT',
            'FULFILL',
            'RESTART',
            'SUPERSEDE',
            'RELATE',
            'ATTRIBUTE'
        ))
);

create index api_act_object_ix
    on api_act (object_id);

create index api_act_thread_ix
    on api_act (thread_id);

------------------------------------------------------------------------------
-- GOVERNANCE: THREAD SUPERSESSION
------------------------------------------------------------------------------

create table api_thread_supersession (
    thread_supersession_id bigserial primary key,
    stream_id              bigint not null references api_stream(stream_id),
    superseding_thread_id  bigint not null references api_thread(thread_id),
    superseded_thread_id   bigint not null references api_thread(thread_id),
    created_by_act_id      bigint,
    created_ts             timestamptz not null default now(),

    constraint api_thread_supersession_pair_uq
        unique (superseding_thread_id, superseded_thread_id),

    constraint api_thread_supersession_no_self_ck
        check (superseding_thread_id <> superseded_thread_id)
);

------------------------------------------------------------------------------
-- RELATIONSHIP
------------------------------------------------------------------------------

create table api_relation (
    relation_id        bigserial primary key,
    stream_id          bigint not null references api_stream(stream_id),
    source_object_id   bigint not null references api_object(object_id),
    relation_type      text not null,
    target_object_id   bigint not null references api_object(object_id),
    ordinal_no         integer,
    thread_id          bigint not null references api_thread(thread_id),
    created_by_act_id  bigint,
    created_ts         timestamptz not null default now()
);

create index api_relation_source_ix
    on api_relation (source_object_id);

create index api_relation_thread_ix
    on api_relation (thread_id);

------------------------------------------------------------------------------
-- OBJECT ATTRIBUTE
------------------------------------------------------------------------------

create table api_object_attr (
    object_attr_id     bigserial primary key,
    object_id          bigint not null references api_object(object_id),
    attr_name          text not null,
    attr_value         text,
    value_type         text,
    thread_id          bigint not null references api_thread(thread_id),
    created_by_act_id  bigint,
    created_ts         timestamptz not null default now()
);

create index api_object_attr_object_ix
    on api_object_attr (object_id);

------------------------------------------------------------------------------
-- Deferred FK wiring (after all tables exist)
------------------------------------------------------------------------------

alter table api_thread
    add constraint api_thread_open_act_fk
    foreign key (opened_by_act_id)
    references api_act(act_id);

alter table api_thread
    add constraint api_thread_close_act_fk
    foreign key (closed_by_act_id)
    references api_act(act_id);

alter table api_act
    add constraint api_act_thread_fk
    foreign key (thread_id)
    references api_thread(thread_id);

------------------------------------------------------------------------------
-- End of table schema
------------------------------------------------------------------------------
