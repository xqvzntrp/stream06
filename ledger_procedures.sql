------------------------------------------------------------------------------
-- ledger_procedures.sql
--
-- Deterministic protocol mutation API
-- No interpretation logic.
------------------------------------------------------------------------------

set search_path = api_ledger;

------------------------------------------------------------------------------
-- record_commit
------------------------------------------------------------------------------

create or replace function record_commit(
    p_stream_code      text,
    p_participant_code text,
    p_object_kind      text,
    p_object_key       text,
    p_payload_json     jsonb default null
)
returns table (
    act_id bigint,
    act_seq bigint,
    thread_id bigint
)
language plpgsql
as $$
declare
    v_stream_id bigint;
    v_participant_id bigint;
    v_object_id bigint;
    v_next_seq bigint;
begin

    select stream_id into v_stream_id
    from api_stream
    where stream_code = upper(p_stream_code);

    if v_stream_id is null then
        raise exception 'unknown stream';
    end if;

    select participant_id into v_participant_id
    from api_participant
    where participant_code = upper(p_participant_code);

    select object_id into v_object_id
    from api_object
    where stream_id = v_stream_id
      and object_kind = upper(p_object_kind)
      and object_key = upper(p_object_key);

    perform 1 from api_stream where stream_id = v_stream_id for update;

    select coalesce(max(a.act_seq),0)+1
    into v_next_seq
    from api_act a
    where a.stream_id = v_stream_id;

    insert into api_act(
        stream_id, participant_id, object_id,
        act_seq, act_type, payload_json
    )
    values(
        v_stream_id, v_participant_id, v_object_id,
        v_next_seq, 'COMMIT', p_payload_json
    )
    returning api_act.act_id into act_id;

    act_seq := v_next_seq;

    insert into api_thread(
        stream_id,
        object_id,
        opened_by_act_id,
        opened_by_participant_id
    )
    values(
        v_stream_id,
        v_object_id,
        act_id,
        v_participant_id
    )
    returning api_thread.thread_id into thread_id;

    update api_act
    set thread_id = record_commit.thread_id
    where api_act.act_id = record_commit.act_id;

    return next;
end;
$$;

------------------------------------------------------------------------------
-- record_fulfill
------------------------------------------------------------------------------

create or replace function record_fulfill(
    p_stream_code text,
    p_participant_code text,
    p_thread_id bigint
)
returns table (
    act_id bigint,
    act_seq bigint
)
language plpgsql
as $$
declare
    v_stream_id bigint;
    v_participant_id bigint;
    v_object_id bigint;
    v_next_seq bigint;
begin

    select stream_id into v_stream_id
    from api_stream
    where stream_code = upper(p_stream_code);

    select participant_id into v_participant_id
    from api_participant
    where participant_code = upper(p_participant_code);

    select object_id into v_object_id
    from api_thread
    where thread_id = p_thread_id
    for update;

    perform 1 from api_stream where stream_id = v_stream_id for update;

    select coalesce(max(a.act_seq),0)+1
    into v_next_seq
    from api_act a
    where a.stream_id = v_stream_id;

    insert into api_act(
        stream_id, participant_id, object_id,
        act_seq, act_type, thread_id
    )
    values(
        v_stream_id, v_participant_id, v_object_id,
        v_next_seq, 'FULFILL', p_thread_id
    )
    returning api_act.act_id, api_act.act_seq
    into act_id, act_seq;

    update api_thread
    set closed_by_act_id = act_id,
        closed_ts = now(),
        closure_type = 'FULFILL'
    where thread_id = p_thread_id;

    return next;
end;
$$;

------------------------------------------------------------------------------
-- record_restart
------------------------------------------------------------------------------

create or replace function record_restart(
    p_stream_code text,
    p_participant_code text,
    p_thread_id bigint
)
returns table (
    act_id bigint,
    act_seq bigint
)
language plpgsql
as $$
declare
    v_stream_id bigint;
    v_participant_id bigint;
    v_object_id bigint;
    v_next_seq bigint;
begin

    select stream_id into v_stream_id
    from api_stream
    where stream_code = upper(p_stream_code);

    select participant_id into v_participant_id
    from api_participant
    where participant_code = upper(p_participant_code);

    select object_id into v_object_id
    from api_thread
    where thread_id = p_thread_id
    for update;

    perform 1 from api_stream where stream_id = v_stream_id for update;

    select coalesce(max(a.act_seq),0)+1
    into v_next_seq
    from api_act a
    where a.stream_id = v_stream_id;

    insert into api_act(
        stream_id, participant_id, object_id,
        act_seq, act_type, thread_id
    )
    values(
        v_stream_id, v_participant_id, v_object_id,
        v_next_seq, 'RESTART', p_thread_id
    )
    returning api_act.act_id, api_act.act_seq
    into act_id, act_seq;

    update api_thread
    set closed_by_act_id = act_id,
        closed_ts = now(),
        closure_type = 'RESTART'
    where thread_id = p_thread_id;

    return next;
end;
$$;

------------------------------------------------------------------------------
-- record_relate
------------------------------------------------------------------------------

create or replace function record_relate(
    p_stream_code text,
    p_participant_code text,
    p_thread_id bigint,
    p_source_kind text,
    p_source_key text,
    p_relation_type text,
    p_target_kind text,
    p_target_key text,
    p_ordinal_no integer default null
)
returns table (
    relation_id bigint,
    created_by_act_id bigint,
    act_seq bigint
)
language plpgsql
as $$
declare
    v_stream_id bigint;
    v_participant_id bigint;
    v_thread_stream_id bigint;
    v_thread_object_id bigint;
    v_source_object_id bigint;
    v_target_object_id bigint;
    v_next_seq bigint;
begin

    select stream_id into v_stream_id
    from api_stream
    where stream_code = upper(p_stream_code);

    if v_stream_id is null then
        raise exception 'unknown stream';
    end if;

    select participant_id into v_participant_id
    from api_participant
    where participant_code = upper(p_participant_code);

    if v_participant_id is null then
        raise exception 'unknown participant';
    end if;

    select stream_id, object_id
    into v_thread_stream_id, v_thread_object_id
    from api_thread
    where thread_id = p_thread_id
    for update;

    if v_thread_object_id is null then
        raise exception 'unknown thread';
    end if;

    if v_thread_stream_id <> v_stream_id then
        raise exception 'thread does not belong to stream';
    end if;

    select object_id into v_source_object_id
    from api_object
    where stream_id = v_stream_id
      and object_kind = upper(p_source_kind)
      and object_key = upper(p_source_key);

    if v_source_object_id is null then
        raise exception 'unknown source object';
    end if;

    select object_id into v_target_object_id
    from api_object
    where stream_id = v_stream_id
      and object_kind = upper(p_target_kind)
      and object_key = upper(p_target_key);

    if v_target_object_id is null then
        raise exception 'unknown target object';
    end if;

    if v_thread_object_id <> v_source_object_id then
        raise exception 'thread does not govern source object';
    end if;

    perform 1 from api_stream where stream_id = v_stream_id for update;

    select coalesce(max(a.act_seq),0)+1
    into v_next_seq
    from api_act a
    where a.stream_id = v_stream_id;

    insert into api_act(
        stream_id, participant_id, object_id,
        act_seq, act_type, thread_id, payload_json
    )
    values(
        v_stream_id,
        v_participant_id,
        v_source_object_id,
        v_next_seq,
        'RELATE',
        p_thread_id,
        jsonb_build_object(
            'relation_type', upper(p_relation_type),
            'target_kind', upper(p_target_kind),
            'target_key', upper(p_target_key),
            'ordinal_no', p_ordinal_no
        )
    )
    returning api_act.act_id, api_act.act_seq
    into created_by_act_id, act_seq;

    insert into api_relation(
        stream_id,
        source_object_id,
        relation_type,
        target_object_id,
        ordinal_no,
        thread_id,
        created_by_act_id
    )
    values(
        v_stream_id,
        v_source_object_id,
        upper(p_relation_type),
        v_target_object_id,
        p_ordinal_no,
        p_thread_id,
        created_by_act_id
    )
    returning api_relation.relation_id
    into relation_id;

    return next;
end;
$$;

------------------------------------------------------------------------------
-- record_supersede
------------------------------------------------------------------------------

create or replace function record_supersede(
    p_stream_code text,
    p_participant_code text,
    p_superseding_thread_id bigint,
    p_superseded_thread_id bigint
)
returns table (
    act_id bigint,
    act_seq bigint,
    thread_supersession_id bigint
)
language plpgsql
as $$
declare
    v_stream_id bigint;
    v_participant_id bigint;
    v_next_seq bigint;
    v_superseding_stream_id bigint;
    v_superseded_stream_id bigint;
    v_superseding_object_id bigint;
    v_superseded_object_id bigint;
    v_superseding_closure_type text;
    v_superseded_closure_type text;
begin

    select stream_id into v_stream_id
    from api_stream
    where stream_code = upper(p_stream_code);

    if v_stream_id is null then
        raise exception 'unknown stream';
    end if;

    select participant_id into v_participant_id
    from api_participant
    where participant_code = upper(p_participant_code);

    if v_participant_id is null then
        raise exception 'unknown participant';
    end if;

    select stream_id, object_id, closure_type
    into v_superseding_stream_id, v_superseding_object_id, v_superseding_closure_type
    from api_thread
    where thread_id = p_superseding_thread_id
    for update;

    if v_superseding_object_id is null then
        raise exception 'unknown superseding thread';
    end if;

    select stream_id, object_id, closure_type
    into v_superseded_stream_id, v_superseded_object_id, v_superseded_closure_type
    from api_thread
    where thread_id = p_superseded_thread_id
    for update;

    if v_superseded_object_id is null then
        raise exception 'unknown superseded thread';
    end if;

    if v_superseding_stream_id <> v_stream_id
       or v_superseded_stream_id <> v_stream_id then
        raise exception 'threads do not belong to stream';
    end if;

    if v_superseding_object_id <> v_superseded_object_id then
        raise exception 'threads govern different objects';
    end if;

    if v_superseding_closure_type <> 'FULFILL'
       or v_superseded_closure_type <> 'FULFILL' then
        raise exception 'supersession requires accepted threads';
    end if;

    perform 1 from api_stream where stream_id = v_stream_id for update;

    select coalesce(max(a.act_seq),0)+1
    into v_next_seq
    from api_act a
    where a.stream_id = v_stream_id;

    insert into api_act(
        stream_id, participant_id, object_id,
        act_seq, act_type, thread_id, payload_json
    )
    values(
        v_stream_id,
        v_participant_id,
        v_superseding_object_id,
        v_next_seq,
        'SUPERSEDE',
        p_superseding_thread_id,
        jsonb_build_object('superseded_thread_id', p_superseded_thread_id)
    )
    returning api_act.act_id, api_act.act_seq
    into act_id, act_seq;

    insert into api_thread_supersession(
        stream_id,
        superseding_thread_id,
        superseded_thread_id,
        created_by_act_id
    )
    values(
        v_stream_id,
        p_superseding_thread_id,
        p_superseded_thread_id,
        act_id
    )
    returning api_thread_supersession.thread_supersession_id
    into thread_supersession_id;

    return next;
end;
$$;
