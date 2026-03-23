------------------------------------------------------------------------------
-- ledger_verify.sql
--
-- Deterministic protocol invariant verification
------------------------------------------------------------------------------

set search_path = api_ledger;

create or replace function assert_count(
    p_sql text,
    p_expected bigint,
    p_msg text
)
returns void
language plpgsql
as $$
declare v bigint;
begin
    execute format('select count(*) from (%s) q', p_sql)
    into v;

    if v <> p_expected then
        raise exception
            'ASSERT FAILED: %, expected %, got %',
            p_msg, p_expected, v;
    end if;
end;
$$;

create or replace function assert_zero(
    p_sql text,
    p_msg text
)
returns void
language plpgsql
as $$
begin
    perform assert_count(p_sql, 0, p_msg);
end;
$$;

------------------------------------------------------------------------------
-- Protocol invariants
------------------------------------------------------------------------------

do $$
begin
    perform assert_zero(
    $query$
    select *
    from v_ambiguous_object
    $query$,
    'no object may have multiple governing threads'
    );

    perform assert_zero(
    $query$
    select a.stream_id, a.object_id
    from (
        select stream_id, object_id
        from v_accepted_thread
        group by stream_id, object_id
    ) a
    left join v_governing_thread g
      on g.stream_id = a.stream_id
     and g.object_id = a.object_id
    where g.thread_id is null
    $query$,
    'accepted objects must converge to a governing thread'
    );

    perform assert_zero(
    $query$
    select open_t.stream_id, open_t.object_id, open_t.thread_id
    from v_thread_status open_t
    join v_governing_thread g
      on g.stream_id = open_t.stream_id
     and g.object_id = open_t.object_id
    where open_t.thread_status = 'OPEN'
    $query$,
    'no open thread may coexist with a governing thread for the same object'
    );

    perform assert_zero(
    $query$
    select r.relation_id
    from v_registry_relation r
    left join v_governing_thread g
      on g.thread_id = r.thread_id
    where g.thread_id is null
    $query$,
    'governed relations must be attached to governing threads'
    );

    perform assert_zero(
    $query$
    select r.relation_id
    from api_relation r
    join v_thread_status ts
      on ts.thread_id = r.thread_id
    where ts.thread_status = 'RESTARTED'
    $query$,
    'no relation may remain on a restarted thread'
    );

    perform assert_zero(
    $query$
    select oa.object_attr_id
    from api_object_attr oa
    left join v_governing_thread g
      on g.thread_id = oa.thread_id
    where g.thread_id is null
    $query$,
    'no attribute may remain on a non-governing thread'
    );

    perform assert_zero(
    $query$
    with seqs as (
        select
            stream_id,
            act_seq,
            row_number() over (partition by stream_id order by act_seq) as expected_seq
        from api_act
    )
    select *
    from seqs
    where act_seq <> expected_seq
    $query$,
    'act_seq must be dense per stream'
    );

    raise notice 'VERIFICATION PASSED';
end;
$$;
