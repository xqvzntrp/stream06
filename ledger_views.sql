------------------------------------------------------------------------------
-- ledger_views.sql
--
-- Deterministic interpretation layer
-- No mutations.
-- Pure semantic derivation.
------------------------------------------------------------------------------

set search_path = api_ledger;

------------------------------------------------------------------------------
-- THREAD STATUS
------------------------------------------------------------------------------

create or replace view v_thread_status as
select
    t.thread_id,
    t.stream_id,
    t.object_id,
    t.opened_ts,
    t.closed_ts,
    t.closure_type,

    case
        when t.closure_type is null then 'OPEN'
        when t.closure_type = 'FULFILL' then 'ACCEPTED'
        when t.closure_type = 'RESTART' then 'RESTARTED'
        else 'UNKNOWN'
    end as thread_status
from api_thread t;

------------------------------------------------------------------------------
-- ACCEPTED THREADS
------------------------------------------------------------------------------

create or replace view v_accepted_thread as
select *
from v_thread_status
where thread_status = 'ACCEPTED';

------------------------------------------------------------------------------
-- SUPERSEDED THREADS
------------------------------------------------------------------------------

create or replace view v_superseded_thread as
select
    stream_id,
    superseded_thread_id as thread_id
from api_thread_supersession;

------------------------------------------------------------------------------
-- CANDIDATE GOVERNING THREAD
------------------------------------------------------------------------------

create or replace view v_candidate_governing_thread as
select t.*
from v_accepted_thread t
left join v_superseded_thread s
  on s.thread_id = t.thread_id
 and s.stream_id = t.stream_id
where s.thread_id is null;

------------------------------------------------------------------------------
-- GOVERNING THREAD COUNT
------------------------------------------------------------------------------

create or replace view v_governing_thread_counts as
select
    stream_id,
    object_id,
    count(*) as governing_thread_count
from v_candidate_governing_thread
group by stream_id, object_id;

------------------------------------------------------------------------------
-- GOVERNING THREAD (UNAMBIGUOUS)
------------------------------------------------------------------------------

create or replace view v_governing_thread as
select t.*
from v_candidate_governing_thread t
join v_governing_thread_counts c
  on c.stream_id = t.stream_id
 and c.object_id = t.object_id
where c.governing_thread_count = 1;

------------------------------------------------------------------------------
-- GOVERNING OBJECT REGISTRY
------------------------------------------------------------------------------

create or replace view v_registry_object as
select
    g.stream_id,
    g.object_id,
    o.object_kind,
    o.object_key,
    o.object_name,
    g.thread_id as governing_thread_id
from v_governing_thread g
join api_object o
  on o.object_id = g.object_id;

------------------------------------------------------------------------------
-- GOVERNING RELATION REGISTRY
------------------------------------------------------------------------------

create or replace view v_registry_relation as
select
    r.stream_id,
    r.relation_id,
    r.source_object_id,
    r.relation_type,
    r.target_object_id,
    r.ordinal_no,
    r.thread_id
from api_relation r
join v_governing_thread gt
  on gt.thread_id = r.thread_id;

------------------------------------------------------------------------------
-- GOVERNANCE AMBIGUITY
------------------------------------------------------------------------------

create or replace view v_ambiguous_object as
select *
from v_governing_thread_counts
where governing_thread_count > 1;

------------------------------------------------------------------------------
-- End of view layer
------------------------------------------------------------------------------
