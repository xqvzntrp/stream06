------------------------------------------------------------------------------
-- ledger_scenario01.sql
--
-- Minimal deterministic lifecycle scenario
------------------------------------------------------------------------------

set search_path = api_ledger;

truncate api_relation cascade;
truncate api_thread_supersession cascade;
truncate api_thread cascade;
truncate api_act cascade;
truncate api_object cascade;
truncate api_participant cascade;
truncate api_stream cascade;

insert into api_stream(stream_code, stream_title)
values ('API','API Stream');

insert into api_participant(participant_code, display_name)
values ('ALICE','Alice');

insert into api_object(stream_id, object_kind, object_key, object_name)
select stream_id,'ENDPOINT','EP_HELLO','Hello Endpoint'
from api_stream where stream_code='API';

insert into api_object(stream_id, object_kind, object_key, object_name)
select stream_id,'PARAMETER','P_NAME','Name Parameter'
from api_stream where stream_code='API';

select * from record_commit('API','ALICE','ENDPOINT','EP_HELLO');

select * from record_fulfill('API','ALICE',1);
