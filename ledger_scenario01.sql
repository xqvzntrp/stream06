------------------------------------------------------------------------------
-- ledger_scenario01.sql
--
-- Minimal deterministic lifecycle scenario
------------------------------------------------------------------------------

set search_path = api_ledger;

truncate api_relation restart identity cascade;
truncate api_thread_supersession restart identity cascade;
truncate api_thread restart identity cascade;
truncate api_act restart identity cascade;
truncate api_object restart identity cascade;
truncate api_participant restart identity cascade;
truncate api_stream restart identity cascade;

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
