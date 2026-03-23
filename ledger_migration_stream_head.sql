------------------------------------------------------------------------------
-- ledger_migration_stream_head.sql
--
-- Add deterministic stream append heads and backfill them for existing data.
------------------------------------------------------------------------------

set search_path = api_ledger;

create table if not exists api_stream_head (
    stream_id     bigint primary key references api_stream(stream_id) on delete cascade,
    next_act_seq  bigint not null
        check (next_act_seq >= 1)
);

insert into api_stream_head(stream_id, next_act_seq)
select
    s.stream_id,
    coalesce(max(a.act_seq) + 1, 1) as next_act_seq
from api_stream s
left join api_act a
  on a.stream_id = s.stream_id
left join api_stream_head h
  on h.stream_id = s.stream_id
where h.stream_id is null
group by s.stream_id;

create or replace function api_stream_insert_head()
returns trigger
language plpgsql
as $$
begin
    insert into api_stream_head(stream_id, next_act_seq)
    values (new.stream_id, 1);

    return new;
end;
$$;

drop trigger if exists api_stream_insert_head_trg on api_stream;

create trigger api_stream_insert_head_trg
after insert on api_stream
for each row
execute function api_stream_insert_head();
