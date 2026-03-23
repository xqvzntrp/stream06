------------------------------------------------------------------------------
-- ledger_verify.sql
--
-- Deterministic semantic verification
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

------------------------------------------------------------------------------
-- Verification: exactly one governing object
------------------------------------------------------------------------------

perform assert_count(
$$
select *
from v_registry_object
where object_key='EP_HELLO'
$$,
1,
'governing endpoint must exist'
);

do $$
begin
    raise notice 'VERIFICATION PASSED';
end;
$$;
