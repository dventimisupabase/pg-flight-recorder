-- oltp_balanced.sql: 50% read, 50% write workload
-- Usage: pgbench -f scenarios/oltp_balanced.sql -c 50 -T 600

\set aid random(1, 100000 * :scale)
\set bid random(1, 1 * :scale)
\set tid random(1, 10 * :scale)
\set delta random(-5000, 5000)
\set coin random(1, 100)

-- 50% read operations
begin;
select abalance
from pgbench_accounts
where aid = :aid;
commit;

-- 50% write operations (TPC-B style transaction)
\if :coin <= 50
begin;
update pgbench_accounts
set abalance = abalance + :delta
where aid = :aid;

select abalance
from pgbench_accounts
where aid = :aid;

update pgbench_tellers
set tbalance = tbalance + :delta
where tid = :tid;

update pgbench_branches
set bbalance = bbalance + :delta
where bid = :bid;

insert into pgbench_history (tid, bid, aid, delta, mtime)
values (:tid, :bid, :aid, :delta, current_timestamp);
commit;
\endif
