-- Tiny simple table just for the demo
create table some_table (
    id bigint primary key generated always as identity,
    -- The load generator will update this a lot
    name text,
    data json
);

create trigger enforce_updating_version_info
    after update on some_table
    referencing old table as updated
    execute procedure update_version_info();

-- A table to track syncing out of can totally have these columns within the tracked table
-- (e.g. just having these as columns on `some_table`), but an indexed column that is always
-- updated (like last_modified_txid) will make sure HOT updates of the table can never happen.
-- Since we're likely to have this outside the main table in production use cases, so does
-- this example. But it's _not_ a requirement.

create table version_info (
    id bigint primary key references some_table(id) on update cascade on delete cascade,

    -- Version is not the same as txid. This is a plain counter. Counter-intuitively, a tx with a lower
    -- txid can change a node that was created after the tx started, so while version will strictly go up,
    -- last_modified_txid can decrease!
    version int not null default 1,
    last_modified_txid bigint not null default adjusted_txid_current(),
    last_modified timestamptz not null default now()
);

-- Populate with random data
insert into some_table (name, data)
    select 'generated-' || random(), json_build_object('random', random())
    from generate_series(1, 30*1000);

insert into version_info(id)
select id from some_table;


-- the id here is used for sorting (more as in following the index in its sorted order),
-- in case of >batch_size updates within the same txid
create index on version_info(last_modified_txid asc, id asc);

-- We do not specify anything with the payload other than that something changed. The listener will figure out
-- what. We don't want a lot of unique-looking notifications. Only one notification will be delivered per
-- notifying transaction that has the same channel+payload.
create trigger version_info_updated
-- We'll trigger even if the update doesn't do anything. Most will,
-- so we don't bother checking since the price of asking for no changes is low
after update or insert on version_info
for each statement execute procedure notify_on_update('table_changed', 'version_info');
