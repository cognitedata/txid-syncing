-- If we pgdump and restore in a different cluster, this will need adjustment.
-- The txid concept will be documented and explained thoroughly somewhere sometime
-- well before this hits prod.
-- "stable" prevents this function from being used in an expression index, which it
-- shouldn't be, while letting Postgres sufficiently inline it. It's trivially
-- leakproof and parallel safe
create or replace function txid_offset() returns bigint immutable leakproof parallel safe language sql as $$ select 0::bigint $$;

-- Adjusted for pgdumps/restores to different clusters, what's the current txid?
create or replace function adjusted_txid_current() returns bigint language sql as $$ select txid_offset() + txid_current() $$;

-- Applications that write to or read from tables where a correct txid is imporant to maintain need to call this first.
-- It only needs to be called once after a pg_restore, but as it's only going to cost a few milliseconds, it's
-- probably a good idea to just run whenever an app connects to a database-dsn for the first time. 
create or replace function configure_txid_offset()
returns bigint
volatile
security definer -- Even read only users need to be able to set this, in case they're first after a pg_restore
language plpgsql
as $$
declare max_txid bigint;
declare new_txid_offset bigint;
begin
    -- This is a bit dirty, but we find all tables that have a txid-column, then
    -- we find the highest txid of all of them. That's the highest txid we have
    -- ever saved, so we want the diff of txid_current and that to be an offset
    -- quote_ident is used just in case the table name has SQL control characters
    execute
        coalesce(
            (
                select 'select max(txid)::bigint as max_txid from (select max(last_modified_txid) as txid from ' ||
                array_to_string(array_agg(quote_ident(table_name)),' union select max(last_modified_txid) as txid from ') ||
                ') txids'
                from information_schema.columns
                where column_name = 'last_modified_txid'
            ),
            -- There are no tables with a txid column
            'select 0'
        )
    into max_txid;

    -- The txid_offset is used to offset the physical txid (which might reset when the
    -- database is recovered from a backup). Even when we recover a backup, we need txids
    -- to be monotonically increasing, which this offset ensures.
    new_txid_offset := coalesce(greatest(0, max_txid - txid_current() + 1, txid_offset()), 0);

    -- Update the function to the determined necessary offset. It can only be set higher
    if new_txid_offset > txid_offset() then
        execute 'create or replace function public.txid_offset() returns bigint immutable leakproof parallel safe language sql as $func$ select (' || new_txid_offset || ')::bigint $func$';
    end if;
    return new_txid_offset;
end; $$;


create or replace function adjusted_txid_snapshot() returns record
stable leakproof parallel safe language sql as $$
    select 
        (select coalesce(array_agg(txid), ARRAY[]::bigint[]) from (select txid_offset() + txid_snapshot_xip(txid_current_snapshot())) as _(txid)) as xip_list,
        (select txid_offset() + txid_snapshot_xmin(txid_current_snapshot())) as xmin,
        (select txid_offset() + txid_snapshot_xmax(txid_current_snapshot())) as xmax,
        (select txid_offset() + txid_current()) as txid_current;
$$;


create or replace function adjusted_txid_snapshot_as_json() returns json
stable leakproof parallel safe language sql as $$
    select to_json(adjusted_txid_snapshot());
$$;


create or replace function notify_on_update()
returns trigger language plpgsql as $$
begin
    perform pg_notify(/* channel = */ TG_ARGV[0], /* payload = */ TG_ARGV[1]);
    return new;
end; $$;


create or replace function update_version_info() returns trigger as $$
begin
    update version_info set
        version = version + 1,
        last_modified_txid = adjusted_txid_current()
    from
        updated
    where
        version_info.id=updated.id and
        last_modified_txid != adjusted_txid_current();

    return new;
end; $$
language plpgsql;


create or replace function notify_on_update()
returns trigger language plpgsql as $$
begin
    perform pg_notify(/* channel = */ TG_ARGV[0], /* payload = */ TG_ARGV[1]);
    return new;
end; $$;
