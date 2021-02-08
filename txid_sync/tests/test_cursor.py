import pytest
from ..cursor import *
import re


batch_size = 1000


def snapshot(txid_current, **kw):
    return {
        "xip_list": kw.get("xip_list", []),
        "xmin": kw.get("xmin", txid_current),
        "xmax": kw.get("xmax", txid_current),
        "txid_current": txid_current
    }

def without_sql_comments(text):
    return re.sub(r'(^\s*--.*$)|(/\*.*?\*/)', '', text, flags=re.MULTILINE)

def stripboth(text):
    """ Remove leading and ending whitespace """
    return re.sub(r'^\s*|\s*$', '', text)

def normalise_whitespace(text):
    return re.sub(r'\s+', ' ', stripboth(text))

class TestCursor:

    id_counter = 0

    def assertNotExceedingBatchSizeMode(self, cursor):
        assert cursor.xid_at is None and cursor.xid_at_id is None, (
            "cursor not expected to think it's scrolling through what could "
            "not fit in a batch size"
        )

    def assertStringsEqualWithNormalisedWhitespace(self, a, b):
        assert normalise_whitespace(a) == normalise_whitespace(b)


    def row_of_txid(self, txid, id=None, version=1, priority=Priority.SCROLL):
        if id is None:
            self.id_counter += 1
            id = self.id_counter

        return dict(
            id=id,
            last_modified_txid=txid,
            priority=priority,
            version=version
        )

    def test_simplest_case_no_changes(self):
        # The cursor produced no rows
        c = Cursor(batch_size, xid_next=2).advance(
            snapshot(3, xip_list=[]), []
        )

        assert c.xid_next == 3
        assert c.xip_list == []

        self.assertNotExceedingBatchSizeMode(c)

    def test_more_changes_than_batch_size(self):
        # It shouldn't matter how we get the rows that make up more than
        # batch size, as any kind of transaction can make that happen
        batch_size = 10
        for priority in (Priority):
            rows = [
                dict(id=i, version=1, last_modified_txid=2, priority=priority)
                # + 1, as if it was exactly batch_size we'd be done
                for i in range(batch_size + 1)
            ]
            c = Cursor(batch_size, xid_next=1).advance(
                snapshot(3, xip_list=[]), rows=rows
            )

            assert c.xid_next == 3
            assert c.xip_list == []
            assert c.xid_at == 2
            assert c.xid_at_id == rows[-2]["id"]

    def test_exactly_batch_size_changes_in_a_version(self):
        batch_size = 10

        for priority in (Priority):
            rows = [
                dict(id=i, version=1, last_modified_txid=2, priority=priority)
                for i in range(batch_size)
            ]
            c = Cursor(batch_size, xid_next=1).advance(
                snapshot(3, xip_list=[]), rows=rows
            )

            assert c.xid_next == 3
            assert c.xip_list == []
            self.assertNotExceedingBatchSizeMode(c)

        # If the peek of the next batch doesn't have the same txid, then
        # we'd just progress like normal. Note that we've skipped 4
        rows.append(
            dict(id=batch_size, version=1, last_modified_txid=5, priority=Priority.SCROLL)
        )

        c = Cursor(batch_size, xid_next=1).advance(
            snapshot(6, xip_list=[]), rows=rows
        )

        assert c.xid_next == 5
        assert c.xip_list == []
        self.assertNotExceedingBatchSizeMode(c)

    def test_xip_list_behaviour(self):
        batch_size = 10

        # 2 eventually finished and did not impact us, 3
        # has not committed yet
        c = Cursor(batch_size, xid_next=5, xip_list=[2, 3, 7]).advance(
            snapshot(8, xip_list=[3, 7]), rows=[
                self.row_of_txid(5)
            ]
        )

        # Highest seen row was 5, and since the batch wasn't full, we know
        # we saw all the changes 5 could have done
        assert c.xid_next == 6

        # 3 must be carried forward, xid_next progressed past it and it remains
        # active. No need to carry 7, since xid_next is less than that
        assert c.xip_list == [3]

        self.assertNotExceedingBatchSizeMode(c)

        c = c.advance(snapshot(10, xip_list=[3, 7]), [
            self.row_of_txid(9)
        ])
        assert c.xid_next == 10
        assert c.xip_list == [3, 7]

        c = c.advance(snapshot(11, xip_list=[7]), [
            self.row_of_txid(3)
        ])
        assert c.xid_next == 10
        assert c.xip_list == [7]

        c = c.advance(snapshot(12, xip_list=[]), [])
        assert c.xid_next == 11
        assert c.xip_list == []

        # While we could advance to 15 and keep the xip_list, 
        # xid_next is just advanced by one, and we get to drop the
        # xip_list
        c2 = c.advance(snapshot(15, xip_list=[13, 14]), [])
        assert c2.xid_next == 12
        assert c2.xip_list == []

        # If instead we got a row past what's in the xip list,
        # we clearly need to keep track of them
        c2 = c.advance(snapshot(16, xip_list=[13, 14]), [
            self.row_of_txid(15)
        ])
        assert c2.xid_next == 16
        assert c2.xip_list == [13, 14]        

    def test_sql_for_cursor(self):
        # Pretend 1 recently committed and changed a bit of everything
        # after running for a long while, we dunno yet about 2 and 3,
        # but we got all the way to 100 before 1 committed, so we have
        # all three cases.
        # The test is blunt, but will do for now.. (No doubt we'll change
        # the SQL when it gets a bit past POC, no point in having comments
        # and debug context in it..)
        c = Cursor(
            batch_size=1000,
            xid_at=1,
            xid_next=100,
            xid_at_id=batch_size + 1,
            xip_list=[2, 3]
        )
        sql = without_sql_comments(c.get_changes_cte())
        self.assertStringsEqualWithNormalisedWhitespace(sql, without_sql_comments("""
/* <Cursor: batch_size=1000, xid_at=1, xid_at_id=1001, xip_list=[2, 3], xid_next=100, number_of_partitions=1, partition_id=0> (previous: None) */
with changes as (
    select priority, last_modified_txid, id, version from (
        select * from (

select 0, last_modified_txid, id, version
from version_info
-- Finishing what's based on (xid_at, xid_at_id) first
where last_modified_txid = 1 and id > 1001
order by last_modified_txid asc, id asc
limit 1001

) as _0(priority, last_modified_txid, id, version)

union all

select * from (

select 1, last_modified_txid, id, version
from version_info
-- Then carry-overed XIPs less than xid_next that have completed
where last_modified_txid = ANY(ARRAY[2, 3])
order by last_modified_txid asc, id asc
limit 1001

) as _1(priority, last_modified_txid, id, version)

union all

select * from (

select 2, last_modified_txid, id, version
from version_info
-- And lastly we continue from xid_next
where last_modified_txid >= 100
order by last_modified_txid asc, id asc
limit 1001

) as _2(priority, last_modified_txid, id, version)
        limit 1001
    ) _
    order by priority asc, last_modified_txid asc, id asc
)
"""))