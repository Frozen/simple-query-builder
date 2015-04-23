# coding=utf-8
import unittest
from simple_query_builder.postgresql import Query, LeftJoin, RightJoin, InnerJoin, CrossJoin, Subquery, Where, WhereOr, WhereIn, \
    Join

class TestMysqlQuery(unittest.TestCase):

    class Q(Query):
        _select = None



    # def test_select(self):
    #
    #     q = Query()
    #     q._select = 1
    #     self.assertEqual("SELECT 1", q.compile())
    #     q._select = 0
    #     self.assertEqual("SELECT 0", q.compile())
    #     q._select = "bla"
    #     self.assertEqual("SELECT bla", q.compile())
    #     self.assertEqual(u"SELECT bla", q.compile())
    #     q._select = u"я"
    #     self.assertEqual(u"SELECT я", q.compile())
    #     q._select = [1, Query()]
    #     self.assertEqual(u"SELECT 1, (SELECT *)", q.compile())

    # def test_from(self):
    #     q = Query()
    #
    #     q._from = "table"
    #     self.assertEqual("SELECT * FROM table", q.compile())
    #     q._from = ["table1", "table2"]
    #     self.assertEqual("SELECT * FROM table1, table2", q.compile())
    #
    # def test_join(self):
    #
    #     q = self.LocalQuery()
    #     s = "table ON user.id=posts.user_id"
    #     q._join = "table ON user.id=posts.user_id"
    #     self.assertEqual("JOIN " + s, q.compile())
    #     q._join = LeftJoin("table", "user.id=posts.user_id")
    #     self.assertEqual("LEFT JOIN " + s, q.compile())
    #     q._join = RightJoin("table", "user.id=posts.user_id")
    #     self.assertEqual("RIGHT JOIN " + s, q.compile())
    #     q._join = InnerJoin("table", "user.id=posts.user_id")
    #     self.assertEqual("INNER JOIN " + s, q.compile())
    #     q._join = CrossJoin("table", "user.id=posts.user_id")
    #     self.assertEqual("CROSS JOIN " + s, q.compile())
    #
    #     q = self.LocalQuery()
    #     q._join = [LeftJoin("user", "user.id=posts.user_id"), RightJoin("user", "user.id=posts.user_id")]
    #     self.assertEqual("LEFT JOIN user ON user.id=posts.user_id, RIGHT JOIN user ON user.id=posts.user_id", q.compile())
    #
    #     q = self.LocalQuery()
    #     q2 = self.LocalQuery()
    #     q2._select = 1
    #     q._join = LeftJoin(Subquery(q2, "foo"), "foo.id=posts.user_id")
    #     self.assertEqual("LEFT JOIN (SELECT 1) AS foo ON foo.id=posts.user_id", q.compile())

    def test_where(self):

        q = self.Q()
        q = q.where("user.id > 1")
        self.assertEqual("WHERE (user.id > 1)", q.compile())

        q = self.Q()
        q = q.where("user.id > :id", 1)
        self.assertEqual("WHERE (user.id > :id)", q.compile())
        self.assertEqual({"id": 1}, q.bind())

        q = self.Q()
        q = q.where("user.id = :id1", 1)
        q = q.whereOr(Where("post.id = 1"), Where("post.id = :id2", 2), "post.id = 3", WhereIn("post.id IN (:values)", [3, 5, 7]))

        self.assertEqual({
            'id1': 1,
            'id2': 2
        }, q.bind())
        self.assertEqual("WHERE (user.id = :id1) AND ((post.id = 1) OR (post.id = :id2) OR (post.id = 3) OR "
                         "(post.id IN (3, 5, 7)))", q.compile())

        # test where with date and datetime
        from datetime import datetime, date

        q = self.Q()
        q = q.where("dt > :dt", date(2015, 5, 7))
        self.assertEqual({"dt": "2015-05-07"}, q.bind())

        q = self.Q()
        q = q.where("dt > :dt", datetime(2014, 10, 01, 23, 11, 1))
        self.assertEqual({"dt": "2014-10-01 23:11:01"}, q.bind())

    # @unittest.skip("incorrect test")
    def test_where_in(self):

        q = self.Q()
        q = q.whereIn("user.id IN (:ids)", [1, 3])
        self.assertEqual("WHERE (user.id IN (1, 3))", q.compile())

        # test empty
        q = self.Q()
        q  = q.whereIn("user.id", [])
        self.assertEqual("", q.compile())

        q = self.Q()
        q = q.whereIn("user.id IN (:id)", Query())
        self.assertEqual("WHERE (user.id IN (SELECT *))", q.compile())

    def test_group_by(self):

        q = self.Q()
        q = q.group_by("user.id")
        self.assertEqual("GROUP BY user.id", q.compile())

        q = self.Q()
        q = q.group_by(["user.id", "date"])
        self.assertEqual("GROUP BY user.id, date", q.compile())

        q2 = q.group_by("some.table")
        self.assertEqual("GROUP BY user.id, date, some.table", q2.compile())
        self.assertEqual("GROUP BY user.id, date", q.compile())

    def test_having(self):

        q = self.Q()
        q = q.having("count(user.id) > 0")
        self.assertEqual("HAVING count(user.id) > 0", q.compile())

        q = self.Q()
        q = q.having(["count(user.id) > 0", "date != now()"])
        q2 = q.having("some.table")
        self.assertEqual("HAVING count(user.id) > 0 AND date != now()", q.compile())
        self.assertEqual("HAVING count(user.id) > 0 AND date != now() AND some.table", q2.compile())

    def test_order_by(self):

        q = self.Q()
        q = q.order_by("user.id")
        self.assertEqual("ORDER BY user.id", q.compile())

        q = self.Q()
        q = q.order_by(["user.id", "date desc"])
        self.assertEqual("ORDER BY user.id, date desc", q.compile())

    def test_limit(self):
        q = self.Q()
        q = q.limit("10")
        self.assertEqual("LIMIT 10", q.compile())
        self.assertEqual("LIMIT 20", q.limit(20).compile())

        q = self.Q()
        q = q.limit(10).offset(20)
        self.assertEqual("LIMIT 10 OFFSET 20", q.compile())

    @unittest.skip("incorrect tests")
    def test_clone(self):

        q = self.Q()

        q.select = '1'
        q.from_ = '2'
        q._join = '3'
        q.where = '4'
        q._group_by = '5'
        q._having = '6'
        q._order_by = '7'
        q._limit = '8'

        q2 = q.clone()

        q2.select = '10'
        q2.from_ = '20'
        q2.join = '30'
        q2.where = '40'
        q2.group_by = '50'
        q2.having = '60'
        q2.order_by = '70'
        q2.limit = '80'

        self.assertEqual('1', q.select)
        self.assertEqual('2', q.from_)
        self.assertEqual('3', q._join)
        self.assertEqual('4', q.where)
        self.assertEqual('5', q._group_by)
        self.assertEqual('6', q._having)
        self.assertEqual('7', q._order_by)
        self.assertEqual('8', q._limit)


@unittest.skip("incorrect test")
class TestHard(unittest.TestCase):

    def test_1(self):

        query =u"SELECT id, (SELECT name FROM clip WHERE (id = c2.id)) " \
                "FROM clip c1 " \
                "JOIN (SELECT id FROM clip WHERE (id < 30)) AS c2 ON c1.id = c2.id " \
                "WHERE (c1.id IN (%(c1_id_0)s))" \

        q1 = Query()
        q2 = Query()
        q3 = Query()

        q1._select = ['name']
        q1._where = Where("id = c2.id")
        q1._from = "clip"

        q2._select = 'id'
        q2._from = 'clip'
        q2._where = Where('id < 30')

        q3._select = ['id', q1]
        q3._from = 'clip c1'
        q3._join = Join(Subquery(q2, 'c2'), 'c1.id = c2.id')
        q3._where = WhereIn('c1.id IN (%(c1_id)s)', [1])

        self.assertEqual(query, q3.compile())


