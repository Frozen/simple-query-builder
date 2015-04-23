# coding=utf-8
import unittest
import datetime
from simple_query_builder.mysql import Query, LeftJoin, RightJoin, InnerJoin, CrossJoin, Subquery, Where, WhereOr, WhereIn, \
    Join


class TestMysqlQuery(unittest.TestCase):

    class LocalQuery(Query):
        _select = None



    def test_select(self):

        q = Query()
        q._select = 1
        self.assertEqual("SELECT 1", q.compile())
        q._select = 0
        self.assertEqual("SELECT 0", q.compile())
        q._select = "bla"
        self.assertEqual("SELECT bla", q.compile())
        self.assertEqual(u"SELECT bla", q.compile())
        q._select = u"я"
        self.assertEqual(u"SELECT я", q.compile())
        q._select = [1, Query()]
        self.assertEqual(u"SELECT 1, (SELECT *)", q.compile())

    def test_from(self):
        q = Query()

        q._from = "table"
        self.assertEqual("SELECT * FROM table", q.compile())
        q._from = ["table1", "table2"]
        self.assertEqual("SELECT * FROM table1, table2", q.compile())

    def test_join(self):

        q = self.LocalQuery()
        s = "table ON user.id=posts.user_id"
        q.join = "table ON user.id=posts.user_id"
        self.assertEqual("JOIN " + s, q.compile())
        q.join = LeftJoin("table", "user.id=posts.user_id")
        self.assertEqual("LEFT JOIN " + s, q.compile())
        q.join = RightJoin("table", "user.id=posts.user_id")
        self.assertEqual("RIGHT JOIN " + s, q.compile())
        q.join = InnerJoin("table", "user.id=posts.user_id")
        self.assertEqual("INNER JOIN " + s, q.compile())
        q.join = CrossJoin("table", "user.id=posts.user_id")
        self.assertEqual("CROSS JOIN " + s, q.compile())

        q = self.LocalQuery()
        q.join = [LeftJoin("user", "user.id=posts.user_id"), RightJoin("user", "user.id=posts.user_id")]
        self.assertEqual("LEFT JOIN user ON user.id=posts.user_id, RIGHT JOIN user ON user.id=posts.user_id", q.compile())

        q = self.LocalQuery()
        q2 = self.LocalQuery()
        q2._select = 1
        q.join = LeftJoin(Subquery(q2, "foo"), "foo.id=posts.user_id")
        self.assertEqual("LEFT JOIN (SELECT 1) AS foo ON foo.id=posts.user_id", q.compile())

    def test_where(self):

        q = self.LocalQuery()
        q._where = "user.id > 1"
        self.assertEqual("WHERE (user.id > 1)", q.compile())

        q = self.LocalQuery()
        q._where = Where("user.id > %(id)s", 1)
        self.assertEqual("WHERE (user.id > %(id)s)", q.compile())
        self.assertEqual({"id": 1}, q.bind())

        q = self.LocalQuery()
        q._where = [Where("user.id = %(id1)s", 1), WhereOr(Where("post.id = 1"), Where("post.id = %(id2)s", 2), "post.id = 3", WhereIn("post.id IN (%(values)s)", [3, 5, 7]))]
        self.assertEqual({
            'id1': 1,
            'id2': 2,
            'values_0': 3,
            'values_1': 5,
            'values_2': 7
        }, q.bind())
        self.assertEqual("WHERE (user.id = %(id1)s) AND ((post.id = 1) OR (post.id = %(id2)s) OR (post.id = 3) OR "
                         "(post.id IN (%(values_0)s, %(values_1)s, %(values_2)s)))", q.compile())

        # test where with date and datetime
        from datetime import datetime, date

        q = self.LocalQuery()
        q._where = Where("dt > %(dt)s", date(2015, 5, 7))
        self.assertEqual({"dt": "2015-05-07"}, q.bind())

        q = self.LocalQuery()
        q._where = Where("dt > %(dt)s", datetime(2014, 10, 01, 23, 11, 1))
        self.assertEqual({"dt": "2014-10-01 23:11:01"}, q.bind())

    def test_where_in(self):

        q = self.LocalQuery()
        q._where = WhereIn("user.id IN (%(ids)s)", [1, 3])
        self.assertEqual("WHERE (user.id IN (%(ids_0)s, %(ids_1)s))", q.compile())

        # test empty
        q = self.LocalQuery()
        q._where = WhereIn("user.id", [])
        self.assertEqual("", q.compile())

        q = self.LocalQuery()
        q._where = WhereIn("user.id IN (%(in)s)", Query())
        self.assertEqual("WHERE (user.id IN (SELECT *))", q.compile())

    def test_group_by(self):

        q = self.LocalQuery()
        q.group_by = "user.id"
        self.assertEqual("GROUP BY user.id", q.compile())

        q = self.LocalQuery()
        q.group_by = ["user.id", "date"]
        self.assertEqual("GROUP BY user.id, date", q.compile())

    def test_having(self):

        q = self.LocalQuery()
        q.having = "count(user.id) > 0"
        self.assertEqual("HAVING count(user.id) > 0", q.compile())

        q = self.LocalQuery()
        q.having = ["count(user.id) > 0", "date != now()"]
        self.assertEqual("HAVING count(user.id) > 0 AND date != now()", q.compile())

    def test_order_by(self):

        q = self.LocalQuery()
        q.order_by = "user.id"
        self.assertEqual("ORDER BY user.id", q.compile())

        q = self.LocalQuery()
        q.order_by = ["user.id", "date desc"]
        self.assertEqual("ORDER BY user.id, date desc", q.compile())

    def test_limit(self):
        q = self.LocalQuery()
        q.limit = "10"
        self.assertEqual("LIMIT 10", q.compile())

        q = self.LocalQuery()
        q.limit = 10
        q.offset = 20
        self.assertEqual("LIMIT 20, 10", q.compile())

    def test_add_where(self):

        q = self.LocalQuery()
        q.add_where(Where("id>1"))
        self.assertEqual("WHERE (id>1)", q.compile())

        q = self.LocalQuery()
        q._where = Where("id > 1")
        q.add_where(Where("id > 2"))
        self.assertEqual("WHERE (id > 1) AND (id > 2)", q.compile())


class TestMysqlHard(unittest.TestCase):

    def test_1(self):

        query =u"SELECT id, (SELECT name FROM clip WHERE (id = c2.id) AND (datein >= %(dt)s)) " \
                "FROM clip c1 " \
                "JOIN (SELECT id FROM clip WHERE (id < 30)) AS c2 ON c1.id = c2.id " \
                "WHERE (c1.id IN (%(c1_id_0)s))" \

        q1 = Query()
        q2 = Query()
        q3 = Query()

        q1._select = ['name']
        q1._where = [Where("id = c2.id"), Where("datein >= %(dt)s", datetime.date(2015, 4, 1))]
        q1._from = "clip"

        q2._select = 'id'
        q2._from = 'clip'
        q2._where = Where('id < 30')

        q3._select = ['id', q1]
        q3._from = 'clip c1'
        q3.join = Join(Subquery(q2, 'c2'), 'c1.id = c2.id')
        q3._where = WhereIn('c1.id IN (%(c1_id)s)', [1])

        self.assertEqual(query, q3.compile())
        self.assertEqual({'dt': '2015-04-01', 'c1_id_0': 1}, q3.bind())


