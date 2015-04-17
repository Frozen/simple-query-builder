# coding=utf-8
import unittest
from simple_query_builder.mysql import Query, LeftJoin, RightJoin, InnerJoin, CrossJoin, Subquery, Where, WhereOr, WhereIn, \
    Join


class TestMysqlQuery(unittest.TestCase):

    class LocalQuery(Query):
        select = None

    def test_select(self):

        q = Query()
        q.select = 1
        self.assertEqual("SELECT 1", q.compile())
        q.select = 0
        self.assertEqual("SELECT 0", q.compile())
        q.select = "bla"
        self.assertEqual("SELECT bla", q.compile())
        self.assertEqual(u"SELECT bla", q.compile())
        q.select = u"я"
        self.assertEqual(u"SELECT я", q.compile())
        q.select = [1, Query()]
        self.assertEqual(u"SELECT 1, (SELECT *)", q.compile())

    def test_from(self):
        q = Query()

        q.from_ = "table"
        self.assertEqual("SELECT * FROM table", q.compile())
        q.from_ = ["table1", "table2"]
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
        q2.select = 1
        q.join = LeftJoin(Subquery(q2, "foo"), "foo.id=posts.user_id")
        self.assertEqual("LEFT JOIN (SELECT 1) as foo ON foo.id=posts.user_id", q.compile())

    def test_where(self):

        q = self.LocalQuery()
        q.where = "user.id > 1"
        self.assertEqual("WHERE (user.id > 1)", q.compile())

        q = self.LocalQuery()
        q.where = Where("user.id > %s", 1)
        self.assertEqual("WHERE (user.id > %s)", q.compile())
        self.assertEqual([1], q.bind)

        q = self.LocalQuery()
        q2 = self.LocalQuery()
        q2.select = "id"
        q2.from_ = "users"
        q2.where = "user.id > 1"
        q.where = WhereIn("user.id", [1, 3, 5])
        self.assertEqual("WHERE (user.id IN (%s))", q.compile())
        q.where = WhereIn("user.id", q2)
        self.assertEqual("WHERE (user.id IN (SELECT id FROM users WHERE (user.id > 1)))", q.compile())

        q = self.LocalQuery()
        q.where = [Where("user.id = %s", 1), WhereOr(Where("post.id = 1"), Where("post.id = %s", 2), "post.id = 3", WhereIn("post.id", [3, 5, 7]))]
        self.assertEqual("WHERE (user.id = %s) AND ((post.id = 1) OR (post.id = %s) OR (post.id = 3) OR (post.id IN (%s)))", q.compile())
        self.assertEqual([1, 2, u'3, 5, 7'], q.bind)

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
        q.limit = [10, 20]
        self.assertEqual("LIMIT 10, 20", q.compile())

    # def test_

class TestMysqlHard(unittest.TestCase):

    def test_1(self):

        query =u"SELECT id, (SELECT name FROM clip WHERE (id = c2.id)) " \
                "FROM clip c1 " \
                "JOIN (SELECT id FROM clip WHERE (id < 30)) AS c2 ON c1.id = c2.id " \
                "WHERE (c1.id IN (%s))" \

        q1 = Query()
        q2 = Query()
        q3 = Query()

        q1.select = ['name']
        q1.where = Where("id = c2.id")
        q1.from_ = "clip"

        q2.select = 'id'
        q2.from_ = 'clip'
        q2.where = Where('id < 30')

        q3.select = ['id', q1]
        q3.from_ = 'clip c1'
        q3.join = Join(Subquery(q2, 'c2'), 'c1.id = c2.id')
        q3.where = WhereIn('c1.id', [1, 2, 3])

        self.assertEqual(query, q3.compile())


