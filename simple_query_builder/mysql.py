

class Compilable:
    pass

class Query(Compilable):

    select = '*'
    from_ = None
    where = None
    join = None
    on = []
    bind = None
    group_by = None
    order_by = None
    having = None
    limit = None

    def __init__(self):
        self.bind = []

    def _compile_select(self, select):
        query = None
        if select is not None:
            columns = []

            if self.is_collection(select):
                for column in select:
                    if isinstance(column, Compilable):
                        column = u"({0})".format(column.compile(self))
                    columns.append(unicode(column))
                query = u"SELECT " + ", ".join(columns)
            else:
                query = u"SELECT " + unicode(select)
        return query

    def _compile_from(self, param):
        query = None
        if param is not None:
            columns = []

            if self.is_collection(param):
                for column in param:
                    columns.append(unicode(column))
                query = "FROM " + ", ".join(columns)
            else:
                query = "FROM " + unicode(param)
        return query

    def _compile_join(self, param):
        columns = []
        if param is not None:
            columns = []

            if self.is_collection(param):
                for column in param:
                    if isinstance(column, Compilable):
                        column = column.compile()
                        columns.append(column)
                    else:
                        columns.append(u"JOIN " + column)
            else:
                if isinstance(param, Compilable):
                    columns.append(param.compile())
                else:
                    columns.append(u"JOIN " + param)

        if columns:
            return u', '.join(columns)

    def _compile_where(self, param):
        columns = []

        if param is None:
            return None

        if not self.is_collection(param):
            param = [param]

        for column in param:
            if not isinstance(column, Compilable):
                column = Where(column)
            columns.append(u"{0}".format(column.compile(self)))

        if columns:
            return u"WHERE {0}".format(u" AND ".join(columns))

    def _compile_group_by(self, param):
        columns = []

        if param is None:
            return None

        if not self.is_collection(param):
            param = [param]

        for column in param:
            if isinstance(column, Compilable):
                column = column.compile()
            columns.append(u"{0}".format(column))

        if columns:
            return u"GROUP BY {0}".format(u", ".join(columns))

    def _compile_having(self, param):
        columns = []

        if param is None:
            return None

        if not self.is_collection(param):
            param = [param]

        for column in param:
            if isinstance(column, Compilable):
                column = column.compile(self)
            columns.append(u"{0}".format(column))

        if columns:
            return u"HAVING {0}".format(u" AND ".join(columns))

    def _compile_order_by(self, param):
        columns = []

        if param is None:
            return None

        if not self.is_collection(param):
            param = [param]

        for column in param:
            if isinstance(column, Compilable):
                column = column.compile(self)
            columns.append(u"{0}".format(column))

        if columns:
            return u"ORDER BY {0}".format(u", ".join(columns))

    def _compile_limit(self, param):
        columns = []

        if param is None:
            return None

        if not self.is_collection(param):
            param = [param]

        for column in param:
            if isinstance(column, Compilable):
                column = column.compile(self)
            columns.append(u"{0}".format(column))

        if columns:
            return u"LIMIT {0}".format(u", ".join(columns))

    def compile(self, parent=None):
        query = [
            self._compile_select(self.select),
            self._compile_from(self.from_),
            self._compile_join(self.join),
            self._compile_where(self.where),
            self._compile_group_by(self.group_by),
            self._compile_having(self.having),
            self._compile_order_by(self.order_by),
            self._compile_limit(self.limit)

        ]

        return ' '.join(filter(lambda x: x, query))

    def is_collection(self, v):
        if isinstance(v, basestring):
            return False
        try:
            iter(v)
            return True
        except TypeError:
            return False


class Where(Compilable):

    def __init__(self, *params):
        self.params = params
        if len(params) == 0:
            raise ValueError("Empty Where params")
        if len(params) == 2:
            self.statement = params[0]
            self.param = params[1]
        if len(params) == 1:
            self.statement = params[0]

    def compile(self, parent):

        statement = self.statement
        if isinstance(statement, Compilable):
            statement = statement.compilable()

        if len(self.params) == 2:
            print("bind", statement)
            parent.bind.append(self.param)
        return u"({0})".format(statement)


class WhereOr(Compilable):

    def __init__(self, *statements):
        self.statements = statements

    def compile(self, parent):

        ors = []

        for statement in self.statements:
            if not isinstance(statement, Compilable):
                statement = Where(statement)
            ors.append(statement.compile(parent))

        if ors:
            return u"({0})".format(u' OR '.join(ors))


class WhereIn(Compilable):

    def __init__(self, statement, values):
        self.statement = statement
        self.values = values

    def compile(self, parent):
        statement = self.statement
        if isinstance(statement, Compilable):
            statement = statement.compile(parent)
        values = self.values
        if isinstance(self.values, Compilable):
            values = values.compile(parent)

        if self.values:
            if is_collection(self.values):
                ret = Where(u"{0} IN (%s)".format(self.statement), u', '.join(map(unicode, self.values))).compile(parent)
                return ret
            # print(statement, values)
            return Where(u"{0} IN ({1})".format(statement, values)).compile(parent)
        else:
            return None


class Join(Compilable):
    word = u""

    def __init__(self, join, on):
        self.join = join
        self.on = on

    def compile(self):
        join = self.join
        if isinstance(self.join, Compilable):
            join = self.join.compile()
        return u"{0}JOIN {1} ON {2}".format(self.word, join, self.on)


class LeftJoin(Join):

    word = u"LEFT "


class RightJoin(Join):
    word = u"RIGHT "


class InnerJoin(Join):
    word = u"INNER "


class CrossJoin(Join):
    word = u'CROSS '


class Subquery(Compilable):

    def __init__(self, query, as_name):
        self.query = query
        self.name = as_name

    def compile(self):
        query = self.query
        if isinstance(query, Compilable):
            query = query.compile()
        return u"({0}) AS {1}".format(query, self.name)


def is_collection(v):
    if isinstance(v, basestring):
        return False
    try:
        iter(v)
        return True
    except TypeError:
        return False


