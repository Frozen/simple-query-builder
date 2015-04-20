import re
from six import string_types
# RE_BIND_PATTERN = ":([a-zA-Z0-9]+)"
from simple_query_builder import stringify_if_date, BaseQuery, Compilable

RE_BIND_PATTERN = ":([a-zA-Z0-9_]+)"
RE_BIND_PATTERN_COMPILED = re.compile(RE_BIND_PATTERN)


class Query(BaseQuery):

    select = '*'
    from_ = None
    where = None
    join = None
    on = []
    group_by = None
    order_by = None
    having = None
    limit = None
    offset = None

    _bind = None

    def __init__(self):
        self._bind = dict()

    def _compile_where(self, parent, params):
        columns = []

        if params is None:
            return None

        if not self.is_collection(params):
            params = [params]

        for column in params:

            if isinstance(column, Compilable):
                column = column.compile(parent)
            else:
                column = Where(column).compile(parent)
            if column is None:
                continue
            columns.append(u"{0}".format(column))

        if columns:
            return u"WHERE {0}".format(u" AND ".join(columns))

    def _compile_limit(self, limit, offset):
        if limit is None:
            return None

        if offset is not None:
            return u"LIMIT {0} OFFSET {1}".format(limit, offset)

        return u"LIMIT {0}".format(limit)


    def compile(self, parent=None):
        if parent is None:
            parent = self
        query = [
            self._compile_select(parent, self.select),
            self._compile_from(self.from_),
            self._compile_join(parent, self.join),
            self._compile_where(parent, self.where),
            self._compile_group_by(self.group_by),
            self._compile_having(self.having),
            self._compile_order_by(self.order_by),
            self._compile_limit(self.limit, self.offset)
        ]

        return ' '.join(filter(lambda x: x, query))


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
            key = _fetch_key(self.statement)
            parent.add_bind(key, stringify_if_date(self.param))
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

        if isinstance(self.values, Compilable):
            return self._compile_subquery(parent)
        else:
            return self._compile_values(parent)

    def _compile_subquery(self, parent):
        compilable = self.values

        statement = self.statement
        try:
            key = _fetch_key(statement)
            statement = RE_BIND_PATTERN_COMPILED.sub("{0}", statement)
            return "(" + statement.format(compilable.compile(parent)) + ")"
        except ValueError:
            return "(" + statement + " IN (" + compilable.compile(parent) + ")" + ")"

    def _compile_values(self, parent):
        statement = self.statement
        if isinstance(statement, Compilable):
            statement = statement.compile(parent)
        values = self.values

        if not self.values:
            return None

        key = _fetch_key(statement)
        statement = RE_BIND_PATTERN_COMPILED.sub("{0}", statement)
        if is_collection(values):
            statement = statement.format(', '.join(map(lambda x: ":{0}_{1}".format(key, x), range(len(values)))))
            for x in range(len(values)):
                parent.add_bind("{0}_{1}".format(key, x), values[x])
            # for (k, value) in enumerate(self.values, start=1):
            #     ret = Where(self.statement, u', '.join(map(unicode, self.values))).compile(parent)
            return "("+statement+")"
        return Where(u"{0} IN ({1})".format(statement, values)).compile(parent)


class Join(Compilable):
    word = u""

    def __init__(self, join, on):
        self.join = join
        self.on = on

    def compile(self, parent=None):
        join = self.join
        if isinstance(self.join, Compilable):
            join = self.join.compile(parent)
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

    def compile(self, parent=None):
        query = self.query
        if isinstance(query, Compilable):
            query = query.compile(parent)
        return u"({0}) AS {1}".format(query, self.name)


def is_collection(v):
    if isinstance(v, basestring):
        return False
    try:
        iter(v)
        return True
    except TypeError:
        return False


def _fetch_key(statement):
    """

    :rtype : str
    """
    try:
        return RE_BIND_PATTERN_COMPILED.search(statement).group(1)
    except AttributeError as e:
        raise ValueError("statement does not contains bind name")

