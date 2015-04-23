import datetime
from six import string_types

__all__ = []


def stringify_if_date(value):
    if isinstance(value, datetime.datetime):
        return value.strftime('%Y-%m-%d %H:%M:%S')
    if isinstance(value, datetime.date):
        return value.strftime('%Y-%m-%d')
    return value


class Compilable(object):
    pass


class BaseQuery(Compilable):



    _bind = None
    _offset = None
    _limit = None
    _having = None
    _order_by = None
    _group_by = None
    _join = None
    _where = None
    _from = None
    _select = '*'

    def __init__(self):
        self._bind = dict()

    def bind(self):
        self.compile()
        return self._bind

    def add_bind(self, key, value):
        # self._bind = self._bind or {}
        self._bind[key] = value

    def select(self, s):
        new = self.clone()
        new._select = s
        return new

    # def where(self, *params):
    #     new = self.clone()
    #     new.add_where(Where(*params))
    #     return new

    def from_(self, s):
        new = self.clone()
        new._from = s
        return new

    def join(self, s):
        new = self.clone()
        new._join = s
        return new

    def group_by(self, s):
        new = self.clone()
        new._add_group_by(s)
        return new

    def having(self, s):
        new = self.clone()
        new._add_having(s)
        return new

    def _add_group_by(self, s):
        self._add_param(self, '_group_by', s)

    def _add_having(self, s):
        self._add_param(self, '_having', s)

    def order_by(self, s):
        new = self.clone()
        new._add_order_by(s)
        return new

    def _add_order_by(self, s):
        self._add_param(self, '_order_by', s)

    def limit(self, limit):
        new = self.clone()
        new._limit = limit
        return new

    def offset(self, offset):
        new = self.clone()
        new._offset = offset
        return new

    def _add_param(self, object, param, s):
        if getattr(object, param) is None:
            setattr(object, param, s)
        elif self.is_collection(getattr(object, param)):
            setattr(object, param, list(getattr(object, param)) + [s])
        else:
            setattr(object, param, [self._group_by, s])


    def _compile_select(self, parent, select):
        query = None
        if select is not None:
            columns = []

            if self.is_collection(select):
                for column in select:
                    if isinstance(column, Compilable):
                        column = u"({0})".format(column.compile(parent))
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

    def _compile_join(self, parent, param):
        columns = []
        if param is not None:
            columns = []

            if self.is_collection(param):
                for column in param:
                    if isinstance(column, Compilable):
                        column = column.compile(parent)
                        columns.append(column)
                    else:
                        columns.append(u"JOIN " + column)
            else:
                if isinstance(param, Compilable):
                    columns.append(param.compile(parent))
                else:
                    columns.append(u"JOIN " + param)

        if columns:
            return u', '.join(columns)

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

    def is_collection(self, v):
        if isinstance(v, string_types):
            return False
        try:
            iter(v)
            return True
        except TypeError:
            return False

    def add_where(self, val):
        if self._where is None:
            self._where = [val]
        elif self.is_collection(self._where):
            where = list(self._where)
            where.append(val)
            self._where = where
        else:
            self._where = [self._where, val]

    def clone(self):
        import copy
        return copy.deepcopy(self)



