import dataclasses
import os
from collections import OrderedDict
from dataclasses import dataclass
from functools import cached_property

import psycopg2

# https://www.postgresql.org/docs/current/datatype.html
# https://www.digitalocean.com/community/tutorials/how-to-use-a-postgresql-database-in-a-flask-application


@dataclass
class Field:
    name: str
    max_length: int = 100
    var_char: bool = False
    not_null: bool = False
    integer: bool = False
    decimal: bool = False
    boolean: bool = False
    default: str | bool | list | int = None
    primary_key: bool = False

    def __str__(self):
        return self.name

    def __hash__(self):
        return hash((self.name, self.primary_key))

    def __eq__(self, value):
        return self.name == value

    def as_sql(self):
        return [self.name]


@dataclass
class CharField(Field):
    var_char: bool = True


@dataclass
class IntegerField(Field):
    integer: int = True


@dataclass
class BooleanField(Field):
    boolean: bool = True


class BaseRelationship:
    is_relationship = True


@dataclass
class ForeignKey(BaseRelationship):
    model: type


@dataclass
class ManyToMany(BaseRelationship):
    model: type


class SQL:
    select = 'SELECT {fields} FROM {name}'
    drop_table = 'DROP TABLE IF EXISTS {name}'
    create_table = 'CREATE TABLE IF NOT EXISTS {name} ({fields})'
    insert_into_table = 'INSERT INTO {name} ({fields}) VALUES ({values})'

    def __init__(self):
        self._cached_sql = None

    @staticmethod
    def finalize_sql(sql):
        if ';' in sql:
            return sql
        return f'{sql};'

    def quote(self, value):
        return f"'{value}'"

    def check_fields(self, fields):
        pass

    def get_sql_maps(self, fields):
        pass

    def join_partials(self, sql_maps, is_create=False):
        partials = []
        formatted_sql_maps = []

        for item in sql_maps:
            if isinstance(item, str):
                formatted_sql_maps.append([item])
            else:
                formatted_sql_maps.append(item)

        for sql_map in formatted_sql_maps:
            if is_create:
                if 'id' in sql_map:
                    continue

            partial = ' '.join(sql_map)
            partials.append(partial)

        return ', '.join(partials)


class Table(SQL):
    alter_table = "ALTER TABLE {name}"
    table_exists = "SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname='public' AND tablename={name})"

    def __init__(self, name):
        super().__init__()
        self.table_name = name
        self._cached_fields = []
        self.field_map = OrderedDict()
        self.relationship_field_map = OrderedDict()

    def __repr__(self):
        return f'<Table [{self.table_name}]>'

    @property
    def fields_count(self):
        # Do not include the ID field in the
        # overall field count
        return len(self._cached_fields) - 1

    @cached_property
    def field_names(self):
        return list(map(lambda x: x.name, self._cached_fields))

    @property
    def has_relationships(self):
        return len(self.relationship_field_map.keys()) > 0

    def check_fields(self, fields):
        errors = []
        for field in fields:
            if not dataclasses.is_dataclass(field):
                errors.append(field)

        if errors:
            raise ValueError('Field should be a dataclass')
        self._cached_fields = fields

    def get_sql_maps(self, fields):
        sql_maps = []
        for field in fields:
            is_relationship_field = getattr(field, 'is_relationship', False)
            if is_relationship_field:
                name = f'{self.table_name}_{field.model.model_name}'
                self.relationship_field_map[name] = field
            else:
                self.field_map[field.name] = field
                sql_maps.append(self.prepare_field(field))
        return sql_maps

    def prepare_field(self, field):
        sql_map = [field.name]
        return self.add_arguments(field, sql_map)

    def add_constraints(self, sql_map, foreign_key=True):
        constraint_map = []
        if foreign_key:
            foreign_key_sql = "{field_name}_id INTEGER REFERENCES {reference_table}({field_name}_id)"
            constraint_map.append(foreign_key_sql)
        # if foreign_key:
        #     constraint_map.append('ADD CONSTRAINT {constraint_name}')
        #     foreign_key_sql = "FOREIGN KEY ({field_name}_id) REFERENCES {reference_table} ({field_name}_id)"
        #     constraint_map.append(foreign_key_sql)
        sql_map.append(constraint_map)

    def add_arguments(self, field, sql_map):
        if field.var_char:
            max_length_sql = f'varchar({field.max_length})'
            sql_map.append(max_length_sql)
        elif field.integer:
            sql_map.append('integer')
        elif field.decimal:
            sql_map.append('decimal')
        elif field.boolean:
            sql_map.append('boolean')

        if field.not_null:
            sql_map.append('NOT NULL')

        if field.primary_key:
            sql_map.append('serial')
            sql_map.append('PRIMARY KEY')

        if field.default is not None:
            default_sql = f'DEFAULT {self.quote(field.default)}'
            sql_map.append(default_sql)

        return sql_map

    def new_table_sql(self, fields=[]):
        self.check_fields(fields)
        sql_maps = self.get_sql_maps(fields)

        sql_arguments = {'name': self.table_name}
        if self.has_relationships:
            self.add_constraints(sql_maps)
            arguments = self.join_partials(sql_maps)
            # TODO: Create the field_id reference in the other table
            # and get the reference table
            arguments = arguments.format(field_name='b', reference_table='a')
        else:
            arguments = self.join_partials(sql_maps)

        sql_arguments.update({'fields': arguments})
        partial_sql = self.create_table.format(**sql_arguments)
        return self.finalize_sql(partial_sql)

    def insert_in_table_sql(self, values):
        if len(values) != self.fields_count:
            raise ValueError('There are more values than fields')

        fields = self.join_partials(self.field_names, is_create=True)

        values = list(map(lambda x: self.quote(x), values))
        values = self.join_partials(values)
        partial_sql = self.insert_into_table.format(
            name=self.table_name,
            fields=fields,
            values=values
        )
        return self.finalize_sql(partial_sql)

    def table_exists_sql(self):
        sql = self.table_exists.format(name=self.quote(self.table_name))
        return self.finalize_sql(sql)

    def select_from_table(self, fields=None):
        if fields is None:
            fields = '*'
        sql = self.select.format(fields=fields, name=self.table_name)
        return self.finalize_sql(sql)


class Database:
    def __init__(self):
        self.connection = None
        # self.base_tables = ['campaigns', 'emails']
        self.tables = OrderedDict()

    @cached_property
    def get_connection(self):
        return psycopg2.connect(
            host='localhost',
            database='emailing_server',
            user=os.getenv('DB_USERNAME', 'emailing_agent'),
            password=os.getenv('DB_PASSWORD', 'touparet')
        )

    @cached_property
    def cursor(self):
        connection = self.get_connection
        return connection, connection.cursor()

    def _execute_cursor(self, sql):
        connection, cursor = self.cursor
        try:
            cursor.execute(sql)
            connection.commit()
        except Exception as e:
            connection.rollback()
        finally:
            return cursor
        # else:
        #     connection.close()

    def _table_exists(self, name):
        table = Table(name)
        return self._execute_cursor(table.table_exists_sql())

    def _create_table(self, name, fields):
        instance = Table(name)
        try:
            sql = instance.new_table_sql(fields)
            # self._execute_cursor(sql)
        except psycopg2.errors.DuplicateTable as e:
            # If the table exists already,
            # just silently fail the creation
            # process
            pass
        else:
            self.tables[instance.table_name] = instance

    def insert_into_table(self, name, values):
        try:
            table = self.tables[name]
        except:
            raise ValueError('Table does not exist')

        sql = table.insert_in_table_sql(values)
        self._execute_cursor(sql)


database = Database()


class Query:
    def __init__(self, queryset):
        self.queryset = queryset


class ModelIterable:
    def __init__(self, queryset, chunks=100):
        self.queryset = queryset
        self.chunks = chunks

    def __iter__(self):
        pass


class QuerySet:
    iterable_class = ModelIterable

    def __init__(self, model, sql=None, query=None):
        self._cache = []
        self._model = model
        # self._sql = sql
        self._query = query or Query(self)

    def __str__(self):
        self.populate_cache()
        return f'<{self.__class__.__name__} {self._cache}>'

    def populate_cache(self):
        if not self._cache:
            self._cache = list(self._model._cursor)

    def count(self):
        self.populate_cache()
        return len(self._cache)


class BaseModel:
    queryset_class = QuerySet

    def __init__(self, name, fields):
        self.fields = fields
        self.model_name = str(name).lower()
        self.verbose_model_name = self.model_name.title()
        self._default_manager = None
        self._connection = database
        self._cursor = None

        if 'id' not in fields:
            fields.insert(0, Field('id', primary_key=True))

        self._connection._create_table(name, fields)

    def __repr__(self):
        return f'<{self.verbose_model_name}>'

    @property
    def get_table(self):
        return Table(self.model_name)

    async def acreate(self, **kwargs):
        pass

    async def aget(self, *args, **kwargs):
        pass

    async def afilter(self, *args, **kwargs):
        pass

    def create(self, **kwargs):
        self._connection.insert_into_table(
            self.model_name,
            list(kwargs.values())
        )

    def all(self):
        sql = self.get_table.select_from_table()
        self._cursor = self._connection._execute_cursor(sql)
        return self.queryset_class(self)

    def get(self, *args, **kwargs):
        pass

    def filter(self, *args, **kwargs):
        return self.queryset_class(self)


country = BaseModel('country', [
    CharField('name')
])


cars = BaseModel('cars', [
    ForeignKey(country)
])

# campaigns = BaseModel('campaigns', [
#     Field('name', var_char=True, not_null=True),
#     CharField('reference'),
#     IntegerField('number_of_steps'),
#     IntegerField('minutes'),
#     BooleanField('active')
# ])

# emails = BaseModel('emails', [
#     # IntegerField('campaign'),
#     ForeignKey(campaigns),
#     IntegerField('email'),
#     IntegerField('current_step')
# ])

# steps = BaseModel('steps', [
#     # IntegerField('emails'),
#     ForeignKey(emails),
#     IntegerField('value'),
#     IntegerField('days')
# ])

# campaigns.create(name='Kendall Jenner')
# print(campaigns.all())
# print(os.getenv('DB_PASSWORD'))

# d.create_table(
#     'campaigns',
#     [Field('id', primary_key=True), CharField('name', not_null=True)]
# )
# d.insert_in_table('campaigns', ['Some campaign'])
# print(d.table_exists('campaigns'))
