from IPython.display import HTML, display
import pandas
import re
import os
from grizly.io.sqlbuilder import get_sql, to_sql, build_column_strings
from grizly.io.excel import read_excel
import sqlparse
from grizly.io.etl import *


def prepend_table(data, expression):
    field_regex = r"\w+[a-z]"
    escapes_regex = r"""[^"]+"|'[^']+'|and\s|or\s"""
    column_names = re.findall(field_regex, expression)
    columns_to_escape = " ".join(re.findall(escapes_regex, expression))
    for column_name in column_names:
        if column_name in columns_to_escape:
            pass
        else:
            _column_name = data["table"] + "." + column_name
            expression = expression.replace(column_name, _column_name)
            columns_to_escape += " {}".format(column_name)
    return expression


class QFrame:
    """
    Parameters
    ----------
    data: dictionary structure holding fields, schema, table, sql
          information

    field : Each field is a dictionary with these keys. For instance, a 
            query field inside fields definition could look like 
            
            data = {"Country":{"as":"country_name", "group_by":"group"
            , "table":"countries", "schema":"salestables"}}

            In the above case "Country" is a field inside the data 
            dictionary

            Field attributes (keys):
            * as: the 'as' of the database column
            * group_by: If this column is grouped. If it's a dim it's always a 
            group. If it's a num it can be any group agg (sum, count, max, min, etc.)
            * expression: if this is a calculated field, this is an sql expression like
            column_name * 2 or 'string_value' etc.
    other data attributes:
            * schema: the schema name
            * table: the table name, this will be the table alias if the field is from
            a subquery
    """

    def __init__(self, data={}, sql="", getfields=[]):
        self.data = data
        self.sql = sql
        self.getfields = getfields  # remove this and put in data
        self.fieldattrs = ["type","as","group_by","expression","select","custom_type"]
        self.fieldtypes = ["dim","num"]
        self.metaattrs = ["limit", "where"]

    def validate_data(self, data):
        # validating fields, need to validate other stuff too

        for field_key in data['fields']:
            for key_attr in data['fields'][field_key]:
                if key_attr not in set(self.fieldattrs):
                    raise AttributeError("Your columns have invalid attributes.")

        for field_key in data["fields"]:
            if "type" in data["fields"][field_key]:
               if data["fields"][field_key]["type"] not in self.fieldtypes:
                    raise ValueError("Your columns have invalid types.")
            else:
                raise KeyError("Some of your columns don't have types.")

    def from_dict(self, data):
        self.validate_data(data)
        self.data = data
        return self

    def read_excel(self, excel_path, sheet_name="", query=""):
        schema, table, columns_qf = read_excel(excel_path, sheet_name, query)
        data = {}
        data["fields"] = columns_qf
        data["schema"] = schema
        data["table"] = table
        return QFrame(data=data)

    def create_sql_blocks(self):
          return build_column_strings(self)

    def query(self, query):
        """
        Query
        -----
        Creates a "where" attribute inside the data dictionary.
        Prepends the table name to each column field. So
        Country = 'Italy' becomes Orders.Country = 'Italy'

        >>> orders = dict with order table fields
        >>> q = QFrame().from_dict(orders)
        >>> expr = q.query(
                            "country!='Italy' 
                                and (Customer='Enel' or Customer='Agip')
                                or Value>1000
                            ")
        >>> assert expr.data["where"] == "
                                    Orders.country!='Italy' 
                                    and (Orders.Customer='Enel' or Orders.Customer='Agip')
                                    or Orders.Value>1000
                                    "

        """
        self.data["where"] = query
        return self

    def assign(self, notable=False, type="num", group_by="", **kwargs):
        """
        Assign expressions.

        Parameters:
        ----------
        notable : Boolean, default False
            If False adds table name to columns names (eg. before: column1, after: table_name.column1). 
            Note: For now it's not working with CASE statements, you should set True value then.
        group_by : string, default ""
            Note: For now not working.

        Examples:
        --------
            >>> value_x_two = "Value * 2"
            >>> q.assign(value_x_two=value_x_two)
            >>> assert q.data["fields"]["value_x_two"]["expression"] == 
                    "Value * 2"

        """
        if kwargs is not None:
            for key in kwargs:
                if not notable:
                    expression = prepend_table(self.data,kwargs[key])
                else:
                    expression = kwargs[key]
                self.data["fields"][key] = {"type": type, "as": key, "group_by":group_by, "expression": expression}
        return self

    def groupby(self, fields):
        for field in fields:
            self.data["fields"][field]["group_by"] = "group"
        return self

    def agg(self, aggtype):
        if isinstance(self.getfields, str):
            self.getfields = [self.getfields]
        if aggtype in ["sum", "count"]:
            for field in self.getfields:
                self.data["fields"][field]["group_by"] = aggtype
                self.data["fields"][field]["as"] = "sum_{}".format(field)
            return self
        else:
            return print("Aggregation type must be sum or count")

    def limit(self, limit):
        self.data["limit"] = str(limit)
        return self

    def select(self):
        """
        Creates a subquery that looks like select col1, col2 from (some sql)
        """
        # sql = get_sql()

    def rename(self, fields):
        for field in fields:
            self.data["fields"][field]["as"] = fields[field]
        self

    def to_html(self):
        from IPython.display import HTML, display

        html_table = "<table>"
        header = "\n".join(["<th>{}</th>".format(th) for th in self.fieldattrs])
        html_table += "<tr><th>{}</th></tr>".format(header)
        for field in self.fields:
            html_table += """<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>
                """.format(
                field,
                self.fields[field]["type"],
                self.fields[field]["group_by"],
                self.fields[field]["where"],
            )
        html_table += "</table>"
        display(HTML(html_table))

    def to_sql(self, engine_string=""):  # put engine_string in fields as meta
        sql = self.sql
        if engine_string != "":
            df = to_sql(sql, engine_string)
        else:
            df = to_sql(sql, self.data["engine_string"])
        return df

    def get_sql(self, subquery=False):
        """
        Overwrites the sql statement inside the class. Returns a class. To get sql use your_class_name.sql

                >>> q = QFrame().read_excel(excel_path, sheet_name, query)
                >>> q.get_sql()
                >>> sql = q.sql
                >>> print(sql)
        """
        self.sql = get_sql(self).sql
        return self


    def create_table(self, table, schema=''):
        """
        Creates a new table in database if the table doesn't exist.

        Parameters:
        ----------
        table : string
            Name of SQL table.
        schema : string, optional
            Specify the schema.
        """
        create_table(self, table, schema)
        return self


    def to_csv(self, csv_path, db='Denodo'):
        """
        Writes table to csv file.

        Parameters:
        ----------
        csv_path : string
            Path to csv file.
        db : {'Denodo', 'Redshift'}, default 'Denodo'
            Name of database.
        """
        if self.sql == '':
            self.create_sql_blocks()
            self.get_sql()

        to_csv(self,csv_path,self.sql,db)
        return self


    def csv_to_s3(self, csv_path, s3_name):
        """
        Writes csv file to s3 in 'teis-data' bucket.

        Parameters:
        ----------
        csv_path : string
            Path to csv file.
        s3_name : string
            Name of s3. 
        """
        csv_to_s3(csv_path,s3_name)
        return self


    def s3_to_rds(self, table, s3_name, schema='', if_exists='fail', sep='\t'):
        """
        Writes s3 to Redshift database.

        Parameters:
        -----------
        table : string
            Name of SQL table.
        s3_name : string
            Name of s3. 
        schema : string, optional
            Specify the schema.
        if_exists : {'fail', 'replace', 'append'}, default 'fail'
            How to behave if the table already exists.
            * fail: Raise a ValueError.
            * replace: Clean table before inserting new values.
            * append: Insert new values to the existing table.
        sep : string, default '\t'
            Separator/delimiter in csv file.
        """
        s3_to_rds(self, table, s3_name, schema=schema , if_exists=if_exists, sep=sep)
        return self

        
    def to_rds(self, table, csv_path, s3_name, schema='', if_exists='fail', sep='\t'):
        """
        Writes table to Redshift database.

        Parameters:
        ----------
        table : string
            Name of SQL table.
        schema : string, optional
            Specify the schema.
        csv_path : string
            Path to csv file.
        s3_name : string
            Name of s3.
        if_exists : {'fail', 'replace', 'append'}, default 'fail'
                How to behave if the table already exists.
                * fail: Raise a ValueError.
                * replace: Clean table before inserting new values.
                * append: Insert new values to the existing table.
        sep : string, default '\t'
            Separator/delimiter in csv file.
        """
        if self.sql == '':
            self.create_sql_blocks()
            self.get_sql()
            
        to_csv(self,csv_path, self.sql)
        csv_to_s3(csv_path, s3_name)
        s3_to_rds(self, table, s3_name, schema=schema, if_exists=if_exists, sep='\t')

        return self


    def delete_where(self, table, schema='', **kwargs):
        """
        Removes records from Redshift table which satisfy **kwargs.

        Parameters:
        ----------
        table : string
            Name of SQL table.
        schema : string, optional
            Specify the schema.

        Examples:
        --------
            >>> q.delete_where('test_table', schema='testing', fiscal_year=2019)

            Will generate and execute query:
            "DELETE FROM testing.test WHERE fiscal_year = '2019'"


            >>> q.delete_where('test_table', schema='testing', fiscal_year=2018, customer='Enel')

            Will generate and execute two queries:
            "DELETE FROM testing.test WHERE fiscal_year = '2018'"
            "DELETE FROM testing.test WHERE customer = 'Enel'"

        """
        delete_where(table, schema, **kwargs)
        return self


    def __getitem__(self, getfields):
        self.getfields = []
        self.getfields.append(getfields)
        return self



def join(l_q, r_q, on, l_table="l_table", r_table="r_table"):
    onstring = ""
    count = 0
    for tup in on:
        count += 1
        if count < len(on):
            onstring += l_table + "." + tup[0] + "=" + r_table + "." + tup[1] + " and "
        else:
            onstring += l_table + "." + tup[0] + "=" + r_table + "." + tup[1]
    l_q_sql = "({}) as {}".format(l_q.sql, l_table)
    r_q_sql = "({}) as {}".format(r_q.sql, r_table)
    sql = "({} JOIN {} ON {})".format(l_q_sql, r_q_sql, onstring)
    sql = sqlparse.format(sql, reindent=True, keyword_case="upper")
    attrs = {"sql": sql}
    d = {}
    for l_field in l_q.fields:
        field_new = l_table + "." + l_field
        d[field_new] = l_q.fields[l_field]
        # d[field_new]["as"] = field_new
    for r_field in r_q.fields:
        if r_field not in l_q.fields:
            field_new = r_table + "." + r_field
            d[field_new] = r_q.fields[r_field]
    return QFrame(fields=d, attrs=attrs)


def union(*args, alias="union"):
    """
    union -> q1, q2, q3
    fields["union"]
    keys: [field_name, type, group]
    """
    sql = ""
    counter = 0
    d = {}
    for arg in args:
        d = {**d, **arg.fields}
        counter += 1
        sql += "({})".format(arg.get_sql().sql)
        if counter < len(args):
            sql += " UNION "
        else:
            sql = "({}) AS {}".format(sql, alias)
    sql = sqlparse.format(sql, reindent=True, keyword_case="upper")
    attrs = {"sql": sql}
    q = QFrame(table=alias, fields=d, attrs=attrs)
    return q





    