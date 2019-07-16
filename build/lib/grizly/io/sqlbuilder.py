import sqlparse
import pandas
from sqlalchemy import create_engine

def to_col_name(data, field, agg="", noas=False):
    col_name = data["table"] + "." + field
    if agg != "":
        col_name = "{}({}) as {}_{}".format(agg, col_name, agg, field)
    else:
        if "as" in data["fields"][field] and noas is False:
            col_name += " as {}".format(data["fields"][field]["as"])
    return col_name


def write(qf, table, drop=False):
    """
    qf: q frame
    table: database table to save into

    drop: if drop is True then the table data will be deleted and
        new data inserted. The SQL looks like so:
        DELETE FROM TABLE
        insert into prices (group, id, price)
            select 
            7, articleId, 1.50
            from article where name like 'ABC%';
    """
    fields = qf.data["fields"]
    columns = []
    if drop == False:
        for field in fields:
            col_name = field
            if fields[field]["type"] == "num":
                col_name += " double"
            if fields[field]["type"] == "dim":
                col_name += " varchar(10000)"
            columns.append(col_name)
        columns_str = ", ".join(columns)
        sql = "CREATE TABLE {} ({})".format(table, columns_str)
    else:
        sql = "DELETE FROM {}".format(table)
        pass  # do insert into instead with append
    sql = sqlparse.format(sql, reindent=True, keyword_case="upper")
    return sql


def to_sql(sql, engine_string):
    engine = create_engine(engine_string)
    df = pandas.read_sql(sql=sql, con=engine)
    for col in df:
        coltype = df[col].dtype
        if coltype in ["float64"]:
            df[col] = df[col].map("{:,.0f}".format)
    return df


def build_column_strings(qf):
    fields = {}
    fields_with_expr = {}
    for field in qf.data["fields"]:
        try:
            qf.data["fields"][field]["expression"]
            fields_with_expr[field] = qf.data["fields"][field]
        except KeyError:
            fields[field] = qf.data["fields"][field]
    select_names = []
    select_aliases = []
    group_dimensions = []
    group_values = []
    types = []
    for field_key in fields:

        column_name = qf.data["table"] +"."+field_key
        if 'group_by' in fields[field_key] and fields[field_key]["group_by"] != "":
            if fields[field_key]["group_by"] != 'group':
                try:
                    alias = fields[field_key]["as"]
                except KeyError:
                    alias = "{}_{}".format(fields[field_key]["group_by"], field_key)
                group_value = "{}({}) as {}".format(fields[field_key]["group_by"], column_name, alias)
                group_values.append(group_value)
                select_names.append(group_value)
                select_aliases.append(alias)
                try:
                    types.append(fields[field_key]["custom_type"])
                except KeyError:
                    if fields[field_key]["type"] == "dim":
                        types.append("VARCHAR(500)")
                    if fields[field_key]["type"] == "num":
                        types.append("FLOAT(53)")
            else:
                group_dimension = column_name
                group_dimensions.append(group_dimension)
        if 'group_by' not in fields[field_key] or fields[field_key]["group_by"] == "" or fields[field_key]["group_by"] == 'group':
            try:
                fields[field_key]["select"]
            except:
                try:
                    alias = fields[field_key]["as"]
                    select_name = column_name + " as "+ alias
                except KeyError:
                    alias = field_key
                    select_name = column_name
                try:
                    types.append(fields[field_key]["custom_type"])
                except KeyError:
                    if fields[field_key]["type"] == "dim":
                        types.append("VARCHAR(500)")
                    if fields[field_key]["type"] == "num":
                        types.append("FLOAT(53)")
                select_names.append(select_name)
                select_aliases.append(alias)
    
    for field in fields_with_expr:
        try:
            types.append(fields_with_expr[field]["custom_type"])
        except KeyError:
            if fields_with_expr[field]["type"] == "dim":
                types.append("VARCHAR(500)")
            if fields_with_expr[field]["type"] == "num":
                types.append("FLOAT(53)")
        expr = fields_with_expr[field]["expression"]
        select_name = expr +" as "+field
        select_names.append(select_name)
        select_aliases.append(field)

    qf.data["sql_blocks"] = {"select_names":select_names, "select_aliases":select_aliases
                                , "group_dimensions":group_dimensions, "group_values":group_values, "types": types}
    return qf

def get_sql(qf):
    # TODO: In case of joins we should use somewhere select_aliases.
    qf.create_sql_blocks()
    data = qf.data
    selects = ', '.join(data['sql_blocks']['select_names'])
    sql = "SELECT {}".format(selects)
    if "schema" in data and data["schema"] != "":
        sql += " FROM {}.{}".format(data["schema"],data["table"])
    else: 
        sql += " FROM {}".format(data["table"])
    if "where" in data:
            sql += " WHERE {}".format(data["where"])
    if data['sql_blocks']['group_dimensions'] != []:
        group_names = ', '.join(data['sql_blocks']['group_dimensions'])
        sql += " GROUP BY {}".format(group_names)
    if "limit" in data:
        sql += " LIMIT {}".format(data["limit"])
    sql = sqlparse.format(sql, reindent=True, keyword_case="upper")
    qf.sql = sql
    return qf