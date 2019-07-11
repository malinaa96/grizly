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
    expressions = {}
    for field in qf.data["fields"]:
        try:
            expressions[field] = qf.data["fields"][field]["expression"]
        except KeyError:
            fields[field] = qf.data["fields"][field]
    select_names = []
    select_aliases = []
    group_dimensions = []
    group_values = []
    for field_key in fields:
        column_name = qf.data["table"] +"."+field_key
        try:
            if fields[field_key]["group_by"] != "" and fields[field_key]["group_by"] != 'group':
                group_value = "{}({}) as {}_{}".format(fields[field_key]["group_by"], column_name
                                                       , fields[field_key]["group_by"], field_key)
                group_values.append(group_value)
            if fields[field_key]["group_by"] == 'group':
                group_dimension = column_name
                group_dimensions.append(group_dimension)
        except KeyError:
            pass
        try:
            if fields[field_key]["group_by"] == "" or fields[field_key]["group_by"] == "group":
                try:
                    fields[field_key]["select"]
                except:
                    select_name = column_name +" as "+ fields[field_key]["as"]
                    select_aliases.append(fields[field_key]["as"])
                    select_names.append(select_name)
        except KeyError:
            try: 
                select_name = column_name +" as "+ fields[field_key]["as"]
            except KeyError: 
                select_name = column_name  
            select_aliases.append(field_key)
            select_names.append(select_name)
    
    for expr_key in expressions:
        formula = expressions[expr_key]
        select_name = formula +" as "+expr_key
        select_names.append(select_name)
        select_aliases.append(expr_key)
        
    qf.data["sql_blocks"] = {"select_names":select_names, "select_aliases":select_aliases
                                , "group_dimensions":group_dimensions, "group_values":group_values}
    return qf

def get_sql(qf):
    # TODO: In case of joins we should use somewhere select_aliases.
    qf.create_sql_blocks()
    data = qf.data
    selects = ', '.join(data['sql_blocks']['select_names']+data['sql_blocks']['group_values'])
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