import sqlparse
from copy import deepcopy



def build_column_strings(data):
    select_names = []
    select_aliases = []
    group_dimensions = []
    group_values = []
    order_by = []
    types = []

    fields = data["select"]["fields"]

    for field in fields:
        expr = field if "expression" not in fields[field] else fields[field]["expression"]
        alias = field if "as" not in fields[field] else fields[field]["as"]

        if "group_by" in fields[field]:
            if fields[field]["group_by"] == "group":
                group_dimensions.append(alias)

            elif fields[field]["group_by"] == "":
                pass

            else:
                if fields[field]["group_by"] == "sum":
                    expr = f"sum({expr})" 
                elif fields[field]["group_by"] == "count":
                    expr = f"count({expr})" 
                elif fields[field]["group_by"] == "max":
                    expr = f"max({expr})" 
                elif fields[field]["group_by"] == "min":
                    expr = f"min({expr})" 
                elif fields[field]["group_by"] == "avg":
                    expr = f"avg({expr})" 
                else:
                    raise AttributeError("Invalid aggregation type.")
                
                group_values.append(alias)

        if "select" not in fields[field] or "select" in fields[field] and fields[field]["select"] != 0:
            select_name = field if expr == alias else f"{expr} as {alias}"

            if "custom_type" in fields[field]:
                type = fields[field]["custom_type"]
            elif fields[field]["type"] == "dim":
                type = "VARCHAR(500)"
            elif fields[field]["type"] == "num":
                type = "FLOAT(53)"
            
            if "order_by" in fields[field]:
                order = fields[field]["order_by"]  if fields[field]["order_by"].upper() == 'DESC' else ''
                order_by.append(f"{alias} {order}")

            select_names.append(select_name)
            select_aliases.append(alias)
            types.append(type)

    # validations 
    # TODO: Check if the group by is correct - group by expression or columns in expression
    # if group_values != []:
    #     not_grouped = set(select_aliases) - set(group_values) - set(group_dimensions)
    #     if not_grouped:
    #         raise AttributeError(f"Fields {not_grouped} must appear in the GROUP BY clause or be used in an aggregate function.")
    
    sql_blocks = {
                    "select_names": select_names, 
                    "select_aliases": select_aliases, 
                    "group_dimensions": group_dimensions, 
                    "group_values": group_values, 
                    "order_by": order_by,
                    "types": types
                }
    
    return sql_blocks

def get_sql(data):
    data['select']['sql_blocks'] = build_column_strings(data)
    sql = ''

    if "union" in data["select"]:
        iterator = 1 
        sq_data = deepcopy(data[f'sq{iterator}'])
        sql += get_sql(sq_data)

        for union in data["select"]["union"]["union_type"]:
            union_type = data["select"]["union"]["union_type"][iterator-1]
            sq_data = deepcopy(data[f'sq{iterator+1}'])
            right_table = get_sql(sq_data)
            
            sql += f" {union_type} {right_table}"
            iterator += 1

    elif "union" not in data["select"]:    
        sql += "SELECT"

        if "distinct" in data["select"] and data["select"]["distinct"] == 1:
            sql += " DISTINCT"

        selects = ', '.join(data["select"]['sql_blocks']['select_names'])
        sql += f" {selects}"

        if "table" in data["select"]:
            if "schema" in data["select"] and data["select"]["schema"] != "":
                sql += " FROM {}.{}".format(data["select"]["schema"],data["select"]["table"])
            else: 
                sql += " FROM {}".format(data["select"]["table"])

        elif "join" in data["select"]:
            iterator = 1
            sq_data = deepcopy(data[f'sq{iterator}'])
            left_table = get_sql(sq_data)
            sql += f" FROM ({left_table}) sq{iterator}"

            for join in data["select"]["join"]["join_type"]:
                join_type = data["select"]["join"]["join_type"][iterator-1]
                sq_data = deepcopy(data[f'sq{iterator+1}'])
                right_table = get_sql(sq_data)
                on = data["select"]["join"]["on"][iterator-1]

                sql += f" {join_type} ({right_table}) sq{iterator+1}"
                if not on in {0, '0'}:
                    sql += f" ON {on}"
                iterator += 1

        elif "table" not in data["select"] and "join" not in data["select"] and "sq" in data:      
            sq_data = deepcopy(data["sq"])
            sq = get_sql(sq_data)
            sql += f" FROM ({sq}) sq"

        if "where" in data["select"]:
            sql += " WHERE {}".format(data["select"]["where"])

        if data["select"]['sql_blocks']['group_dimensions'] != []:
            group_names = ', '.join(data["select"]['sql_blocks']['group_dimensions'])
            sql += f" GROUP BY {group_names}"

        if "having" in data["select"]:
            sql += " HAVING {}".format(data["select"]["having"])

    if data["select"]["sql_blocks"]["order_by"] != []:
        order_by = ', '.join(data["select"]["sql_blocks"]["order_by"])
        sql += f" ORDER BY {order_by}"

    if "limit" in data["select"] and data["select"]["limit"] != '':
        sql += " LIMIT {}".format(data["select"]["limit"])

    sql = sqlparse.format(sql, reindent=True, keyword_case="upper")
    return sql

