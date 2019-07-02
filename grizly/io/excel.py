import pandas


def read_excel(excel_path, sheet_name=""):
    if sheet_name != "":
        fields = pandas.read_excel(excel_path, sheet_name=sheet_name).fillna("")
    else:
        fields = pandas.read_excel(excel_path).fillna("")
    schema = fields["schema"][0]
    table = fields["table"][0]

    columns_qf = {}
    for index, row in fields.iterrows():
        columns_qf[row["column"]] = {}
        columns_qf[row["column"]]["type"] = row["column_type"]
        columns_qf[row["column"]]["group_by"] = row["group_by"]
        if row["column_as"] != "":
            columns_qf[row["column"]]["as"] = row["column_as"]
    return schema, table, columns_qf
