import pytest
import sqlparse
import os
from ..api import QFrame, union, join

from grizly.io.sqlbuilder import (
    write, 
    build_column_strings, 
    get_sql
)

from grizly.io.etl import (
    to_csv,
    create_table,
    csv_to_s3,
    s3_to_csv,
    s3_to_rds
)

from grizly.core.utils import (
    read_store,
    check_if_exists,
    delete_where
)

def write_out(out):
    with open(
        os.getcwd() + "\\grizly\\grizly\\tests\\output.sql",
        "w",
    ) as f:
        f.write(out)


def clean_testexpr(testsql):
    testsql = testsql.replace("\n", "")
    testsql = testsql.replace("\t", "")
    testsql = testsql.replace("\r", "")
    testsql = testsql.replace("  ", "")
    testsql = testsql.replace(" ", "")
    return testsql


def test_from_dict():
    customers = {
        "select": {
            "fields": {
                "Country": {"type": "dim", "as": "Country"},
                "Customer": {"type": "dim", "as": "Customer"}
            }
        }
    }
    orders = {
        "select": {
            "fields": {
                "Order": {"type": "dim", "as": "Bookings"},
                "Part": {"type": "dim", "as": "Part"},
                "Customer": {"type": "dim", "as": "Customer"},
                "Value": {"type": "num"}
            },
            "table": "Orders",
        }
    }

    q = QFrame().from_dict(customers)

    assert q.data["select"]["fields"]["Country"] == {"type": "dim", "as": "Country"}

def test_from_dict_2():
    customers = {
        "select": {
            "fields": {
                "Country": {"type": "dim", "as": "Country"},
                "Customer": {"type": "dim", "as": "Customer"}
            }
        }
    }
    orders = {
        "select": {
            "fields": {
                "Order": {"type": "dim", "as": "Bookings"},
                "Part": {"type": "dim", "as": "Part"},
                "Customer": {"type": "dim", "as": "Customer"},
                "Value": {"type": "num"}
            },
            "table": "Orders"
        }

    }

    q = QFrame().from_dict(orders)

    assert q.data["select"]["fields"]["Value"] == {"type": "num"}

def test_read_excel():
    q = QFrame().read_excel(
        os.getcwd() + "\\grizly\\grizly\\tests\\tables.xlsx",
        sheet_name="orders",
    )
    assert q.data["select"]["fields"]["Order_Nr"] == {
        "type": "dim",
        "group_by": "group",
        "as": "Order_Number",
    }

def test_query():
    orders = {
        "select": {
            "fields": {
                "Order": {"type": "dim", "as": "Bookings"},
                "Part": {"type": "dim", "as": "Part"},
                "Customer": {"type": "dim", "as": "Customer"},
                "Value": {"type": "num"}
            },
            "table": "Orders"
        }
    }

    q = QFrame().from_dict(orders)
    expr = q.query(
        """country!='Italy' 
                and (Customer='Enel' or Customer='Agip')
                or Value>1000
            """
    )
    testexpr = """country!='Italy' 
                and (Customer='Enel' or Customer='Agip')
                or Value>1000
            """
    assert expr.data["select"]["where"] == testexpr


def test_assign():
    orders = {
        "select": {
            "fields": {
                "Order": {"type": "dim", "as": "Bookings"},
                "Part": {"type": "dim", "as": "Part"},
                "Customer": {"type": "dim", "as": "Customer"},
                "Value": {"type": "num"},
            },
            "table": "Orders"
        }
    }

    q = QFrame().from_dict(orders)
    value_x_two = "Value * 2"
    q.assign(value_x_two=value_x_two)
    assert q.data["select"]["fields"]["value_x_two"]["expression"] == "Orders.Value * 2"

def test_group_by():
    orders = {
        "select": {
            "fields": {
                "Order": {"type": "dim", "as": "Bookings"},
                "Part": {"type": "dim", "as": "Part"},
                "Customer": {"type": "dim", "as": "Customer"},
                "Value": {"type": "num"}
            },
            "table": "Orders"
        }
    }
    q = QFrame().from_dict(orders)
    q.groupby(["Order", "Customer"])
    order = {"type": "dim", "as": "Bookings", "group_by": "group"}
    customer = {"type": "dim", "as": "Customer", "group_by": "group"}
    assert q.data["select"]["fields"]["Order"] == order
    assert q.data["select"]["fields"]["Customer"] == customer


def test_groupby_agg():
    orders = {
        "select": {
            "fields": {
                "Order": {"type": "dim", "as": "Bookings"},
                "Part": {"type": "dim", "as": "Part"},
                "Customer": {"type": "dim", "as": "Customer"},
                "Value": {"type": "num"}
            },
            "table": "Orders"
        }
    }
    q = QFrame().from_dict(orders)
    q.groupby(["Order", "Customer"])["Value"].agg("sum")
    value = {"type": "num", "group_by": "sum", "as": "Value"}
    assert q.data["select"]["fields"]["Value"] == value

def test_write_sql_table():
    orders = {
        "select": {
            "fields": {
                "Order_Nr": {"type": "dim", "as": "Bookings"},
                "Part": {"type": "dim", "as": "Part"},
                "Customer": {"type": "dim"},
                "Value": {"type": "num"}
            },
            "table": "Orders"
        }
    }
    q = QFrame().from_dict(orders)
    #sql = write(q, "Orders", drop=True)

def test_to_sql():
    q = QFrame().read_excel(
        os.getcwd() + "\\grizly\\grizly\\tests\\tables.xlsx",
        sheet_name="cb_invoices",
    )
    engine = "sqlite:///" + os.getcwd() + "\\grizly\\grizly\\tests\\chinook.db"
    q.assign(sales="Quantity*UnitPrice", type='num')
    q.groupby(["TrackId"])[("Quantity")].agg("sum")
    q.get_sql()
    df = q.to_sql(engine_string=engine)
    testdata = str( {
            'select': { 
                'fields': {
                    'InvoiceLineId': {'type': 'dim', 'group_by': ''}, 
                    'InvoiceId': {'type': 'dim', 'group_by': ''}, 'TrackId': {'type': 'dim', 'group_by': 'group'}, 
                    'UnitPrice': {'type': 'num', 'group_by': ''}, 
                    'Quantity': {'type': 'num', 'group_by': 'sum', 'as': 'Quantity'}, 
                    'sales': {'type': 'num', 'as': 'sales', 'group_by': '', 'expression': 'Quantity*UnitPrice'}
                }, 
                'schema': '', 
                'table': 'invoice_items', 
                'sql_blocks': {
                    'select_names': ['InvoiceLineId', 'InvoiceId', 'TrackId', 'UnitPrice', 'sum(Quantity) as Quantity', 'Quantity*UnitPrice as sales'], 
                    'select_aliases': ['InvoiceLineId', 'InvoiceId', 'TrackId', 'UnitPrice', 'Quantity', 'sales'], 
                    'group_dimensions': ['TrackId'], 
                    'group_values': ['Quantity'], 
                    'types': ['VARCHAR(500)', 'VARCHAR(500)', 'VARCHAR(500)', 'FLOAT(53)', 'FLOAT(53)', 'FLOAT(53)']
                }
            }
        }
    )
    
    # write_out(str(q.data))
    assert testdata == str(q.data)

def test_validation_data():
    orders = {
        "select": {
            "fields": {
                "Order_Nr": {"type": "dim", "as": "Bookings"},
                "Part": {"type": "dim", "as": "Part"},
                "Customer": {"type": "num"},
                "Value": {"type": "num"}
            },
            "table": "Orders"
        }
    }
    QFrame().validate_data(orders)

def test_build_column_strings():
    orders = {
        "select": {
            "fields": {
                "Order_Nr": {"type": "dim", "as": "Bookings"},
                "Value": {"type": "num"},
                "Value_div": {"type": "num", "as": "Value_div", "group_by": "", "expression": "Value/100"}
            },
            "table": "Orders"
        }
    }
    q = QFrame().from_dict(orders)
    assert build_column_strings(q).data["select"]["sql_blocks"]["select_names"] == ["Order_Nr as Bookings","Value", "Value/100 as Value_div"]
    assert build_column_strings(q).data["select"]["sql_blocks"]["select_aliases"] == ["Bookings", "Value","Value_div"]

def test_create_sql_blocks():
    orders = {
        "select": {
            "fields": {
                "Order_Nr": {"type": "dim", "as": "Bookings"},
                "Part": {"type": "dim", "as": "Part"},
                "Customer": {"type": "dim"},
                "Value": {"type": "num"},
                "Value_div": {"type": "num", "as": "Value_div", "group_by": "", "expression": "Orders.Value/100"}
            },
            "table": "Orders",
        }
    }
    q = QFrame().from_dict(orders)
    assert q.create_sql_blocks().data == build_column_strings(q).data

def test_assign():
    orders = {
        "select": {
            "fields": {
                "Order_Nr": {"type": "dim", "as": "Bookings"},
                "Part": {"type": "dim", "as": "Part"},
                "Customer": {"type": "dim"},
                "Value": {"type": "num"}
            },
            "table": "Orders"
        }
    }
    q = QFrame().from_dict(orders).assign(Value_div="Value/100", type='num')
    assert q.data["select"]["fields"]["Value_div"] == {"type": "num", "as": "Value_div", "group_by": "", "expression": "Value/100"}

def test_get_sql():
    orders = {
        "select": {
            "fields": {
                "Order_Nr": {"type": "dim", "as": "Bookings"},
                "Part": {"type": "dim"},
                "Customer": {"type": "dim"},
                "Value": {"type": "num"},
                "New_case": {"type": "num", "as": "New_case",  "group_by": "", "expression": "CASE WHEN Bookings = 100 THEN 1 ELSE 0 END"},
            },
            "table": "Orders"
        }
    }
    q = QFrame().from_dict(orders)
    q.limit(5)
    q.groupby(q.data["select"]["fields"])["Value"].agg("sum")
    testsql = """SELECT Order_Nr AS Bookings,
                    Part,
                    Customer,
                    sum(Value) AS Value,
                    CASE
                        WHEN Bookings = 100 THEN 1
                        ELSE 0
                    END AS New_case
                FROM Orders
                GROUP BY Bookings,
                        Part,
                        Customer,
                        New_case
                LIMIT 5
            """
    sql = get_sql(q).sql
    # write_out(str(sql))
    assert clean_testexpr(sql) == clean_testexpr(testsql)



def test_get_sql_with_select_attr():
    q = QFrame().read_excel(os.getcwd() + "\\grizly\\grizly\\tests\\tables.xlsx", sheet_name="orders")

    testsql = """
        SELECT Order_Nr AS Order_Number, 
                Part,
                CustomerID_1,
                sum(Value) AS Value,
                CASE
                    WHEN CustomerID_1 <> NULL THEN CustomerID_1
                    ELSE CustomerID_2
                END AS CustomerID
        FROM orders_schema.orders
        GROUP BY Order_Number,
                Part,
                CustomerID_1,
                CustomerID_2
            """

    sql = q.get_sql().sql
    write_out(str(sql))
    assert clean_testexpr(sql) == clean_testexpr(testsql)


def test_check_if_exists():
    assert check_if_exists('fiscal_calendar_weeks','baseviews') == True