import pytest
import sqlparse
import os
from copy import deepcopy

from grizly.core.qframe import (
    QFrame, 
    union, 
    join
)

from grizly.io.sqlbuilder import (
    write, 
    build_column_strings, 
    get_sql
)

orders = {
    "select": {
        "fields": {
            "Order": {
                "type": "dim", 
                "as": "Bookings"
            },
            "Part": {
                "type": "dim", 
                "as": "Part"
            },
            "Customer": {
                "type": "dim", 
                "as": "Customer"
            },
            "Value": {"type": "num"
            },
        },
        "table": "Orders"
    }
}

customers = {
    "select": {
        "fields": {
            "Country": {
                "type": "dim", 
                "as": "Country"
            },
            "Customer": {
                "type": "dim", 
                "as": "Customer"
            }
        }
    }
}

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
    testsql = testsql.lower()
    return testsql


def test_validation_data():
    QFrame().validate_data(deepcopy(orders))


def test_from_dict():

    q = QFrame().from_dict(deepcopy(customers))
    assert q.data["select"]["fields"]["Country"] == {"type": "dim", "as": "Country"}

    q = QFrame().from_dict(deepcopy(orders))
    assert q.data["select"]["fields"]["Value"] == {"type": "num"}


def test_read_excel():
    excel_path = os.path.join(os.getcwd(), 'grizly', 'grizly', 'tests', 'tables.xlsx')
    q = QFrame().read_excel(excel_path,sheet_name="orders",)
    assert q.data["select"]["fields"]["Order_Nr"] == {
        "type": "dim",
        "group_by": "group",
        "as": "Order_Number",
    }

def test_create_sql_blocks():
    q = QFrame().from_dict(deepcopy(orders))
    assert build_column_strings(q).data["select"]["sql_blocks"]["select_names"] == [
        "Order as Bookings","Part", "Customer", "Value"]
    assert build_column_strings(q).data["select"]["sql_blocks"]["select_aliases"] == [
        "Bookings", "Part","Customer", "Value"]
    assert q.create_sql_blocks().data == build_column_strings(q).data


def test_rename():
    q = QFrame().from_dict(deepcopy(orders))
    q.rename({'Customer': 'Customer_Name', 'Value': 'Sales'})
    assert q.data['select']['fields']['Customer']['as'] == 'Customer_Name'
    assert q.data['select']['fields']['Value']['as'] == 'Sales'


def test_remove():
    q = QFrame().from_dict(deepcopy(orders))
    q.remove(['Part', 'Order'])
    assert 'Part' and 'Order' not in q.data['select']['fields']


def test_distinct():
    q = QFrame().from_dict(deepcopy(orders))
    q.distinct()
    sql = q.get_sql().sql
    assert sql[7:15].upper() == 'DISTINCT'

    
def test_query():
    q = QFrame().from_dict(deepcopy(orders))
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
    q = QFrame().from_dict(deepcopy(orders))
    value_x_two = "Value * 2"
    q.assign(value_x_two=value_x_two, type='num')
    q.assign(Value_div="Value/100", type='num')
    assert q.data["select"]["fields"]["value_x_two"]["expression"] == "Value * 2"
    assert q.data["select"]["fields"]["Value_div"] == {
        "type": "num", 
        "as": "Value_div", 
        "group_by": "", 
        "expression": "Value/100"
        }


def test_groupby():
    q = QFrame().from_dict(deepcopy(orders))
    q.groupby(["Order", "Customer"])
    order = {"type": "dim", "as": "Bookings", "group_by": "group"}
    customer = {"type": "dim", "as": "Customer", "group_by": "group"}
    assert q.data["select"]["fields"]["Order"] == order
    assert q.data["select"]["fields"]["Customer"] == customer


def test_agg():
    q = QFrame().from_dict(deepcopy(orders))
    q.groupby(["Order", "Customer"])["Value"].agg("sum")
    value = {"type": "num", "group_by": "sum"}
    assert q.data["select"]["fields"]["Value"] == value


def test_orderby():
    q = QFrame().from_dict(deepcopy(orders))
    q.orderby("Value")
    assert q.data["select"]["fields"]["Value"]["order_by"] == 'ASC'

    q.orderby(["Order", "Part"], ascending=[False, True])
    assert q.data["select"]["fields"]["Order"]["order_by"] == 'DESC'
    assert q.data["select"]["fields"]["Part"]["order_by"] == 'ASC'

    q.get_sql()
    sql = q.sql

    testsql = """
            SELECT
                Order AS Bookings,
                Part,
                Customer,
                Value
            FROM Orders
            ORDER BY Bookings DESC,
                    Part,
                    Value
            """
    assert clean_testexpr(sql) == clean_testexpr(testsql)



def test_limit():
    q = QFrame().from_dict(deepcopy(orders))
    q.limit(10)
    sql = q.get_sql().sql
    assert sql[-8:].upper() == 'LIMIT 10'


def test_select():
    q = QFrame().from_dict(deepcopy(orders))
    q.select(['Customer', 'Value'])
    q.groupby('sq.Customer')['sq.Value'].agg('sum')
    q.get_sql()

    sql = q.sql
    # write_out(str(sql))
    testsql = """
            SELECT sq.Customer AS Customer,
                    sum(sq.Value) AS Value
                FROM
                (SELECT
                ORDER AS Bookings,
                        Part,
                        Customer,
                        Value
                FROM Orders) sq
                GROUP BY Customer
            """
    assert clean_testexpr(sql) == clean_testexpr(testsql)


def test_get_sql():
    q = QFrame().from_dict(deepcopy(orders))
    q.assign(New_case="CASE WHEN Bookings = 100 THEN 1 ELSE 0 END", type="num")
    q.limit(5)
    q.groupby(q.data["select"]["fields"])["Value"].agg("sum")
    testsql = """SELECT Order AS Bookings,
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
    sql = q.get_sql().sql
    # write_out(str(sql))
    assert clean_testexpr(sql) == clean_testexpr(testsql)
    assert q.get_sql() == get_sql(q) 


def test_get_sql_with_select_attr():
    excel_path = os.path.join(os.getcwd(), 'grizly', 'grizly', 'tests', 'tables.xlsx')
    q = QFrame().read_excel(excel_path, sheet_name="orders")

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
    # write_out(str(sql))
    assert clean_testexpr(sql) == clean_testexpr(testsql)
    assert q.get_sql() == get_sql(q) 



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
    




