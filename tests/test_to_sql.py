import pytest
from ..api import QFrame, sql


def get_query():
    columns = {'FY18_Sales':{'type':'num'}, 'FY19_Sales':{'type':'num'}
           , 'Country':{'type':'dim'}, 'Year':{'type':'dum'}
          }
    q = QFrame(schema='te_industrial', table='sales_weekly_flash').from_dict(columns)
    q['Test'] = {'type':'num'}
    q.groupby(['Country'])['FY18_Sales', 'FY19_Sales'].agg('sum')
    q[(q['Country'] == 'Italy') 
      & (q['Country'] == 'Germany') 
      & (q['Country'] != 'France') 
      & (q['Year'] != 1006)
      & (q['Test'] == 1000)
     ]
    return q
    
def test_query_with_group_by():
    q = get_query()
    testsql = """SELECT FY18_Sales, FY19_Sales, Country, Year, Test, sum(sales_weekly_flash.FY18_Sales), sum(sales_weekly_flash.FY19_Sales) FROM te_industrial.sales_weekly_flash WHERE Country='Italy' and Country='Germany' and Country<>'France' and Year<>1006 and Test=1000 GROUP BY sales_weekly_flash.Country"""
    sqlstring = sql(q)
    assert sqlstring == testsql
    

def test_field_calculations():
    q = get_query()
    q['2_Year_Sales'] = q['FY18_Sales'] + q['FY19_Sales']
    expr1 = q.fields['2_Year_Sales']['expr']
    testexpr1 = 'sales_weekly_flash.FY18_Sales + sales_weekly_flash.FY19_Sales'
    
    q['YoY_Delta'] = q['FY18_Sales'] - q['FY19_Sales']
    expr2 = q.fields['YoY_Delta']['expr']
    testexpr2 = 'sales_weekly_flash.FY18_Sales - sales_weekly_flash.FY19_Sales'
    
    q['Sales_and_Integers'] = q['FY18_Sales'] - q['FY20_Sales'] - 1 + q['FY20_Sales'] + 2
    expr3 = q.fields['Sales_and_Integers']['expr']
    testexpr3 = 'sales_weekly_flash.FY18_Sales - sales_weekly_flash.FY20_Sales - 1 + sales_weekly_flash.FY20_Sales + 2'
    
    assert expr1 == testexpr1
    assert expr2 == testexpr2
    assert expr3 == testexpr3