from json import dumps

from shillelagh.adapters.registry import registry
from shillelagh.backends.apsw.db import connect
from adapter import CustomJsonAPI

registry.add('customJsonAPI', CustomJsonAPI)

if __name__ == "__main__":
    connection = connect(":memory:", adapters=['customJsonAPI'])
    cursor = connection.cursor()

    # SQL = 'SELECT * from "http://3.108.177.44:9090/filter-service/v1/analytics/filter-param-rawquery"  as a WHERE a.calendar_month_year_str="Apr-2022"'
    # SQL = 'SELECT * from "http://3.108.177.44:9090/filter-service/v1/analytics/filter-param-rawquery?baseQueryName=gmv_monthly_financial_yr_v1&where%calendar_month_year_str=Apr-2022"'
    # SQL = '''SELECT * from "http://3.108.177.44:9090/filter-service/v1/analytics/filter-param-rawquery?'{\"baseQueryName\":\"channel_wise_Top10_sku_inventory_daily\",\"paramMap\":{\"whereCondStm\":\" and stm.channel_name IN ('\''Flipkart'\'','\''Amazon.in'\'')\",\"whereCondStm1\":\" and stm1.channel_name IN ('\''Flipkart'\'','\''Amazon.in'\'')\"},\"customCondition\":{\"replaceAllParams\":true}}'"'''

    # SQL ='''SELECT * from "http://3.108.177.44:9090/filter-service/v1/analytics/filter-param-rawquery?arg_baseQueryName=gmv_monthly_financial_yr_v1&arg_paramMap%whereCondStm=and stm.channel_name IN ('Flipkart','Amazon.in')&&arg_paramMap%whereCondStm1=and stm1.channel_name IN ('Flipkart','Amazon.in')&arg_customCondition%replaceAllParam=true"'''

    SQL = '''SELECT * from "http://3.108.177.44:9090/filter-service/v1/analytics/filter-param-rawquery?arg_baseQueryName=distinctCategoryNames&arg_paramMap%whereCondStm=and stm.calendar_date between '2023-01-17T11:02' and '2023-01-24T11:02'&arg_paramMap%whereCondStm1=and stm1.calendar_date between '2023-02-17T12:02' and '2023-02-24T12:02'&arg_customCondition%replaceAllParam=true"'''

    for row in cursor.execute(SQL):
        print(row)
