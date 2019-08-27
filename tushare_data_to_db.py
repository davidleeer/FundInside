# -*- coding: utf-8 -*-
"""
Created on Thu Aug 15 10:17:06 2019
import data from Tushare and store as df
for the limitation of access 250 stocks each time

@author: David
"""

import MySQLdb as mdb
import pymysql
import tushare as ts
import datetime as dt
import pandas as pd

pro = ts.pro_api()

tick_list = pro.stock_basic(exchange='', list_status='L',
                            fields='ts_code, symbol, name, area, industry, \
                            list_date')

now = dt.datetime.utcnow()
df = pd.DataFrame()
for i in range(1000, 1250):
    d = tick_list['ts_code'][i]

    df_temp = pro.daily(ts_code=d, start_date='20100101', end_date='20190720')

    df = df.append(df_temp)

df['created_date'] = now
df['last_updated_date'] = now

# define functon to insert df into database
pymysql.install_as_MySQLdb()


def insert_daily_data_into_db(data, table, name):

    daily_data = []
    dv_id = 'TUSHARE'
    for i in range(0, len(data)):
        d = data.iloc[i]
        # dt1 = data.index[i].date()
        dt2 = d[11].date()
        dt3 = d[12].date()
        dt4 = (dv_id, d[0], d[1], dt2, dt3,
               d[2].item(), d[3].item(), d[4].item(), d[5].item(),
               d[6].item(), d[7].item(), d[8].item(),
               d[9].item(), d[10].item())

        daily_data.append(dt4)

    db_host = 'localhost'
    db_user = 'super_user'
    db_pass = 'password'
    db_name = 'securities_china'

    con = mdb.connect(
        host=db_host, user=db_user, passwd=db_pass, db=db_name
    )

    final_str = 'INSERT INTO daily_price (%s) \n VALUES (%s) ' \
        % (table, name)

    cur = con.cursor()
    cur.executemany(final_str, daily_data)
    con.commit()


# execute function with parameters
column_str = '''data_vendor_id, ts_code, trade_date, created_date, last_updated_date, high_price, low_price, open_price,
                    close_price, pre_close_price, change_price, pct_change, volume, amount'''
insert_str = ('%s, ' * 14)[:-2]

insert_daily_data_into_db(df, column_str, insert_str)

# check db status
pymysql.install_as_MySQLdb()


db_host = 'localhost'
db_user = 'super_user'
db_pass = 'password'
db_name = 'securities_china'

con = mdb.connect(
    host=db_host, user=db_user, passwd=db_pass, db=db_name
)

str = '''SELECT * FROM daily_price'''
cur = con.cursor()
cur.execute(str)
