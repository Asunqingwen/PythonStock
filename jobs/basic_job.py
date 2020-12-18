import libs.common as common
import sys
import time
import pandas as pd
import tushare as ts
from sqlalchemy.types import NVARCHAR
from sqlalchemy import inspect
import datetime
import MySQLdb


####### 3.pdf 方法。宏观经济数据
# 接口全部有错误。只专注股票数据。
def stat_all(datetime_):
    # data = ts.get_deposit_rate()
    # print(data)

    data = ts.get_hs300s()
    print(data)


# main函数入口
if __name__ == '__main__':
    # 检查，如果执行 select 1 失败，说明数据库不存在，然后创建一个新的数据库。
    try:
        with common.conn() as db:
            db.execute("select 1")
    except Exception as e:
        print("check MARIADB_DB error and create new one :", e)
        # 创建新数据库
        common.create_new_database()
    # 执行数据初始化
    # 使用方法传递
    common.init_pro_api()
    common.run_with_args(stat_all)
