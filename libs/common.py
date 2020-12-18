'''
数据库配置
'''
import platform
from datetime import datetime, timedelta
import time
import sys
import os
import MySQLdb
from sqlalchemy import create_engine
from sqlalchemy.types import NVARCHAR
from sqlalchemy import inspect
import tushare as ts
import pandas as pd
import traceback

MARIADB_HOST = "192.168.10.69" if (os.environ.get('MARIADB_HOST') is None) else os.environ.get('MARIADB_HOST')
MARIADB_USER = "root" if (os.environ.get('MARIADB_USER') is None) else os.environ.get('MARIADB_USER')
MARIADB_PWD = "root" if (os.environ.get('MARIADB_PWD') is None) else os.environ.get('MARIADB_PWD')
MARIADB_DB = "stock_data" if (os.environ.get('MARIADB_DB') is None) else os.environ.get('MARIADB_DB')

print("MARIADB_HOST :", MARIADB_HOST, ",MARIADB_USER :", MARIADB_USER, ",MARIADB_DB :", MARIADB_DB)
MARIADB_CONN_URL = "mysql+mysqldb://" + MARIADB_USER + ":" + MARIADB_PWD + "@" + MARIADB_HOST + ":3306/" + MARIADB_DB + "?charset=utf8"
print("MARIADB_CONN_URL :", MARIADB_CONN_URL)

# tushare的TOKEN
TUSHARE_TOKEN = '57eb22505d66053e2995118bcb882775a83324006a72fa98ebc1eee7'


# 初始化pro接口
def init_pro_api():
    tushare_token = os.environ.get('TUSHARE')
    if tushare_token is None:
        ts.set_token(TUSHARE_TOKEN)
    else:
        ts.set_token(tushare_token)
    pro = ts.pro_api()
    return pro


def engine():
    engine = create_engine(
        MARIADB_CONN_URL,
        encoding='utf8',
        convert_unicode=True
    )
    return engine


def engine_to_db(to_db):
    MARIADB_CONN_URL_NEW = "mysql+mysqldb://" + MARIADB_USER + ":" + MARIADB_PWD + "@" + MARIADB_HOST + ":3306/" + to_db + "?charset=utf8"
    engine = create_engine(
        MARIADB_CONN_URL_NEW,
        encoding='utf8',
        convert_unicode=True
    )
    return engine


def create_new_database():
    with MySQLdb.connect(MARIADB_HOST, MARIADB_USER, MARIADB_PWD, 'mysql', charset="utf8") as db:
        try:
            create_sql = "CREATE DATABASE IF NOT EXISTS %s CHARACTER SET utf8 COLLATE utf8_general_ci" % MARIADB_DB
            print(create_sql)
            db.autocommit(on=True)
            db.cursor().execute(create_sql)
        except Exception as e:
            print("error CREATE DATABASE :", e)


# 连接数据库
def conn():
    db = None
    try:
        db = MySQLdb.connect(MARIADB_HOST, MARIADB_USER, MARIADB_PWD, MARIADB_DB, charset='utf8')
        # db.autocommit(on=True)
    except Exception as e:
        print('Error connecting to MariaDB Platform: ', e)
    db.autocommit(on=True)
    return db.cursor()


# 插入数据
def insert(sql, params=()):
    with conn() as db:
        print("insert sql:" + sql)
        try:
            db.execute(sql, params)
        except Exception as e:
            print("error :", e)


# 查询数据
def select(sql, params=()):
    with conn() as db:
        print("select sql:" + sql)
        try:
            db.execute(sql, params)
        except Exception as e:
            print("error :", e)
        result = db.fetchall()
        return result


# 通用函数，获得日期参数
def run_with_args(run_func):
    datetime_begin = datetime.now()  # 默认当日执行
    datetime_begin_str = datetime_begin.strftime("%Y-%m-%d %H:%M:%S.%f")
    db_str = "MARIADB_HOST :" + MARIADB_HOST + ",MARIADB_USER :" + MARIADB_USER + ",MARIADB_DB :" + MARIADB_DB
    print("\n######################### " + db_str + "  ######################### ")
    print("\n######################### begin run %s %s  #########################" % (run_func, datetime_begin_str))
    start = time.time()
    # 支持数据重跑机制,最多两个参数——日期，循环次数
    argvs = sys.argv
    # python3 xxx.py 2020-12-8 10 or python3 xxx.py 2020-12-8
    if len(argvs) > 1:
        year, month, day = argvs[1].split('-')
        datetime_begin = datetime(int(year), int(month), int(day))
    loop = 1
    if len(argvs) > 2:
        loop = int(argvs[2])
    for i in range(loop):
        # 循环插入多次数据，重复跑历史数据使用。
        # time.sleep(5)
        datetime_new = datetime_begin + timedelta(days=i)
        try:
            run_func(datetime_new)
        except Exception as e:
            print("error :", e)
            traceback.print_exc()
    print("######################### finish %s , use time: %s #########################" % (
        datetime_begin_str, time.time() - start))


if __name__ == '__main__':
    conn()
