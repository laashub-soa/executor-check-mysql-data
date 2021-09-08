import time

import pymysql.cursors


# 参考文档: https://github.com/PyMySQL/PyMySQL

def get_connection(mysql_config):
    """
    获取连接
    :param mysql_config: 数据库配置信息
    :return: 数据库连接
    """
    mysql_config["cursorclass"] = pymysql.cursors.DictCursor
    connection = pymysql.connect(**mysql_config)
    return connection


def query(mysql_config, sql, params=None):
    """
    查询
    :param mysql_config: 数据库配置信息
    :param sql: sql语句
    :param params: 参数
    :return: 查询结果列表
    """
    if not params:
        params = {}
    connection = get_connection(mysql_config)
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(sql, params)
            result = cursor.fetchall()
    return result


def change(mysql_config, sql):
    # def change(mysql_config, sql, params=None):
    """
    变更
    :param mysql_config: 数据库配置信息
    :param sql: sql语句
    :param params: 参数
    :return: 变更数据行的id列表
    """
    # if not params:
    #     params = {}
    if not isinstance(sql, list):
        sql = [sql]
    execute_result = []
    connection = get_connection(mysql_config)
    with connection:
        for idx, item in enumerate(sql):
            with connection.cursor() as cursor:
                num = cursor.execute(item)
                # num = cursor.execute(item, params)
                if num is None:
                    raise Exception("SQL执行异常, 请检查SQL语句以及两边的数据库表结构字段类型及长度")
                if num > 0:
                    last_rowid = int(cursor.lastrowid)
                    execute_result = execute_result + list(range(last_rowid - num + 1, last_rowid + 1))
                connection.commit()
        time.sleep(0.5)
    return execute_result
