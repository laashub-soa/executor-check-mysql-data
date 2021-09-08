import os
import time

import yaml

from component import mymysql

table_define_fc = None
application = None
db_config = None
table_defines = []  # {"s_d": "","s_t": "","t_d": "","t_t": ""}
total_table_count = 0
select_sql_list = []
now_time_second = int(time.time())

if not os.path.exists("running_position_record.txt"):
    with open("running_position_record.txt", "w", encoding="utf-8")as f:
        f.write("")


def load_config():
    global table_define_fc
    global application
    global db_config
    with open("table_define.txt", "r", encoding="utf-8")as f:
        table_define_fc = f.readlines()
    with open("application.yaml", "r", encoding="utf-8")as f:
        application = yaml.safe_load(f.read())
    with open("db.yaml", "r", encoding="utf-8")as f:
        db_config = yaml.safe_load(f.read())


def gen_table_define():
    def append_table_define(s_d, s_t, t_d, t_t):
        global total_table_count
        table_defines.append({"s_d": s_d, "s_t": s_t, "t_d": t_d, "t_t": t_t})
        total_table_count += 1
        if t_d in application["db_mapping"]:
            table_defines.append({"s_d": s_d, "s_t": s_t, "t_d": application["db_mapping"][t_d], "t_t": t_t})
            total_table_count += 1

    revert_table_distribute_merge = {}
    for key in application["table_distribute_merge"]:
        for item in application["table_distribute_merge"][key]:
            revert_table_distribute_merge[item] = key
    db_name = None
    table_define_merge_record = {}  # {"m-wms": "wms"}
    is_skip_db = True
    for item in table_define_fc:
        item = item.strip()
        if "" == item:
            continue
        if "(" in item:
            is_skip_db = False
            db_name = item[:item.find("(")]
            if db_name in revert_table_distribute_merge:
                db_name = revert_table_distribute_merge[db_name]
                if db_name in table_define_merge_record:
                    if item == table_define_merge_record[db_name]:
                        is_skip_db = True
            continue
        if is_skip_db:
            continue
        append_table_define(db_name, item, db_name, item)


def gen_select_sql():
    for item in table_defines:
        s_d = item["s_d"]
        s_t = item["s_t"]
        t_d = item["t_d"]
        t_t = item["t_t"]

        def gen_select_sql_detail(db_name, table_name):
            if "-" in db_name:
                table_distribute_unpacks = application["table_distribute_merge"][db_name]
                select_sql_detail = "select "
                for table_distribute_unpack_item in table_distribute_unpacks:
                    if select_sql_detail != "select":
                        select_sql_detail += "+"
                    select_sql_detail += "(select count(1)  from %s.%s)" % (table_distribute_unpack_item, table_name)
                select_sql_detail += " as total_count "
            else:
                select_sql_detail = "select count(1) as total_count from %s.%s;" % (db_name, table_name)
            return select_sql_detail

        source_select_sql = gen_select_sql_detail(s_d, s_t)
        target_select_sql = gen_select_sql_detail(t_d, t_t)
        select_sql_list.append({
            "source_select_sql": source_select_sql,
            "target_select_sql": target_select_sql,
            "s_d": s_d,
            "s_t": s_t,
            "t_d": t_d,
            "t_t": t_t,
        })


def running_position_is_can_skip(s_d, s_t, t_d, t_t):
    running_id = "%s_%s_%s_%s" % (s_d, s_t, t_d, t_t)
    with open("running_position_record.txt", "r", encoding="utf-8")as f:
        running_position_record_fc = f.readlines()
    return running_id + "\n" in running_position_record_fc


def running_position_record(s_d, s_t, t_d, t_t):
    running_id = "%s_%s_%s_%s" % (s_d, s_t, t_d, t_t)
    with open("running_position_record.txt", "r", encoding="utf-8")as f:
        running_position_record_fc = f.readlines()
        if running_id not in running_position_record_fc:
            with open("running_position_record.txt", "a", encoding="utf-8")as f:
                f.write(running_id + "\n")


def execute_select_sql():
    for index in range(len(select_sql_list) - 1):
        item = select_sql_list[index]
        source_select_sql = item["source_select_sql"]
        target_select_sql = item["target_select_sql"]
        s_d = item["s_d"]
        s_t = item["s_t"]
        t_d = item["t_d"]
        t_t = item["t_t"]
        if running_position_is_can_skip(s_d, s_t, t_d, t_t):
            print_to_file("跳过")
            continue
        print_to_file("执行进度: " + str(index / len(select_sql_list)) + "%" + "(%s.%s)" % (t_d, t_t))
        # source
        source_select_result = mymysql.query(db_config["source"], source_select_sql)
        source_select_result = source_select_result[0]["total_count"]
        # print(source_select_result)
        # target
        target_select_result = mymysql.query(db_config["target"], target_select_sql)
        target_select_result = target_select_result[0]["total_count"]
        # print(target_select_result)

        if int(source_select_result) - int(target_select_result) > application["maximum_tolerance_count"]:
            # TODO 告警提示两边表数据相差过大
            alarm_msg = """
            两边表(%s.%s)数据相差过大
            如何检查?
            源端执行: %s
            目标端执行: %s
            """ % (t_d, t_t, source_select_sql, target_select_sql)
            print_to_file(alarm_msg)
        running_position_record(s_d, s_t, t_d, t_t)
        time.sleep(1)
    print_to_file("检查完成")


def print_to_file(msg_str):
    print(msg_str)
    msg_str += "\n"
    global now_time_second
    if not os.path.exists("prints"):
        os.mkdir("prints")
    print_file_path = os.path.join("prints", "print-%s.txt" % now_time_second)
    if not os.path.exists(print_file_path):
        with open(print_file_path, "w")as f:
            f.write("")
    with open(print_file_path, "a", encoding="utf-8")as f:
        f.write(str(msg_str))


if __name__ == '__main__':
    load_config()
    gen_table_define()
    # print(table_defines)
    # print(total_table_count)
    gen_select_sql()
    print_to_file(select_sql_list)
    execute_select_sql()
