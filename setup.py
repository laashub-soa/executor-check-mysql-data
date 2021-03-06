import os
import time

import yaml

from component import mymysql, dingding_webhook

table_define_fc = None
application = None
db_config = None
table_defines = []  # {"s_d": "","s_t": "","t_d": "","t_t": ""}
total_table_count = 0
select_sql_list = []
now_time_second = int(time.time())

if not os.path.exists("temp"):
    os.mkdir("temp")
if not os.path.exists(os.path.join("temp", "running_position_record.txt")):
    with open(os.path.join("temp", "running_position_record.txt"), "w", encoding="utf-8")as f:
        f.write("")


def load_config():
    global table_define_fc
    global application
    global db_config
    with open(os.path.join("configs", "table_define.txt"), "r", encoding="utf-8")as f:
        table_define_fc = f.readlines()
    with open(os.path.join("configs", "application.yaml"), "r", encoding="utf-8")as f:
        application = yaml.safe_load(f.read())
    with open(os.path.join("configs", "db.yaml"), "r", encoding="utf-8")as f:
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
    with open(os.path.join("temp", "running_position_record.txt"), "r", encoding="utf-8")as f:
        running_position_record_fc = f.readlines()
    return running_id + "\n" in running_position_record_fc


def running_position_record(s_d, s_t, t_d, t_t):
    running_id = "%s_%s_%s_%s" % (s_d, s_t, t_d, t_t)
    with open(os.path.join("temp", "running_position_record.txt"), "r", encoding="utf-8")as f:
        running_position_record_fc = f.readlines()
        if running_id not in running_position_record_fc:
            with open(os.path.join("temp", "running_position_record.txt"), "a", encoding="utf-8")as f:
                f.write(running_id + "\n")


def diff_source_target_data_count_too_many(s_d, s_t, t_d, t_t, diff_count):
    file_path = os.path.join("temp", "diff_source_target_data_count_too_many.txt")
    with open(file_path, "a")as f:
        f.write("%s.%s:%s.%s:%s\n" % (s_d, s_t, t_d, t_t, diff_count))


def execute_select_sql():
    index = 0
    for item in select_sql_list:
        if type(item) == str:
            continue
        index += 1
        source_select_sql = item["source_select_sql"]
        target_select_sql = item["target_select_sql"]
        s_d = item["s_d"]
        s_t = item["s_t"]
        t_d = item["t_d"]
        t_t = item["t_t"]
        if running_position_is_can_skip(s_d, s_t, t_d, t_t):
            print_to_file("??????")
            continue
        print_to_file("????????????: " + str(int((index / len(select_sql_list)) * 100)) + "%" + "(%s.%s)" % (t_d, t_t))
        # source
        source_select_result = mymysql.query(db_config["source"], source_select_sql)
        source_select_result = source_select_result[0]["total_count"]
        # print(source_select_result)
        # target
        target_select_result = mymysql.query(db_config["target"], target_select_sql)
        target_select_result = target_select_result[0]["total_count"]
        # print(target_select_result)

        diff_source_target_data_count = int(source_select_result) - int(target_select_result)
        if diff_source_target_data_count > application["maximum_tolerance_count"]:
            diff_source_target_data_count_too_many(s_d, s_t, t_d, t_t, diff_source_target_data_count)
            alarm_msg = """
            ?????????(%s.%s)??????????????????(%s)
            ?????????????
            ????????????: %s
            ???????????????: %s
            """ % (t_d, t_t, diff_source_target_data_count, source_select_sql, target_select_sql)
            print_to_file(alarm_msg)
            do_alarm(alarm_msg)
        running_position_record(s_d, s_t, t_d, t_t)
        time.sleep(1)
    print_to_file("????????????")


def print_to_file(msg_str):
    print(msg_str)
    msg_str += "\n"
    global now_time_second
    temp_print_path = os.path.join("temp", "prints")
    if not os.path.exists(temp_print_path):
        os.mkdir(temp_print_path)
    if not os.path.exists(temp_print_path):
        os.mkdir(temp_print_path)
    print_file_path = os.path.join(temp_print_path, "print-%s.txt" % now_time_second)
    if not os.path.exists(print_file_path):
        with open(print_file_path, "w")as f:
            f.write("")
    with open(print_file_path, "a", encoding="utf-8")as f:
        f.write(str(msg_str))


def reset():
    with open(os.path.join("temp", "running_position_record.txt"), "w", encoding="utf-8")as f:
        f.write("")


def service():
    load_config()
    gen_table_define()
    # print(table_defines)
    # print(total_table_count)
    gen_select_sql()
    print_to_file(select_sql_list)
    execute_select_sql()
    reset()


def do_alarm(alarm_msg):
    at_phones = application["follow_of_user"]
    at_phones_str = "  "
    for item in at_phones:
        at_phones_str += "@" + str(item)
    alarm_result = dingding_webhook.alarm(application["dingding_webhook_access_token"][0], "check",
                                          alarm_msg + at_phones_str, at_phones)
    return alarm_result


if __name__ == '__main__':
    while True:
        try:
            service()
        except Exception as e:
            import traceback, sys

            traceback.print_exc()  # ??????????????????
            exc_type, exc_value, exc_traceback = sys.exc_info()
            error = str(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))
            msg_template_details = error
            print_to_file(do_alarm(msg_template_details))

        time.sleep(2 * 60 * 60)  # ??????2?????????????????????????????????????????????
