import yaml

table_define_fc = None
application = None
table_define = []  # {"s_d": "","s_t": "","t_d": "","t_t": ""}
total_table_count = 0


def load_config():
    global table_define_fc
    global application
    with open("table_define.txt", "r", encoding="utf-8")as f:
        table_define_fc = f.readlines()
    with open("application.yaml", "r", encoding="utf-8")as f:
        application = yaml.safe_load(f.read())


def gen_table_define():
    def append_table_define(s_d, s_t, t_d, t_t):
        global total_table_count
        table_define.append({"s_d": s_d, "s_t": s_t, "t_d": t_d, "t_t": t_t})
        total_table_count += 1
        if t_d in application["db_mapping"]:
            table_define.append({"s_d": s_d, "s_t": s_t, "t_d": application["db_mapping"][t_d], "t_t": t_t})
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
    pass


if __name__ == '__main__':
    load_config()
    gen_table_define()
    print(table_define)
    print(total_table_count)
    gen_select_sql()
