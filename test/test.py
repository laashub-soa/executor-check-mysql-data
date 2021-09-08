import yaml

table_define_fc = None
application_fc = None


def load_config():
    global table_define_fc
    global application_fc
    with open("table_define.txt", "r", encoding="utf-8")as f:
        table_define_fc = f.read()
    with open("application.yaml", "r", encoding="utf-8")as f:
        application_fc = yaml.safe_load(f.read())


def gen_select_sql():
    pass


if __name__ == '__main__':
    load_config()
    gen_select_sql()
