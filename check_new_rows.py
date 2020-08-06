import pandas as pd
import re
import psycopg2
import config

only_dight = re.compile('[^0-9]')
only_float = re.compile('[^0-9,]')

DB = psycopg2.connect(options=f'-c search_path={config.options}', database=config.database, user=config.user,
                      password=config.password, host=config.host, port=config.port)
MY_CURSOR = DB.cursor()

MY_CURSOR.execute('select inn from person')
inn = MY_CURSOR.fetchall()
inns = []
for i in inn:
    i = list(i)
    inns.append(int(i[0]))

new_decl = pd.read_excel('declarations/declarations.xlsx', header=0, sheet_name=0)
old_decl = pd.read_excel('declarations/declarations_old.xlsx', header=0, sheet_name=0)


def get_sql_data(table_name, column, data):
    cortag = []
    for i in data:
        MY_CURSOR.execute(f"select name_, year_, inn from {table_name} where {column}={i}")
        d = MY_CURSOR.fetchall()
        cortag.append(d)
    return cortag


def find_item_no_double_idx(df, col_name):
    df = df[col_name].drop_duplicates(keep='first')

    return df


def get_content_from_no_double(df, type='str'):
    df = list(df)

    content = []
    for i in range(len(df)):
        cort = df[i]

        if type == 'str':
            cort = str(cort)
        elif type == 'int':
            cort = int(cort)
        elif type == 'float':
            cort = float(cort)

        content.append(cort)

    return content


def find_item(df, col, type='str'):
    content = []
    for i in range(len(df)):
        cort = str(df.iloc[i, col]).strip()
        if type == 'str':
            cort = str(cort)
        elif type == 'int':
            cort = int(cort)
        elif type == 'float':
            cort = float(cort)
        content.append(cort)
    return content


def find_new(new, old):
    g = []
    for element in new:
        if element in old:
            # while element in new:
            new.remove(element)
        else:
            g.append(element)
    return g


old_inn_idx = find_item_no_double_idx(old_decl, 'inn')

old_inn = get_content_from_no_double(old_inn_idx, 'int')  # get old inn from declarations_old

new_inn = find_new(inns, old_inn)

print(new_inn)
print('len(new_inn)... ', len(new_inn))

f = get_sql_data('person', 'inn', new_inn)

for i in f:
    print(i)
