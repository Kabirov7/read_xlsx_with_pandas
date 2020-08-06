import config
import psycopg2

DB = psycopg2.connect(options=f'-c search_path={config.options}', database=config.database, user=config.user,
                      password=config.password, host=config.host, port=config.port)
MY_CURSOR = DB.cursor()

MY_CURSOR.execute("select * from person where name_='nan'")

nan_name = MY_CURSOR.fetchall()

print(nan_name)
print(len(nan_name))

# for nan in nan_name:
#     print(nan)
#     nan = list(nan)
#     MY_CURSOR.execute(f"select * from declarationss where inn={nan[1]}")
#     notnan = MY_CURSOR.fetchall()
#     print('nonan',notnan)
#     notnan = list(notnan)
#     name = notnan[0].replace('\xa0',' ').replace('\n',' ')
#     MY_CURSOR.execute(f"update person set name_='{name}' where inn={nan[1]}")
#     DB.commit()