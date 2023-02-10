###%%writefile final_project.pu
#!/usr/bin/python3

import os
import shutil
from datetime import datetime

import pandas as pd
import psycopg2

FILES_PREFIX = '/home/de11an/kart/project/'
PROCESSED_FILES_DIR = '/home/de11an/kart/project/archive/'

# improvement: пароли в отдельном файлике. Пример у тебя есть
conn_b = psycopg2.connect(database = "bank",
            host =     "de-edu-db.chronosavant.ru",
            user =     "bank_etl",
            password = "bank_etl_password",
            port =     5432)


conn = psycopg2.connect(database = "edu",
            host = "de-edu-db.chronosavant.ru",
            user = "de11an",
            password = "peregrintook",
            port = "5432")

conn_b.autocommit = False
conn.autocommit = False

cursor_b = conn_b.cursor()
cursor = conn.cursor()

#------Очистка stg------------------------
# improvement: загнать в список и сделать прим: cars = []  cur.executemany("INSERT INTO cars VALUES(NULL, ?, ?)", cars)
# cursor_e.execute( """delete from de11an.kart_stg_transactions""" )
# conn_e.commit() 
# cursor_e.execute( """delete from de11an.kart_stg_terminals""" ) 
# conn_e.commit()
# cursor_e.execute( """delete from de11an.kart_stg_terminals_del""" ) 
# conn_e.commit()
# cursor_e.execute( """delete from de11an.kart_stg_blacklist""" ) 
# conn_e.commit()
# cursor_e.execute( """delete from de11an.kart_stg_cards""" ) 
# conn_e.commit()
# cursor_e.execute( """delete from de11an.kart_stg_cards_dl""" ) 
# conn_e.commit()
# cursor_e.execute( """delete from de11an.kart_stg_accounts""" )
# conn_e.commit()
# cursor_e.execute( """delete from de11an.kart_stg_accounts_dl""" )
# conn_e.commit()
# cursor_e.execute( """delete from de11an.kart_stg_clients""" )
# conn_e.commit()
# cursor_e.execute( """delete from de11an.kart_stg_clients_dl""" )
# conn_e.commit()

cursor.execute( """
    delete from de11an.kart_stg_transactions; 
    delete from de11an.kart_stg_terminals; 
    delete from de11an.kart_stg_terminals_del; 
    delete from de11an.kart_stg_blacklist; 
    delete from de11an.kart_stg_cards; 
    delete from de11an.kart_stg_cards_dl; 
    delete from de11an.kart_stg_accounts;
    delete from de11an.kart_stg_accounts_dl;
    delete from de11an.kart_stg_clients;
    delete from de11an.kart_stg_clients_dl;
""" )
conn.commit()

############################################################################################################################################
# Загрузите файл transactions_01032021.txt в стейджинг (используйте код из snippet_pg.py). 
# Для простейшего варианта решения допустимо использовать имя файла «хардкодом», то есть записать в код как есть.

cursor.execute("""
    select
        max_update_dt
    from de11an.kart_META_all
    where schema_name='de11an' and table_name='kart_stg_transactions'
""")
last_date = cursor.fetchone()[0] 
# improvement: если первым более новый - сбой. Need sort files (50:20 dtd 21.01.2023)
file = None
file_dt = None
for f in os.listdir(FILES_PREFIX): 
    if not f.startswith('transactions_'):
        continue
    _, file_date = f.split('_')
    file_dt = datetime.strptime(file_date.replace('.txt', ''), '%d%m%Y')
    if file_dt > last_date:
        file = f
        break
df = pd.read_csv(f'{FILES_PREFIX}/{file}')
df['update_dt'] = file_dt.strftime('%Y-%m-%d')
df = df[['trans_id', 'trans_date', 'card_num', 'open_type', 'amt', 'open_result', 'terminal', 'update_dt']]

cursor.executemany( """ INSERT INTO de11an.kart_stg_terminals(
                                terminal_id,
                                terminal_type,
                                terminal_city,
                                terminal_address,
                                update_dt 
                            ) VALUES( %s, %s, %s, %s, %s ) """, df.values.tolist() )
cursor.executemany( """ INSERT INTO de11an.kart_stg_terminals_del(
                                terminal_id
                            ) VALUES( %s ) """, map(lambda x: [x], df['terminal_id'].values.tolist()) )

# Загрузите файл terminals_01032021.xlsx в стейджинг аналогично предыдущему пункту.

cursor.execute("""
    select
        max_update_dt
    from de11an.kart_META_all
    where schema_name='de11an' and table_name='kart_stg_terminals'
""")
last_date = cursor.fetchone()[0]

file = None
file_dt = None
for f in os.listdir(FILES_PREFIX): # improvement: если первым более новый - сбой. Need sort files (50:20 dtd 21.01.2023)
    if not f.startswith('terminals_'):
        continue
    _, file_date = f.split('_')
    file_dt = datetime.strptime(file_date.replace('.xlsx', ''), '%d%m%Y')
    if file_dt > last_date:
        file = f
        break
df = pd.read_excel((f'{FILES_PREFIX}/{file}'), sheet_name='terminals', header=0, index_col=None )
df['update_dt'] = file_dt.strftime('%Y-%m-%d')
df = df[['terminal_id', 'terminal_type', 'terminal_city', 'terminal_address', 'update_dt']]
cursor.executemany( """ INSERT INTO de11an.kart_stg_terminals(
                                terminal_id,
                                terminal_type,
                                terminal_city,
                                terminal_address,
                                update_dt 
                            ) VALUES( %s, %s, %s, %s, %s ) """, df.values.tolist() )
cursor.executemany( """ INSERT INTO de11an.kart_stg_terminals_del(
                                terminal_id
                            ) VALUES( %s ) """, map(lambda x: [x], df['terminal_id'].values.tolist()) )

# • Загрузите файл passport_blacklist_01032021.xlsx в стейджинг аналогично предыдущему пункту.
###################################################################################################
# improvement: фильтр строк по дате в мете. Настроить инкрементальную загрузку

cursor.execute("""
    select
        max_update_dt
    from de11an.kart_META_all
    where schema_name='de11an' and table_name='kart_stg_blacklist'
""")
last_date = cursor.fetchone()[0]

file = None
file_dt = None
for f in os.listdir(FILES_PREFIX): 
    if not f.startswith('passport_blacklist_'):
        continue
    _, _, file_date = f.split('_')
    file_dt = datetime.strptime(file_date.replace('.xlsx', ''), '%d%m%Y')
    if file_dt > last_date:
        file = f
        break
df = pd.read_excel((f'{FILES_PREFIX}/{file}'), sheet_name='blacklist', header=0, index_col=None )
df['update_dt'] = file_dt.strftime('%Y-%m-%d')
df = df[['date', 'passport', 'update_dt']]

# df['parsed_date'] = df[0].apply(lambda x: datetime.strptime(x, '%d.%m.%Y'))
# не понимаю почему сбой. Вероятно не может обработать названия столбцов. Как исправить?
# mask = data['parsed_date'] > file_dt

cursor.executemany( """ INSERT INTO kart_stg_blacklist(
                                entry_dt, 
                                passport_num,
                                update_dt
                            ) VALUES( %s, %s, %s ) """, df.values.tolist() ) # fix: отсутствует фильтр. дубли
conn.commit()
# • Загрузите таблицу bank.clients в стейджинг. Используйте следующий подход: 
# improvement: инкрементальную загрузку можно сделать:
# -в cursor_edu дату из меты
# draft: ("""select * from info.clients where date > %s """, cursor_edu)

cursor_b.execute( """select * from info.clients c limit 50;""" ) #fix: delete limit 50 # improvement: инкрементальную загрузку с датой из меты. с селекте учесть?
records = cursor_b.fetchall() 

names = [ x[0] for x in cursor_b.description ]
df = pd.DataFrame( records, columns = names ) 

cursor.executemany( """INSERT INTO de11an.kart_stg_clients (client_id, last_name, first_name, 
    patrinymic, date_of_birth, passport_num, passport_valid_to, phone, create_dt, update_dt) 
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", df.values.tolist())

conn.commit()

# • Загрузите таблицу bank.accounts в стейджинг. Используйте код из предыдущего пункта.

cursor_b.execute( """select * from info.accounts a limit 50;""" )

cursor.executemany( """INSERT INTO de11an.kart_stg_accounts 
(account_num, valid_to, client, create_dt, update_dt) 
    VALUES (%s,%s,%s,%s,%s)"""
    , cursor_b.fetchall() )

conn.commit()

# • Загрузите таблицу bank.cards в стейджинг. Используйте код из предыдущего пункта.

cursor_b.execute( """select * from info.cards c2 limit 50;""" )

cursor.executemany( """INSERT INTO de11an.kart_stg_cards 
( card_num, account_num, create_dt, update_dt) VALUES (%s,%s,%s,%s)"""
    , cursor_b.fetchall() )

conn.commit()

# • Загрузите данные из стейджинга в целевую таблицу xxxx_dwh_dim_terminals (используйте код из SCD1_incremental_load.sql).







# Для простейшего случая можно пропустить шаги инкрементальной загрузки, удаления и управления метаданными.
# • Загрузите данные из стейджинга в целевую таблицу xxxx_dwh_dim_cards. Используйте код из предыдущего пункта.
# • Загрузите данные из стейджинга в целевую таблицу xxxx_dwh_dim_accounts. Используйте код из предыдущего пункта.
# • Загрузите данные из стейджинга в целевую таблицу xxxx_dwh_dim_clients. Используйте код из предыдущего пункта.
# • Загрузите данные из стейджинга в целевую таблицу xxxx_dwh_fact_passport_blacklist. 
# Напоминаем, что в фактовые таблицы данные перекладываются «простым инсертом», то есть необходимо выполнить один INSERT INTO … SELECT …
# • Загрузите данные из стейджинга в целевую таблицу xxxx_dwh_fact_transactions. 
# Используйте код из предыдущего пункта.
# • Напишите скрипт, соединяющий нужные таблицы для поиска операций, совершенных при недействующем договоре 
# (это самый простой случай мошенничества). Отладьте ваш скрипт для одной даты в DBeaver, он должен выдавать результат. 
# В простейшем варианте допустимо использовать «хардкод» для задания дня отчета.













# conn_b.commit()
# conn.commit()
# cursor_b.close()
# cursor_e.close()
# conn_b.close()
# conn.close()
# quit()
