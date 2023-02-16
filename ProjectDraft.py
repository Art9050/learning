#!/usr/bin/python3

import os
import shutil
from datetime import datetime

import pandas as pd
import psycopg2

from dotenv import load_dotenv

load_dotenv()

USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")
HOST = os.getenv("HOST")
PORT = os.getenv("PORT")
DB_NAME = os.getenv("DB_NAME")

USERNAME_B = os.getenv("USERNAME_B")
PASSWORD_B = os.getenv("PASSWORD_B")
HOST_B = os.getenv("HOST_B")
PORT_B = os.getenv("PORT_B")
DB_NAME_B = os.getenv("DB_NAME_B")

FILES_PREFIX = '/home/de11an/kart/project/'
PROCESSED_FILES_DIR = '/home/de11an/kart/project/archive/'

conn_b = psycopg2.connect(
            database = DB_NAME_B,
            host =     HOST_B,
            user =     USERNAME_B,
            password = PASSWORD_B,
            port =     PORT_B)


conn = psycopg2.connect(
            database = DB_NAME,
            host = HOST,
            user = USERNAME,
            password = PASSWORD,
            port = PORT)

conn_b.autocommit = False
conn.autocommit = False

cursor_b = conn_b.cursor()
cursor = conn.cursor()
############################################################################################################################################
#------Clear stg------------------------
cursor.execute( """
    delete from de11an.kart_stg_transactions; 
    delete from de11an.kart_stg_terminals; 
    delete from de11an.kart_stg_terminals_del; 
    delete from de11an.kart_stg_blacklist; 
    delete from de11an.kart_stg_cards; 
    delete from de11an.kart_stg_cards_del; 
    delete from de11an.kart_stg_accounts;
    delete from de11an.kart_stg_accounts_del;
    delete from de11an.kart_stg_clients;
    delete from de11an.kart_stg_clients_del;
""" )
conn.commit()
############################################################################################################################################
#------INSERT stg------------------------
# Загрузите файл transactions_01032021.txt в стейджинг (используйте код из snippet_pg.py). 
# Для простейшего варианта решения допустимо использовать имя файла «хардкодом», то есть записать в код как есть.

cursor.execute("""
    select
        max_update_dt
    from de11an.kart_META_all
    where schema_name='de11an' and table_name='kart_stg_transactions'
""")
last_date = cursor.fetchone()[0] 
# IMPROVEMENT: если первым более новый - сбой
# TECHNICAL REQUIREMENT: Предполагается что в один день приходит по одному такому файлу. Желающие могут придумать, обосновать и 
# реализовать более технологичные и учитывающие сбои способы обработки (за это будет повышен балл).
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
if file == None:
    print('No such file or directory: ', FILES_PREFIX, 'transactions_')
    quit()
df = pd.read_csv(f'{FILES_PREFIX}/{file}', sep=';')
df['update_dt'] = file_dt.strftime('%Y-%m-%d')
df = df[['transaction_id', 'transaction_date', 'amount', 'card_num', 'oper_type', 'oper_result', 'terminal', 'update_dt']]

shutil.move(
    f'{FILES_PREFIX}/{file}',
    f'{PROCESSED_FILES_DIR}/{file}.backup'
)

# IMPROVEMENT: Преобразование типа можно вынести в DF. Не понял как. Есть пробелы? Замена ',' не помогла
# df['amount'].apply(lambda x: str(x).replace(",", ".")).astype('decimal')
# df.dtypes
# df = pd.read_csv(f'{FILES_PREFIX}/{file}', sep=';', , decimal=",")

cursor.executemany( """ INSERT INTO de11an.kart_stg_transactions (
                                trans_id,
                                trans_date,
                                amt, 
                                card_num, 
                                oper_type, 
                                oper_result, 
                                terminal, 
                                update_dt 
                            ) VALUES( %s, %s, %s, %s, %s, %s, %s, %s ) """, df.values.tolist() )

# Загрузите файл terminals_*.xlsx в стейджинг аналогично предыдущему пункту.

cursor.execute("""
    select
        max_update_dt
    from de11an.kart_META_all
    where schema_name='de11an' and table_name='kart_stg_terminals'
""")
last_date = cursor.fetchone()[0]

file = None
file_dt = None

for f in os.listdir(FILES_PREFIX):
    if not f.startswith('terminals_'):
        continue
    _, file_date = f.split('_')
    file_dt = datetime.strptime(file_date.replace('.xlsx', ''), '%d%m%Y')
    if file_dt > last_date:
        file = f
        break
if file == None:
    print('No such file or directory: ', FILES_PREFIX, 'terminals_')
    quit()
df = pd.read_excel((f'{FILES_PREFIX}/{file}'), sheet_name='terminals', header=0, index_col=None )
df['update_dt'] = file_dt.strftime('%Y-%m-%d')
df = df[['terminal_id', 'terminal_type', 'terminal_city', 'terminal_address', 'update_dt']]

shutil.move(
    f'{FILES_PREFIX}/{file}',
    f'{PROCESSED_FILES_DIR}/{file}.backup'
)

shutil.move(
    f'{FILES_PREFIX}/{file}',
    f'{PROCESSED_FILES_DIR}/{file}')

cursor.executemany( """ INSERT INTO de11an.kart_stg_terminals(
                                terminal_id,
                                terminal_type,
                                terminal_city,
                                terminal_address,
                                update_dt 
                            ) VALUES( %s, %s, %s, %s, %s ) """, df.values.tolist() )
conn.commit()
# • Загрузите файл passport_blacklist_01032021.xlsx в стейджинг аналогично предыдущему пункту.

# IMPROVEMENT: фильтр строк по дате в мете. Настроить инкрементальную загрузку

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
if file == None:
    print('No such file or directory: ', FILES_PREFIX, 'transactions_')
    quit()
df = pd.read_excel((f'{FILES_PREFIX}/{file}'), sheet_name='blacklist', header=0, index_col=None )
df['update_dt'] = file_dt.strftime('%Y-%m-%d')
df = df[['date', 'passport', 'update_dt']]

shutil.move(
    f'{FILES_PREFIX}/{file}',
    f'{PROCESSED_FILES_DIR}/{file}.backup'
)

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

# IMPROVEMENT: инкрементальную загрузку сделать:
# -в cursor_edu дату из меты
# draft: ("""select * from info.clients where date > %s """, cursor_edu)

cursor_b.execute( """select * from info.clients;""" ) 
records = cursor_b.fetchall() 

names = [ x[0] for x in cursor_b.description ]
df = pd.DataFrame( records, columns = names ) 

cursor.executemany( """INSERT INTO de11an.kart_stg_clients (client_id, last_name, first_name, 
    patronymic, date_of_birth, passport_num, passport_valid_to, phone, create_dt, update_dt) 
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", df.values.tolist())

conn.commit()

# • Загрузите таблицу bank.accounts в стейджинг. Используйте код из предыдущего пункта.

cursor_b.execute( """select * from info.accounts;""" )

cursor.executemany( """INSERT INTO de11an.kart_stg_accounts 
(account_num, valid_to, client, create_dt, update_dt) 
    VALUES (%s,%s,%s,%s,%s)"""
    , cursor_b.fetchall() )

conn.commit()

# • Загрузите таблицу bank.cards в стейджинг.

cursor_b.execute( """select * from info.cards;""" ) 

cursor.executemany( """INSERT INTO de11an.kart_stg_cards 
( card_num, account_num, create_dt, update_dt) VALUES (%s,%s,%s,%s)"""
    , cursor_b.fetchall() )

conn.commit()

############################################################################################################################################
#------INSERT stg del------------------------
cursor.execute( """
    insert into de11an.kart_stg_terminals_del( terminal_id )
        select terminal_id from de11an.kart_stg_terminals;
    insert into de11an.kart_stg_cards_del( card_num )
        select card_num from de11an.kart_stg_cards;
    insert into de11an.kart_stg_accounts_del( account_num )
        select account_num from de11an.kart_stg_accounts;
    insert into de11an.kart_stg_clients_del( client_id )
        select client_id from de11an.kart_stg_clients;
""")

conn.commit()
############################################################################################################################################
#------INSERT trgt------------------------
# IMPROVEMENT: следующие 4 сделать кодогенератором по аналогии с циклом:
# column = str(input())
# column_name = []
# column_name = (column.split(', '))
# for i in column_name:
#    print('or stg.' + i + ' <> dim.' + i + ' or ( stg.' + i + ' is null and dim.' + i + ' is not null ) or ( stg.' + i + ' is not null and dim.' + i + ' is null )')
# Вход: 1. список полей stg, 2. список полей dim, 3. имя stg, 4. имя  dim

# • Загрузите данные из стейджинга в целевую таблицу xxxx_dwh_dim_terminals (используйте код из SCD1_incremental_load.sql).

# --Загрузка в приемник "вставок" на источнике (формат SCD2).
cursor.execute( """
    insert into de11an.kart_dwh_dim_terminals_hist( 
        terminal_id, 
        terminal_type, 
        terminal_city, 
        terminal_address, 
        effective_from, 
        effective_to, 
        deleted_flg
        )
    select 
        stg.terminal_id, 
        stg.terminal_type, 
        stg.terminal_city, 
        stg.terminal_address, 
        stg.update_dt, 
        to_date( '9999-12-31', 'YYYY-MM-DD' ),
        FALSE
    from de11an.kart_stg_terminals stg
    left join de11an.kart_dwh_dim_terminals_hist dim
    on stg.terminal_id = dim.terminal_id
        and dim.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
        and dim.deleted_flg = FALSE
    where dim.terminal_id is null;
""")
# --Обновление в приемнике "обновлений" на источнике (формат SCD2).
cursor.execute( """
    update de11an.kart_dwh_dim_terminals_hist
    set
        effective_to = tmp.update_dt  - interval '1 second'
    from (
        select 
            stg.terminal_id, 
            stg.update_dt
        from de11an.kart_stg_terminals stg
        inner join de11an.kart_dwh_dim_terminals_hist dim
            on stg.terminal_id = dim.terminal_id
                and dim.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
                and dim.deleted_flg = FALSE
        where 1=0
            or stg.terminal_type <> dim.terminal_type or ( stg.terminal_type is null and dim.terminal_type is not null ) or ( stg.terminal_type is not null and dim.terminal_type is null )
            or stg.terminal_city <> dim.terminal_city or ( stg.terminal_city is null and dim.terminal_city is not null ) or ( stg.terminal_city is not null and dim.terminal_city is null )
            or stg.terminal_address <> dim.terminal_address or ( stg.terminal_address is null and dim.terminal_address is not null ) or ( stg.terminal_address is not null and dim.terminal_address is null )		
    ) tmp
    where de11an.kart_dwh_dim_terminals_hist.terminal_id = tmp.terminal_id; 
""")
cursor.execute( """
    insert into de11an.kart_dwh_dim_terminals_hist( 
        terminal_id, 
        terminal_type, 
        terminal_city, 
        terminal_address, 
        effective_from, 
        effective_to, 
        deleted_flg
        )
    select 
        stg.terminal_id, 
        stg.terminal_type, 
        stg.terminal_city, 
        stg.terminal_address, 
        stg.update_dt, 
        to_date( '9999-12-31', 'YYYY-MM-DD' ),
        FALSE
    from de11an.kart_stg_terminals stg
    inner join de11an.kart_dwh_dim_terminals_hist dim
    on stg.terminal_id = dim.terminal_id
        and dim.effective_to = stg.update_dt  - interval '1 second'
        and dim.deleted_flg = FALSE
    where 1=0
        or stg.terminal_type <> dim.terminal_type or ( stg.terminal_type is null and dim.terminal_type is not null ) or ( stg.terminal_type is not null and dim.terminal_type is null )
        or stg.terminal_city <> dim.terminal_city or ( stg.terminal_city is null and dim.terminal_city is not null ) or ( stg.terminal_city is not null and dim.terminal_city is null )
        or stg.terminal_address <> dim.terminal_address or ( stg.terminal_address is null and dim.terminal_address is not null ) or ( stg.terminal_address is not null and dim.terminal_address is null )	
    ;
""")
# -- Удаление в приемнике удаленных в источнике записей (формат SCD2).
cursor.execute( """
    insert into de11an.kart_dwh_dim_terminals_hist( 
        terminal_id, 
        terminal_type, 
        terminal_city, 
        terminal_address, 
        effective_from, 
        effective_to, 
        deleted_flg
        )
    select 
        dim.terminal_id, 
        dim.terminal_type, 
        dim.terminal_city, 
        dim.terminal_address, 
        now(),
        to_date( '9999-12-31', 'YYYY-MM-DD' ),	
        TRUE	
    from de11an.kart_dwh_dim_terminals_hist dim
    where 1=1
        and dim.terminal_id in (
            select dim.terminal_id
            from de11an.kart_dwh_dim_terminals_hist dim
            left join de11an.kart_stg_terminals_del stg
            on stg.terminal_id = dim.terminal_id
            where 1=1
                and stg.terminal_id is null
                and dim.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
                and dim.deleted_flg = FALSE
        )
        and dim.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
        and dim.deleted_flg = FALSE;	
""")
cursor.execute( """
    update de11an.kart_dwh_dim_terminals_hist
    set 
        effective_to = now() - interval '1 second'
    where 1=1
        and terminal_id in (
            select dim.terminal_id
            from de11an.kart_dwh_dim_terminals_hist dim
            left join de11an.kart_stg_terminals_del stg
            on stg.terminal_id = dim.terminal_id
            where 1=1
                and stg.terminal_id is null
                and dim.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
                and dim.deleted_flg = FALSE
        )
        and de11an.kart_dwh_dim_terminals_hist.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
        and de11an.kart_dwh_dim_terminals_hist.deleted_flg = FALSE;
""")
conn.commit()
# • Загрузите данные из стейджинга в целевую таблицу xxxx_dwh_dim_cards.

# --Загрузка в приемник "вставок" на источнике (формат SCD2).

# • Загрузите данные из стейджинга в целевую таблицу xxxx_dwh_dim_cards. 
# --Загрузка в приемник "вставок" на источнике (формат SCD2).
cursor.execute( """
	insert into de11an.kart_dwh_dim_cards_hist(
		card_num ,
		account_num ,
		effective_from ,
		effective_to ,
		deleted_flg 
		)
	select 
		stg.card_num ,
		stg.account_num ,
		stg.create_dt ,
		to_date( '9999-12-31', 'YYYY-MM-DD' ),
		FALSE
	from de11an.kart_stg_cards stg
	left join de11an.kart_dwh_dim_cards_hist dim
		on stg.card_num = dim.card_num
			and dim.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
			and dim.deleted_flg = FALSE
	where dim.card_num is null;
""")

# • Загрузите данные из стейджинга в целевую таблицу xxxx_dwh_dim_cards. Используйте код из предыдущего пункта.
# --Загрузка в приемник "вставок" на источнике (формат SCD2).
# fix: double?
cursor.execute( """
	insert into de11an.kart_dwh_dim_cards_hist(
		card_num ,
		account_num ,
		effective_from ,
		effective_to ,
		deleted_flg 
		)
	select 
		stg.card_num ,
		stg.account_num ,
		stg.create_dt ,
		to_date( '9999-12-31', 'YYYY-MM-DD' ),
		FALSE
	from de11an.kart_stg_cards stg
	left join de11an.kart_dwh_dim_cards_hist dim
		on stg.card_num = dim.card_num
			and dim.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
			and dim.deleted_flg = FALSE
	where dim.card_num is null;
""")
# --Обновление в приемнике "обновлений" на источнике (формат SCD2).
cursor.execute( """
	update de11an.kart_dwh_dim_cards_hist
	set
		effective_to = tmp.update_dt  - interval '1 second'
	from (
		select 
			stg.card_num ,
			stg.update_dt
		from de11an.kart_stg_cards stg
		inner join de11an.kart_dwh_dim_cards_hist dim
			on stg.card_num = dim.card_num
				and dim.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
				and dim.deleted_flg = FALSE
		where 1=0
			or stg.account_num <> dim.account_num or ( stg.account_num is null and dim.account_num is not null ) or ( stg.account_num is not null and dim.account_num is null )
	) tmp
	where de11an.kart_dwh_dim_cards_hist.card_num = tmp.card_num; 
""")
cursor.execute( """
	insert into de11an.kart_dwh_dim_cards_hist(
		card_num ,
		account_num ,
		effective_from ,
		effective_to ,
		deleted_flg 
		)
		select 
			stg.card_num ,
			stg.account_num ,
			stg.create_dt ,
			to_date( '9999-12-31', 'YYYY-MM-DD' ),
			FALSE
		from de11an.kart_stg_cards stg
		inner join de11an.kart_dwh_dim_cards_hist dim
			on stg.card_num = dim.card_num
			and dim.effective_to = stg.update_dt  - interval '1 second'
			and dim.deleted_flg = FALSE
		where 1=0
			or stg.account_num <> dim.account_num or ( stg.account_num is null and dim.account_num is not null ) or ( stg.account_num is not null and dim.account_num is null )
		;
""")
# -- Удаление в приемнике удаленных в источнике записей (формат SCD2).
cursor.execute( """
	insert into de11an.kart_dwh_dim_cards_hist(
		card_num ,
		account_num ,
		effective_from ,
		effective_to ,
		deleted_flg 
		)
		select 
			dim.card_num ,
			dim.account_num ,
			now(),
			to_date( '9999-12-31', 'YYYY-MM-DD' ),	
			TRUE	
		from de11an.kart_dwh_dim_cards_hist dim
		where 1=1
			and dim.card_num in (
				select dim.card_num
				from de11an.kart_dwh_dim_cards_hist dim
				left join de11an.kart_stg_cards_del stg
				on stg.card_num = dim.card_num
				where 1=1
					and stg.card_num is null
					and dim.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
					and dim.deleted_flg = FALSE
			)
			and dim.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
			and dim.deleted_flg = FALSE;
""")
cursor.execute( """
	update de11an.kart_dwh_dim_cards_hist
	set 
		effective_to = now() - interval '1 second'
	where 1=1
		and card_num in (
			select dim.card_num
			from de11an.kart_dwh_dim_cards_hist dim
			left join de11an.kart_stg_cards_del stg
			on stg.card_num = dim.card_num
			where 1=1
				and stg.card_num is null
				and dim.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
				and dim.deleted_flg = FALSE
		)
		and de11an.kart_dwh_dim_cards_hist.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
		and de11an.kart_dwh_dim_cards_hist.deleted_flg = FALSE;
""")
conn.commit()
# • Загрузите данные из стейджинга в целевую таблицу xxxx_dwh_dim_accounts. Используйте код из предыдущего пункта.
# --Загрузка в приемник "вставок" на источнике (формат SCD2).
cursor.execute( """
	insert into de11an.kart_dwh_dim_accounts_hist(
		account_num ,
		valid_to ,
		client ,
		effective_from ,
	    effective_to ,
	    deleted_flg 
		)
	select 
		stg.account_num ,
		stg.valid_to ,
		stg.client ,
		stg.create_dt ,
		to_date( '9999-12-31', 'YYYY-MM-DD' ),
		FALSE
	from de11an.kart_stg_accounts stg
	left join de11an.kart_dwh_dim_accounts_hist dim
		on stg.account_num = dim.account_num
			and dim.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
			and dim.deleted_flg = FALSE
	where dim.account_num is null;
""")
# --Обновление в приемнике "обновлений" на источнике (формат SCD2).
cursor.execute( """
	update de11an.kart_dwh_dim_accounts_hist
	set
		effective_to = tmp.update_dt  - interval '1 second'
	from (
		select 
			stg.account_num ,
			stg.update_dt
		from de11an.kart_stg_accounts stg
		inner join de11an.kart_dwh_dim_accounts_hist dim
			on stg.account_num = dim.account_num
				and dim.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
				and dim.deleted_flg = FALSE
		where 1=0
			or stg.valid_to <> dim.valid_to or ( stg.valid_to is null and dim.valid_to is not null ) or ( stg.valid_to is not null and dim.valid_to is null )
			or stg.client <> dim.client or ( stg.client is null and dim.client is not null ) or ( stg.client is not null and dim.client is null )
		) tmp
	where de11an.kart_dwh_dim_accounts_hist.account_num = tmp.account_num; 
""")
cursor.execute( """
	insert into de11an.kart_dwh_dim_accounts_hist(
		account_num ,
		valid_to ,
		client ,
		effective_from ,
	    effective_to ,
	    deleted_flg 
		)
		select 
			stg.account_num ,
			stg.valid_to ,
			stg.client ,
			stg.create_dt ,
			to_date( '9999-12-31', 'YYYY-MM-DD' ),
			FALSE
		from de11an.kart_stg_accounts stg
		inner join de11an.kart_dwh_dim_accounts_hist dim
			on stg.account_num = dim.account_num
			and dim.effective_to = stg.update_dt  - interval '1 second'
			and dim.deleted_flg = FALSE
		where 1=0
			or stg.valid_to <> dim.valid_to or ( stg.valid_to is null and dim.valid_to is not null ) or ( stg.valid_to is not null and dim.valid_to is null )
			or stg.client <> dim.client or ( stg.client is null and dim.client is not null ) or ( stg.client is not null and dim.client is null )
		;
""")	
# -- Удаление в приемнике удаленных в источнике записей (формат SCD2).
cursor.execute( """
insert into de11an.kart_dwh_dim_accounts_hist(
		account_num ,
		valid_to ,
		client ,
		effective_from ,
	    effective_to ,
	    deleted_flg 
		)
		select 
			dim.account_num ,
			dim.valid_to ,
			dim.client ,
			now(),
			to_date( '9999-12-31', 'YYYY-MM-DD' ),	
			TRUE	
		from de11an.kart_dwh_dim_accounts_hist dim
		where 1=1
			and dim.account_num in (
				select dim.account_num
				from de11an.kart_dwh_dim_accounts_hist dim
				left join de11an.kart_stg_accounts stg
				on stg.account_num = dim.account_num
				where 1=1
					and stg.account_num is null
					and dim.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
					and dim.deleted_flg = FALSE
			)
			and dim.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
			and dim.deleted_flg = FALSE;
""")
cursor.execute( """
	update de11an.kart_dwh_dim_accounts_hist
		set 
			effective_to = now() - interval '1 second'
		where 1=1
			and account_num in (
				select dim.account_num
				from de11an.kart_dwh_dim_accounts_hist dim
				left join de11an.kart_stg_accounts_del stg
				on stg.account_num = dim.account_num
				where 1=1
					and stg.account_num is null
					and dim.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
					and dim.deleted_flg = FALSE
			)
			and de11an.kart_dwh_dim_accounts_hist.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
			and de11an.kart_dwh_dim_accounts_hist.deleted_flg = FALSE;
""")
conn.commit()

# • Загрузите данные из стейджинга в целевую таблицу xxxx_dwh_dim_clients
# --Загрузка в приемник "вставок" на источнике (формат SCD2).
cursor.execute( """
	insert into  de11an.kart_dwh_dim_clients_hist(
		client_id,
		last_name,
		first_name,
		patronymic, 
		date_of_birth,
		passport_num,
		passport_valid_to,
		phone,
		effective_from ,
	    effective_to ,
	    deleted_flg 
	)
	select 
		stg.client_id ,
		stg.last_name ,
		stg.first_name ,
		stg.patronymic ,
		stg.date_of_birth ,
		stg.passport_num ,
		stg.passport_valid_to ,
		stg.phone ,
		stg.create_dt ,
		to_date( '9999-12-31', 'YYYY-MM-DD' ),
		FALSE
	from de11an.kart_stg_clients stg
	left join  de11an.kart_dwh_dim_clients_hist dim
		on stg.client_id = dim.client_id
			and dim.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
			and dim.deleted_flg = FALSE
	where dim.client_id is null;
""")
# --Обновление в приемнике "обновлений" на источнике (формат SCD2).
cursor.execute( """
	update de11an.kart_dwh_dim_clients_hist
	set
		effective_to = tmp.update_dt  - interval '1 second'
	from (
		select 
			stg.client_id ,
			stg.update_dt
		from de11an.kart_stg_clients stg
		inner join  de11an.kart_dwh_dim_clients_hist dim
			on stg.client_id = dim.client_id
				and dim.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
				and dim.deleted_flg = FALSE
		where 1=0
			or stg.last_name <> dim.last_name or ( stg.last_name is null and dim.last_name is not null ) or ( stg.last_name is not null and dim.last_name is null )
			or stg.first_name <> dim.first_name or ( stg.first_name is null and dim.first_name is not null ) or ( stg.first_name is not null and dim.first_name is null )
			or stg.patronymic <> dim.patronymic or ( stg.patronymic is null and dim.patronymic is not null ) or ( stg.patronymic is not null and dim.patronymic is null )
			or stg.date_of_birth <> dim.date_of_birth or ( stg.date_of_birth is null and dim.date_of_birth is not null ) or ( stg.date_of_birth is not null and dim.date_of_birth is null )
			or stg.passport_num <> dim.passport_num or ( stg.passport_num is null and dim.passport_num is not null ) or ( stg.passport_num is not null and dim.passport_num is null )
			or stg.passport_valid_to <> dim.passport_valid_to or ( stg.passport_valid_to is null and dim.passport_valid_to is not null ) or ( stg.passport_valid_to is not null and dim.passport_valid_to is null )
			or stg.phone <> dim.phone or ( stg.phone is null and dim.phone is not null ) or ( stg.phone is not null and dim.phone is null )
		) tmp
	where de11an.kart_dwh_dim_clients_hist.client_id = tmp.client_id; 
""")
cursor.execute( """
	insert into de11an.kart_dwh_dim_clients_hist(
			client_id,
			last_name,
			first_name,
			patronymic, 
			date_of_birth,
			passport_num,
			passport_valid_to,
			phone,
			effective_from ,
		    effective_to ,
		    deleted_flg 
		)
		select 
			stg.client_id ,
			stg.last_name ,
			stg.first_name ,
			stg.patronymic ,
			stg.date_of_birth ,
			stg.passport_num ,
			stg.passport_valid_to ,
			stg.phone ,
			stg.create_dt ,
			to_date( '9999-12-31', 'YYYY-MM-DD' ),
			FALSE
		from de11an.kart_stg_clients stg
		inner join de11an.kart_dwh_dim_clients_hist dim
			on stg.client_id = dim.client_id
			and dim.effective_to = stg.update_dt  - interval '1 second'
			and dim.deleted_flg = FALSE
		where 1=0
			or stg.last_name <> dim.last_name or ( stg.last_name is null and dim.last_name is not null ) or ( stg.last_name is not null and dim.last_name is null )
			or stg.first_name <> dim.first_name or ( stg.first_name is null and dim.first_name is not null ) or ( stg.first_name is not null and dim.first_name is null )
			or stg.patronymic <> dim.patronymic or ( stg.patronymic is null and dim.patronymic is not null ) or ( stg.patronymic is not null and dim.patronymic is null )
			or stg.date_of_birth <> dim.date_of_birth or ( stg.date_of_birth is null and dim.date_of_birth is not null ) or ( stg.date_of_birth is not null and dim.date_of_birth is null )
			or stg.passport_num <> dim.passport_num or ( stg.passport_num is null and dim.passport_num is not null ) or ( stg.passport_num is not null and dim.passport_num is null )
			or stg.passport_valid_to <> dim.passport_valid_to or ( stg.passport_valid_to is null and dim.passport_valid_to is not null ) or ( stg.passport_valid_to is not null and dim.passport_valid_to is null )
			or stg.phone <> dim.phone or ( stg.phone is null and dim.phone is not null ) or ( stg.phone is not null and dim.phone is null )
		;
""")
# -- Удаление в приемнике удаленных в источнике записей (формат SCD2).
cursor.execute( """	
	insert into de11an.kart_dwh_dim_clients_hist(
			client_id,
			last_name,
			first_name,
			patronymic, 
			date_of_birth,
			passport_num,
			passport_valid_to,
			phone,
			effective_from ,
		    effective_to ,
		    deleted_flg 
		)
		select 
			client_id,
			last_name,
			first_name,
			patronymic, 
			date_of_birth,
			passport_num,
			passport_valid_to,
			phone,
			now(),
			to_date( '9999-12-31', 'YYYY-MM-DD' ),	
			TRUE	
		from de11an.kart_dwh_dim_clients_hist dim
		where 1=1
			and dim.client_id in (
				select dim.client_id
				from de11an.kart_dwh_dim_clients_hist dim
				left join de11an.kart_stg_clients stg
				on stg.client_id = dim.client_id
				where 1=1
					and stg.client_id is null
					and dim.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
					and dim.deleted_flg = FALSE
			)
			and dim.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
			and dim.deleted_flg = FALSE;	
""")
cursor.execute( """		
	update de11an.kart_dwh_dim_clients_hist
		set 
			effective_to = now() - interval '1 second'
		where 1=1
			and client_id in (
				select dim.client_id
				from de11an.kart_dwh_dim_clients_hist dim
				left join de11an.kart_stg_clients_del stg
				on stg.client_id = dim.client_id
				where 1=1
					and stg.client_id is null
					and dim.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
					and dim.deleted_flg = FALSE
			)
			and de11an.kart_dwh_dim_clients_hist.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
			and de11an.kart_dwh_dim_clients_hist.deleted_flg = FALSE;		
""")

# • Загрузите данные из стейджинга в целевую таблицу xxxx_dwh_fact_passport_blacklist. 
# Напоминаем, что в фактовые таблицы данные перекладываются «простым инсертом», то есть необходимо выполнить один INSERT INTO … SELECT …
cursor.execute( """	
	insert into  de11an.kart_DWH_FACT_passport_blacklist (
		passport_num,
		entry_dt 
		)
	select 
		passport_num,
		entry_dt
	from de11an.kart_stg_blacklist;
""")

# • Загрузите данные из стейджинга в целевую таблицу xxxx_dwh_fact_transactions. 
# Напоминаем, что в фактовые таблицы данные перекладываются «простым инсертом», то есть необходимо выполнить один INSERT INTO … SELECT …
cursor.execute( """	
	insert into de11an.kart_dwh_fact_trasactions (
		trans_id,
		trans_date,
		card_num,
		oper_type,
		amt,
		oper_result,
		terminal,
		update_dt
	)
	select 
		trans_id,
		trans_date,
		card_num,
		oper_type,
		CAST ( REPLACE(amt, ',', '.') AS decimal ),
		oper_result,
		terminal,
		update_dt
	 from de11an.kart_stg_transactions;
""")

conn.commit()

# type1 Совершение операции при просроченном или заблокированном паспорте.
cursor.execute( """
	insert into de11an.kart_rep_fraud (
		select 
			a.trans_date as event_dt,
			passport_num as passport,
			fio,
			phone,
			1 as event_type,
			date(a.trans_date) as report_dt
		from(
			select distinct 
				tr.trans_id,
				tr.trans_date as trans_date,
				tr.card_num,
				last_name || ' ' || first_name || ' ' || patronymic as fio,
				cl.passport_num,
				passport_valid_to,
				phone,
				entry_dt as entry_dt
			from de11an.kart_dwh_fact_trasactions tr
			left join de11an.kart_dwh_dim_cards_hist card
				on trim(tr.card_num) = trim(card.card_num)
			left join de11an.kart_dwh_dim_accounts_hist acc
				on trim(card.account_num) = trim(acc.account_num)
			left join de11an.kart_dwh_dim_clients_hist cl
				on trim(acc.client) = trim(cl.client_id)
			left join de11an.kart_dwh_fact_passport_blacklist bl
				on trim(cl.passport_num) = trim(bl.passport_num)
			where 1=0
				or date(trans_date) > date(passport_valid_to)
				or date(trans_date) >= date(entry_dt) --- '>=' в день блокировки паспорта и далее
			) as a
        where date(a.trans_date) > (select max(max_update_dt) from de11an.kart_meta_all) -- фильтр - только записи текущего дня
	);
""")
conn.commit()

# type2 Напишите скрипт, соединяющий нужные таблицы для поиска операций, совершенных при недействующем договоре 

cursor.execute( """
	insert into de11an.kart_rep_fraud (
		select
			a.trans_date as event_dt,
			passport_num as passport,
			fio,
			phone,
			2 as event_type,
			date(a.trans_date) as report_dt
		from(
		select distinct 
			tr.trans_id,
			tr.trans_date as trans_date,
			tr.card_num,
			card.account_num,
			valid_to,
			passport_num,
			last_name || ' ' || first_name || ' ' || patronymic as fio,
			phone
		from de11an.kart_dwh_fact_trasactions tr
		left join de11an.kart_dwh_dim_cards_hist card
			on trim(tr.card_num) = trim(card.card_num)
		left join de11an.kart_dwh_dim_accounts_hist acc
			on trim(card.account_num) = trim(acc.account_num)
		left join de11an.kart_dwh_dim_clients_hist cl
			on acc.client = cl.client_id
		where date(trans_date) > valid_to	
		) as a
        where date(a.trans_date) > (select max(max_update_dt) from de11an.kart_meta_all) -- фильтр - только записи текущего дня
	);
""")
conn.commit()

# type3 Совершение операций в разных городах в течение одного часа.
cursor.execute( """
	insert into de11an.kart_rep_fraud (
		with diferent_city as(
			select
				tr.trans_id,
				tr.trans_date,
				LAG(tr.trans_date) OVER(PARTITION BY tr.card_num ORDER BY tr.trans_date) AS lag_trans_date,
				tr.card_num,
				terminal,
				terminal_city,
				LAG(terminal_city) OVER(PARTITION BY tr.card_num ORDER BY tr.trans_date) AS lag_city,
				last_name || ' ' || first_name || ' ' || patronymic as fio,
				cl.passport_num,
				phone
			from de11an.kart_dwh_fact_trasactions tr
			left join de11an.kart_dwh_dim_cards_hist card
				on trim(tr.card_num) = trim(card.card_num)
			left join de11an.kart_dwh_dim_accounts_hist acc
				on trim(card.account_num) = trim(acc.account_num)
			left join de11an.kart_dwh_dim_clients_hist cl
				on trim(acc.client) = trim(cl.client_id)
			left join de11an.kart_dwh_dim_terminals_hist ter
				on tr.terminal = terminal_id
		)
		select 
			trans_date as event_dt,
			passport_num as passport,
			fio,
			phone,
			3 as event_type,
			date(trans_date) as report_dt
		from diferent_city
		where 1=1
			and lag_trans_date is not null
			and terminal_city <> lag_city
			and (trans_date - lag_trans_date) < time '01:00'
			and date(trans_date) > (select date(max(max_update_dt)) from de11an.kart_meta_all); -- фильтр - только записи текущего дня
	);
""")
conn.commit()

# type4 Попытка подбора суммы. 
# В течение 20 минут проходит более 3х операций со следующим шаблоном – каждая последующая меньше предыдущей, при этом 
# отклонены все кроме последней. Последняя операция (успешная) в такой цепочке считается мошеннической.

cursor.execute( """
	insert into de11an.kart_rep_fraud (
		select 
			tr.trans_date as event_dt,
			cl.passport_num as passport,
			last_name || ' ' || first_name || ' ' || patronymic as fio,
			phone,
			4 as event_type,
			date(tr.trans_date) as report_dt
		from(
			select 
				trans_id,
				LAG(trans_date, 3) OVER(PARTITION BY card_num ORDER BY trans_date) AS lag3_trans_date,	
				trans_date,
				card_num,
				LAG(amt, 3) OVER(PARTITION BY card_num ORDER BY trans_date) AS lag3_amt,
				LAG(amt, 2) OVER(PARTITION BY card_num ORDER BY trans_date) AS lag2_amt,
				LAG(amt) OVER(PARTITION BY card_num ORDER BY trans_date) AS lag1_amt,
				amt,
				LAG(oper_result, 3) OVER(PARTITION BY card_num ORDER BY trans_date) AS lag3_oper_result,
				LAG(oper_result, 2) OVER(PARTITION BY card_num ORDER BY trans_date) AS lag2_oper_result,
				LAG(oper_result) OVER(PARTITION BY card_num ORDER BY trans_date) AS lag1_oper_result,
				oper_result	
			from de11an.kart_dwh_fact_trasactions
		) as tr
		left join de11an.kart_dwh_dim_cards_hist card
			on trim(tr.card_num) = trim(card.card_num)
		left join de11an.kart_dwh_dim_accounts_hist acc
			on trim(card.account_num) = trim(acc.account_num)
		left join de11an.kart_dwh_dim_clients_hist cl
			on trim(acc.client) = trim(cl.client_id)
		Where 1=1
			and oper_result = 'SUCCESS' ----более 3х операций, отклонены все кроме последней
			and lag1_oper_result = 'REJECT' 
			and lag2_oper_result = 'REJECT'
			and lag3_oper_result = 'REJECT'
			and amt < lag1_amt and lag1_amt < lag2_amt and lag2_amt < lag3_amt --каждая последующая меньше предыдущей
			and (trans_date - lag3_trans_date) <= time '00:20' --В течение 20 минут
			and date(trans_date) > (select max(max_update_dt) from de11an.kart_meta_all)  -- фильтр - только записи текущего дня
		);
""")
conn.commit()

########################################################################################################
#-- Обновление метаданных.
cursor.execute( """	
	update de11an.kart_meta_all
	set max_update_dt = 
	coalesce(
	    ( select max( update_dt ) from de11an.kart_stg_cards ),
	    ( select max_update_dt from de11an.kart_meta_all
	      where schema_name='de11an' and table_name='kart_stg_cards' )   
	)
	where schema_name='de11an' and table_name = 'kart_stg_cards';
	
	update de11an.kart_meta_all
	set max_update_dt = 
	coalesce(
	    ( select max( update_dt ) from de11an.kart_stg_terminals ),
	    ( select max_update_dt from de11an.kart_meta_all
	      where schema_name='de11an' and table_name='kart_stg_terminals' )   
	)
	where schema_name='de11an' and table_name = 'kart_stg_terminals';
	
	update de11an.kart_meta_all
	set max_update_dt = 
	coalesce(
	    ( select max( update_dt ) from de11an.kart_stg_transactions ),
	    ( select max_update_dt from de11an.kart_meta_all
	      where schema_name='de11an' and table_name='kart_stg_transactions' )   
	)
	where schema_name='de11an' and table_name = 'kart_stg_transactions';
	
	update de11an.kart_meta_all
	set max_update_dt = 
	coalesce(
	    ( select max( update_dt ) from de11an.kart_stg_accounts ),
	    ( select max_update_dt from de11an.kart_meta_all
	      where schema_name='de11an' and table_name='kart_stg_accounts' )   
	)
	where schema_name='de11an' and table_name = 'kart_stg_accounts';
	
	update de11an.kart_meta_all
	set max_update_dt = 
	coalesce(
	    ( select max( update_dt ) from de11an.kart_stg_clients ),
	    ( select max_update_dt from de11an.kart_meta_all
	      where schema_name='de11an' and table_name='kart_stg_clients' )   
	)
	where schema_name='de11an' and table_name = 'kart_stg_clients';
	
	update de11an.kart_meta_all
	set max_update_dt = 
	coalesce(
	    ( select max( update_dt ) from de11an.kart_stg_blacklist ),
	    ( select max_update_dt from de11an.kart_meta_all
	      where schema_name='de11an' and table_name='kart_stg_kart_stg_blacklistclients' )   
	)
	where schema_name='de11an' and table_name = 'kart_stg_blacklist';
""")

conn_b.commit()
conn.commit()
cursor_b.close()
cursor.close()
conn_b.close()
conn.close()
# quit()

# @**Кодогенератор**
# вход: значения полей списком 	
# прим: 'terminal_type, terminal_city, terminal_address'
# column = str(input())
# column_name = []
# column_name = (column.split(', '))
# for i in column_name:
#    print('or stg.' + i + ' <> dim.' + i + ' or ( stg.' + i + ' is null and dim.' + i + ' is not null ) or ( stg.' + i + ' is not null and dim.' + i + ' is null )')
