###%%writefile final_project.pu
#!/usr/bin/python3

import os
import shutil
from datetime import datetime

import pandas as pd
import psycopg2

FILES_PREFIX = '/home/de11an/kart/project/'
PROCESSED_FILES_DIR = '/home/de11an/kart/project/archive/'

# IMPROVEMENT: пароли в отдельном файлике. Пример есть
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
# IMPROVEMENT: если первым более новый - сбой. Need sort files (50:20 dtd 21.01.2023)
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
for f in os.listdir(FILES_PREFIX): # IMPROVEMENT: если первым более новый - сбой. Need sort files (50:20 dtd 21.01.2023)
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

# IMPROVEMENT: инкрементальную загрузку сделать:
# -в cursor_edu дату из меты
# draft: ("""select * from info.clients where date > %s """, cursor_edu)

cursor_b.execute( """select * from info.clients c limit 50;""" ) #fix: delete limit 50
records = cursor_b.fetchall() 

names = [ x[0] for x in cursor_b.description ]
df = pd.DataFrame( records, columns = names ) 

cursor.executemany( """INSERT INTO de11an.kart_stg_clients (client_id, last_name, first_name, 
    patronymic, date_of_birth, passport_num, passport_valid_to, phone, create_dt, update_dt) 
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", df.values.tolist())

conn.commit()

# • Загрузите таблицу bank.accounts в стейджинг. Используйте код из предыдущего пункта.

cursor_b.execute( """select * from info.accounts a limit 50;""" ) #fix: delete limit 50

cursor.executemany( """INSERT INTO de11an.kart_stg_accounts 
(account_num, valid_to, client, create_dt, update_dt) 
    VALUES (%s,%s,%s,%s,%s)"""
    , cursor_b.fetchall() )

conn.commit()

# • Загрузите таблицу bank.cards в стейджинг. Используйте код из предыдущего пункта.

cursor_b.execute( """select * from info.cards c2 limit 50;""" ) #fix: delete limit 50

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
    insert into de11an.kart_dim_terminals_hist( 
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
    left join de11an.kart_dim_terminals_hist dim
    on stg.terminal_id = dim.terminal_id
        and dim.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
        and dim.deleted_flg = FALSE
    where dim.terminal_id is null;
""")
# --Обновление в приемнике "обновлений" на источнике (формат SCD2).
cursor.execute( """
    update de11an.kart_dim_terminals_hist
    set
        effective_to = tmp.update_dt  - interval '1 second'
    from (
        select 
            stg.terminal_id, 
            stg.update_dt
        from de11an.kart_stg_terminals stg
        inner join de11an.kart_dim_terminals_hist dim
            on stg.terminal_id = dim.terminal_id
                and dim.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
                and dim.deleted_flg = FALSE
        where 1=0
            or stg.terminal_type <> dim.terminal_type or ( stg.terminal_type is null and dim.terminal_type is not null ) or ( stg.terminal_type is not null and dim.terminal_type is null )
            or stg.terminal_city <> dim.terminal_city or ( stg.terminal_city is null and dim.terminal_city is not null ) or ( stg.terminal_city is not null and dim.terminal_city is null )
            or stg.terminal_address <> dim.terminal_address or ( stg.terminal_address is null and dim.terminal_address is not null ) or ( stg.terminal_address is not null and dim.terminal_address is null )		
    ) tmp
    where de11an.kart_dim_terminals_hist.terminal_id = tmp.terminal_id; 
""")
cursor.execute( """
    insert into de11an.kart_dim_terminals_hist( 
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
    inner join de11an.kart_dim_terminals_hist dim
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
    insert into de11an.kart_dim_terminals_hist( 
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
    from de11an.kart_dim_terminals_hist dim
    where 1=1
        and dim.terminal_id in (
            select dim.terminal_id
            from de11an.kart_dim_terminals_hist dim
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
    update de11an.kart_dim_terminals_hist
    set 
        effective_to = now() - interval '1 second'
    where 1=1
        and terminal_id in (
            select dim.terminal_id
            from de11an.kart_dim_terminals_hist dim
            left join de11an.kart_stg_terminals_del stg
            on stg.terminal_id = dim.terminal_id
            where 1=1
                and stg.terminal_id is null
                and dim.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
                and dim.deleted_flg = FALSE
        )
        and de11an.kart_dim_terminals_hist.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
        and de11an.kart_dim_terminals_hist.deleted_flg = FALSE;
""")
# • Загрузите данные из стейджинга в целевую таблицу xxxx_dwh_dim_cards. Используйте код из предыдущего пункта.
# • Загрузите данные из стейджинга в целевую таблицу xxxx_dwh_dim_cards. Используйте код из предыдущего пункта.
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

# • Загрузите данные из стейджинга в целевую таблицу xxxx_dwh_dim_clients. Используйте код из предыдущего пункта.


# • Загрузите данные из стейджинга в целевую таблицу xxxx_dwh_fact_passport_blacklist. 
# Напоминаем, что в фактовые таблицы данные перекладываются «простым инсертом», то есть необходимо выполнить один INSERT INTO … SELECT …


# • Загрузите данные из стейджинга в целевую таблицу xxxx_dwh_fact_transactions. 
# Напоминаем, что в фактовые таблицы данные перекладываются «простым инсертом», то есть необходимо выполнить один INSERT INTO … SELECT …



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


# @**Кодогенератор**
# вход: значения полей списком 	
# прим: 'terminal_type, terminal_city, terminal_address'
# column = str(input())
# column_name = []
# column_name = (column.split(', '))
# for i in column_name:
#    print('or stg.' + i + ' <> dim.' + i + ' or ( stg.' + i + ' is null and dim.' + i + ' is not null ) or ( stg.' + i + ' is not null and dim.' + i + ' is null )')
