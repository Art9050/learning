#!/usr/bin/python3

import pandas as pd

# файлик в DF
---medicine_df = pd.read_excel( 'medicine.xlsx', sheet_name='easy', header=0, index_col=None )

medicine_df = pd.read_excel( 'medicine.xlsx', sheet_name='hard', header=0, index_col=None )

import psycopg2

conn = psycopg2.connect(database = "edu",
                        host = "de-edu-db.chronosavant.ru",
                        user = "de11an",
                        password = "peregrintook",
                        port = "5432")

conn.autocommit = False

cursor = conn.cursor()

# создать табличку в базе через курсор 
cursor.execute("CREATE TABLE de11an.kart_medicine (person_id integer, test varchar(20),  result varchar(20))")  
conn.commit()

# наполнение таблички из DF
cursor.executemany( """INSERT INTO de11an.kart_medicine (person_id, test, result) VALUES (%s, %s, %s)""", medicine_df.values.tolist() )
conn.commit()

# Сохраните таблицу с расшифрованными значениями и результатами анализа в таблице de11an.xxxx_med_results в базе данных.
# Создание таблички
cursor.execute("CREATE TABLE de11an.kart_med_results (phone varchar(20), person_name varchar(50), test_name varchar(20), result_fg varchar(20))")  
conn.commit()

# Наполнение таблички
cursor.execute( 
"""INSERT INTO de11an.kart_med_results( phone, person_name, test_name, result_fg)(
WITH med_results_all as(
	select 
		person_id,
		test,
		man.name as test_name,	
		is_simple,
		result,
		min_value,
		max_value,
		case
			when cast(result as float) between cast(min_value as float) and cast(max_value as float) then 'Норма'
			when cast(result as float) > cast(max_value as float) then 'Повышен'
			when cast(result as float) < cast(min_value as float) then 'Понижен'
			else 'Неверная запись'
		end as result_fg,
		mn.name as person_name,
		phone
	from de11an.kart_medicine md
	LEFT JOIN de.med_an_name man
		on test = man.id
	left join de.med_name mn
		on person_id = mn.id
	Where is_simple = 'N'
	union all
	select 
		person_id,
		test,
		man.name as test_name,
		is_simple,
		result,
		min_value,
		max_value,
		case
			when trim(lower(result)) ~'^[(отр)*]|-' then 'Отрицательный'
			when trim(lower(result)) ~'^[(пол)*]|\+' then 'Положительный'
			else 'Неверная запись'
		end as result_fg,
		mn.name as person_name,
		phone
	from de11an.kart_medicine md
	LEFT JOIN de.med_an_name man
		on test = man.id
	left join de.med_name mn
		on person_id = mn.id
	Where is_simple = 'Y'
)
SELECT 
	phone, 
	person_name, 
	test_name, 
	result_fg 
FROM med_results_all
where person_id in (
	select 
		person_id
	from med_results_all
	WHERE result_fg in  ('Повышен', 'Понижен', 'Положительный')
	group by person_id
	having count(result_fg) > 1
	Order BY person_id
	)
	and result_fg in  ('Повышен', 'Понижен', 'Положительный'))
""" )
conn.commit()

# наполнить df 
cursor.execute("""SELECT * FROM de11an.kart_med_results""")

# создать df # наполнить df
names = [ x[0] for x in cursor.description ]
df = pd.DataFrame( records, columns = names )

# Сохранить Сохранить в xlsx
df.to_excel( 'kart_med_results.xlsx', sheet_name='sheet1', header=True, index=False )

# закрыть курсор
cursor.close()
conn.close()

# закрыть python3
quit()
