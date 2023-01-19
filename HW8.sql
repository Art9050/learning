--Возьмите в работу скрипт инкрементальной загрузки SCD1 из вебинара 15.
--Преобразуйте его для работы с SCD2.

----------------------------------------------------------------------------
-- Подготовка данных

--truncate table de11an.kart_source;
--truncate table de11an.kart_stg;
--truncate table de11an.kart_target;
--truncate table de11an.kart_meta;

-- добавление
insert into de11an.kart_source ( id, val, update_dt ) values ( 1, 'A', now() );
insert into de11an.kart_source ( id, val, update_dt ) values ( 2, 'B', now() );
insert into de11an.kart_source ( id, val, update_dt ) values ( 3, 'C', now() );
insert into de11an.kart_source ( id, val, update_dt ) values ( 4, 'D', now() );

-- обновление
update de11an.kart_source set val = 'X', update_dt = now() where id = 3;

-- удаление
delete from de11an.kart_source where id = 2;

-- выборки
select * from de11an.kart_source;
select * from de11an.kart_stg;
select * from de11an.kart_target;
select * from de11an.kart_meta;

-- Подготовка таблиц

create table de11an.kart_source( 
	id integer,
	val varchar(50),
	update_dt timestamp(0)
);

create table de11an.kart_stg( 
	id integer,
	val varchar(50),
	update_dt timestamp(0)
);


create table de11an.kart_target ( 
 id integer, 
 val varchar(50), 
 start_dt timestamp(0), 
 end_dt timestamp(0), 
 del_fg char(1) DEFAULT 0
);

--drop table de11an.kart_target;

create table de11an.kart_meta(
    schema_name varchar(30),
    table_name varchar(30),
    max_update_dt timestamp(0)
);

create table de11an.kart_stg_del( 
	id integer
);

insert into de11an.kart_meta( schema_name, table_name, max_update_dt )
values( 'de11an','kart_SOURCE', to_timestamp('1900-01-01','YYYY-MM-DD') );

----------------------------------------------------------------------------
-- Инкрементальная загрузка SCD1

-- 1. Очистка стейджинговых таблиц

delete from de11an.kart_stg;
delete from de11an.kart_stg_del;

-- 2. Захват данных из источника (измененных с момента последней загрузки) в стейджинг

insert into de11an.kart_stg( id, val, update_dt )
select id, val, update_dt from de11an.kart_source
where update_dt > ( select max_update_dt from de11an.kart_meta where schema_name='de11an' and table_name='kart_SOURCE' );

-- 3. Захват в стейджинг ключей из источника полным срезом для вычисления удалений.

insert into de11an.kart_stg_del( id )
select id from de11an.kart_source;

-- 4. Загрузка в приемник "вставок" на источнике (формат SCD1).
--сравнивает по ID если нет - вставлять, de11an.kart_target.end_dt - ставить макс_дату
insert into de11an.kart_target( id, val, start_dt, end_dt )
select 
	stg.id, 
	stg.val, 
	stg.update_dt, 
	to_timestamp('3000-12-31','YYYY-MM-DD')
from de11an.kart_stg stg
left join de11an.kart_target tgt
on stg.id = tgt.id
where tgt.id is null;


-- 5. Обновление в приемнике "обновлений" на источнике (формат SCD1).
--5-1. Обновление записей удовлетворяющих условия. 
update de11an.kart_target
set 
	end_dt = tmp.update_dt - interval  '1 sec' 
from (
	select 
		stg.id, 
		stg.val, 
		stg.update_dt, 
		to_timestamp('3000-12-31','YYYY-MM-DD')
	from de11an.kart_stg stg
	inner join de11an.kart_target tgt
	on stg.id = tgt.id
	where 1=1
--		and stg.id = tgt.id 
		and stg.update_dt > (select 
								max_update_dt 
							from de11an.kart_meta
							)
		and to_timestamp('1900-01-01','YYYY-MM-DD') <> (select max_update_dt from de11an.kart_meta) --отсекает первую итерацию
) tmp
where kart_target.id = tmp.id; --ошибка в логике при первой итерации - у меня все данный больше меты

--5-2. Вставляем обновленную запись новой строкой c макс_датой
insert into de11an.kart_target( id, val, start_dt, end_dt ) (
	select 
		stg.id, 
		stg.val, 
		stg.update_dt, 
		to_timestamp('3000-12-31','YYYY-MM-DD')
	from de11an.kart_stg stg
	inner join de11an.kart_target tgt
	on stg.id = tgt.id
	where 1=1
--		and stg.id = tgt.id 
		and stg.update_dt > (select 
								max_update_dt 
							from de11an.kart_meta
							)
		and to_timestamp('1900-01-01','YYYY-MM-DD') <> (select max_update_dt from de11an.kart_meta) --отсекает первую итерацию
);  --дублирует данные!


--truncate table de11an.kart_target;
select * from de11an.kart_target; ----------------------------------------------------------------------------------------------------------------------------------


-- 6. Удаление в приемнике удаленных в источнике записей (формат SCD1).
--вар1 изменение fg при отсутствующем ID и "закрытие" записи
update de11an.kart_target 
set 
	del_fg = 1,
	end_dt = coalesce( (select max( update_dt ) from de11an.kart_stg), now())  - interval  '1 sec' 
where 1=1
	and kart_target.id not in (select id from de11an.kart_stg_del stg)
	and end_dt = to_timestamp('3000-12-31','YYYY-MM-DD');

--вар2 - добавление строки
--"закрытие" записи
update de11an.kart_target 
set 
	--del_fg = 1
	end_dt =  coalesce( (select max( update_dt ) from de11an.kart_stg), now())  - interval  '1 sec'  
where 1=1
	and kart_target.id not in (select id from de11an.kart_stg_del stg)
	and end_dt = to_timestamp('3000-12-31','YYYY-MM-DD');

--добавление строки с закрытым fg:
insert into de11an.kart_target( id, val, start_dt, end_dt, del_fg) (
	select 
		id, 
		val, 
		start_dt, 
		to_timestamp('3000-12-31','YYYY-MM-DD'),
		1 
	FROM de11an.kart_target tgt
	where 1=1
		and tgt.id not in (select id from de11an.kart_stg_del stg)
		and end_dt =  (select max( end_dt ) 
						from de11an.kart_target
						where end_dt <> to_timestamp('3000-12-31','YYYY-MM-DD') 
						)
	);

-- 7. Обновление метаданных.

update de11an.kart_meta
set max_update_dt = coalesce( (select max( update_dt ) from de11an.kart_stg ), ( select max_update_dt from de11an.kart_meta where schema_name='de11an' and table_name='kart_SOURCE' ) )
where schema_name='de11an' and table_name = 'kart_SOURCE';
