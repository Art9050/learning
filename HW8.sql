--Возьмите в работу скрипт инкрементальной загрузки SCD1 из вебинара 15.
--Преобразуйте его для работы с SCD2.

----------------------------------------------------------------------------
-- Подготовка данных

truncate table de11an.kart_source;
truncate table de11an.kart_stg;
truncate table de11an.kart_target;
truncate table de11an.kart_meta;

-- добавление
insert into de11an.kart_source ( id, val, update_dt ) values ( 1, 'A', now() );
insert into de11an.kart_source ( id, val, update_dt ) values ( 2, 'B', now() );
insert into de11an.kart_source ( id, val, update_dt ) values ( 3, 'C', now() );
insert into de11an.kart_source ( id, val, update_dt ) values ( 4, 'D', now() );

-- обновление
update de11an.kart_source set val = 'X', update_dt = now() where id = 3;

-- удаление
delete from de11an.kart_source where id = 3;

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

--create table de11an.kart_target (
--	id integer,
--	val varchar(50),
--	create_dt timestamp(0),
--	update_dt timestamp(0)
--);

create table de11an.kart_target1 ( 
 id integer, 
 val varchar(50), 
 create_dt timestamp(0), 
 update_dt timestamp(0), 
 del_fg char(1) DEFAULT 0
);

-----------------------------------------------------------------Т.к. SCD2, то уже не update_dt, а start_dt и end_dt, 

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

insert into de11an.kart_target( id, val, create_dt, update_dt )
select 
	stg.id, 
	stg.val, 
	stg.update_dt, 
	null 
from de11an.kart_stg stg
left join de11an.kart_target tgt
on stg.id = tgt.id
where tgt.id is null;

сравнивает по ID если нет вставлять, de11an.kart_target.update_dt - ставить макс_дату

-- 5. Обновление в приемнике "обновлений" на источнике (формат SCD1).

update de11an.kart_target
set 
	val = tmp.val,
	update_dt = tmp.update_dt
from (
	select 
		stg.id, 
		stg.val, 
		stg.update_dt, 
		null 
	from de11an.kart_stg stg
	inner join de11an.kart_target tgt
	on stg.id = tgt.id
	where stg.val <> tgt.val or ( stg.val is null and tgt.val is not null ) or ( stg.val is not null and tgt.val is null )
) tmp
where kart_target.id = tmp.id; 

1. Обновление записей удовлетворяющих условия. менять макс_дату на (дату_создания для следующего одновления - минимальный интервал) - есть в ДЗ7
2. Вставляем обновленную запись новой строкой


-- 6. Удаление в приемнике удаленных в источнике записей (формат SCD1).

delete from de11an.kart_target
where id in (
	select tgt.id
	from de11an.kart_target tgt
	left join de11an.kart_stg_del stg
	on stg.id = tgt.id
	where stg.id is null
);

переписать на обновление флага при отсутствующем ID

-- 7. Обновление метаданных.

update de11an.kart_meta
set max_update_dt = coalesce( (select max( update_dt ) from de11an.kart_stg ), ( select max_update_dt from de11an.kart_meta where schema_name='de11an' and table_name='kart_SOURCE' ) )
where schema_name='de11an' and table_name = 'kart_SOURCE';

-- 8. Фиксация транзакции.

commit;
