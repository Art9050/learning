
--Создайте таблицу DE11AN.XXXX_SALARY_HIST, где XXXX - ваш идентификатор. 
--
--В таблице должна быть SCD2 версия таблицы DE.HISTGROUP 
--(поля PERSON, CLASS, SALARY, EFFECTIVE_FROM, EFFECTIVE_TO).
--Возьмите в работу таблицы DE11AN.XXXX_SALARY_HIST и DE.SALARY_PAYMENTS. 
--Напишите SQL скрипт, выводящий таблицу платежей сотрудникам. 
--
--В таблице должны быть поля PAYMENT_DT, PERSON, PAYMENT, MONTH_PAID, MONTH_REST. 
--
--Результат выполнения сохраните в таблицу DE11AN.XXXX_SALARY_LOG.
--• PAYMENT_DT - дата выплаты,
--• MONTH_PAID - суммарно выплачено в месяце на дату последней выплаты,
--• MONTH_REST - осталось выплатить за месяц.
--Проверяется в первую очередь понимание как соединять фактовую таблицу 
--с SCD2 таблицей (нельзя все расчеты сделать над DE.SALARY_PAYMENTS, ведь 
--работнику могут недоплатить или переплатить).
--В ответе приложите SQL скрипт, таблица DE11AN.XXXX_SALARY_LOG должна быть заполнена.


select * from DE.histgroup h;
select * from DE.salary_payments sp;
--truncate table DE11AN.kart_SALARY_HIST;
select * from DE11AN.kart_SALARY_HIST;
--drop table DE11AN.kart_SALARY_HIST;
select * from DE11AN.kart_SALARY_LOG;
--Drop table DE11AN.kart_SALARY_LOG;

create table DE11AN.kart_SALARY_HIST(
	person varchar(50),
	"class" varchar(50),
	salary integer,
	EFFECTIVE_FROM timestamp, 
	EFFECTIVE_TO timestamp
);

create table DE11AN.kart_SALARY_LOG(
PAYMENT_DT timestamp, 
PERSON varchar(50), 
PAYMENT integer, 
MONTH_PAID integer, 
MONTH_REST integer
);


INSERT INTO DE11AN.kart_SALARY_HIST( person, class, salary, effective_from, effective_to)
	(select
		person,
		class,
		salary,
		dt as effective_from,
		coalesce(
			lead(dt) over (partition by person order by dt),
			to_date('3000-01-01', 'YYYY-MM-DD')
		) - interval '1 sec' as effective_to
	from de.histgroup);

INSERT INTO DE11AN.kart_SALARY_LOG(PAYMENT_DT, PERSON, PAYMENT, MONTH_PAID, MONTH_REST)(
	select 
	dt as PAYMENT_DT, 
	PERSON, 
	PAYMENT, 
	SUM(PAYMENT) OVER (PARTITION BY extract(MONTH from sp.dt), PERSON ORDER BY dt) AS MONTH_PAID,
	((SELECT 
		salary 
	from DE11AN.kart_SALARY_HIST sh
	where (sp.dt between effective_from and effective_to)
		and sp.person = sh.person) - SUM(PAYMENT) OVER (PARTITION BY extract(MONTH from sp.dt), PERSON ORDER BY dt)
	) as MONTH_REST
	from DE.salary_payments sp
	);
