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
	and result_fg in  ('Повышен', 'Понижен', 'Положительный')
;
