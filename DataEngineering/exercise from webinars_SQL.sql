/*
8-9. Найдите как можно больше валидных и как можно меньше
невалидных номеров договоров и дат в таблице DE.PAYMENTS.
Валидные реквизиты договора: 12345/67 от 21.01.2022, они
приведены в поле reason_correct для самопроверки.
*/

SELECT COUNT(*) FROM DE.PAYMENTS;
--------------------------------------------------
SELECT COUNT(*) 
FROM DE.PAYMENTS
WHERE reason ~ '[0-9]{5}([^а-яА-Я]+?)(\d+)\D+(\d+?\D\d+?\D\d+$)';
--------------------------------------------------
/*8-12. Выведите имена покупателей (oe.customers), которые совершили заказ (oe.orders) с
возрастанием суммы (более поздний заказ на большую сумму, чем более ранний).*/
--------------------------------------------------
SELECT * FROM oe.customers;
SELECT * FROM oe.orders;
--------------------------------------------------
--SELECT 
--	customer_id,
--	order_date,
--	order_total
--FROM oe.orders
--WHERE customer_id in (
--	SELECT 
--		customer_id --, COUNT(order_id) as CountOrder
--	FROM oe.orders
--	GROUP BY customer_id
--	HAVING COUNT(order_id) > 1
--	)
--ORDER BY customer_id;
--------------------------------------------------
--SELECT DISTINCT 
--	c.customer_id
--FROM CoutnOrder c
--WHERE Count > 1 AND order_total > COALESCE(LagMaxOrder, 0);
--------------------------------------------------
WITH CoutnOrder AS(
	SELECT 
		customer_id
	,	order_date
	,	order_total
	,	order_id
	,	COUNT(order_id) OVER (PARTITION BY customer_id ORDER BY order_date) AS Count
	,	LAG(order_total) OVER (PARTITION BY customer_id ORDER BY order_date) AS LagMaxOrder
	FROM oe.orders
)
SELECT DISTINCT 
	c.customer_id
,	cust_first_name
FROM CoutnOrder c
INNER JOIN OE.customers c2 
	on c.customer_id = c2.customer_id
WHERE Count > 1 AND order_total > COALESCE(LagMaxOrder, 0);
--------------------------------------------------
