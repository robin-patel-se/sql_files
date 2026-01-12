SELECT COUNT(DISTINCT ioce.email) AS users
FROM data_vault_mvp.dwh.iterable__order_cancelled_event ioce
WHERE ioce.cancellation_tstamp BETWEEN '2020-01-01' AND '2021-09-30';


SELECT COUNT(DISTINCT i.email) AS users
FROM data_vault_mvp.dwh.iterable__order_complete_event i
WHERE i.booking_completed_timestamp BETWEEN '2020-01-01' AND '2021-09-30';

SELECT COUNT(DISTINCT i.email) AS users
FROM data_vault_mvp.dwh.iterable__order_complete_event i
WHERE i.booking_completed_timestamp BETWEEN '2020-01-01' AND '2021-09-30'
  AND i.booking_status = 'COMPLETE';


SELECT COUNT(DISTINCT i.email) AS users
FROM data_vault_mvp.dwh.iterable__order_complete_event i
WHERE i.check_in_date BETWEEN '2020-01-01' AND '2021-09-30'
  AND i.booking_status = 'COMPLETE';


SELECT COUNT(DISTINCT i.email) AS users
FROM data_vault_mvp.dwh.iterable__order_complete_event i
WHERE i.posu_city = 'Berlin'
  AND i.booking_status = 'COMPLETE';

SELECT COUNT(DISTINCT i.email) AS users
FROM data_vault_mvp.dwh.iterable__order_complete_event i
WHERE i.posu_country = 'Greece'
  AND i.booking_status = 'COMPLETE';

SELECT COUNT(DISTINCT i.email) AS users
FROM data_vault_mvp.dwh.iterable__order_complete_event i
WHERE i.no_nights >= 4
  AND i.booking_status = 'COMPLETE';

SELECT COUNT(DISTINCT i.email) AS users
FROM data_vault_mvp.dwh.iterable__order_complete_event i
WHERE i.gross_revenue_gbp >= 299
  AND i.booking_status = 'COMPLETE';

SELECT COUNT(DISTINCT i.email) AS users
FROM data_vault_mvp.dwh.iterable__order_complete_event i
WHERE i.sale_type = 'Hotel'
  AND i.booking_status = 'COMPLETE';

------------------------------------------------------------------------------------------------------------------------
--spot check users

SELECT *
FROM data_vault_mvp.dwh.iterable__order_complete_event ioce
WHERE ioce.shiro_user_id = 53468


SELECT *
FROM data_vault_mvp.dwh.iterable__order_cancelled_event c
WHERE c.shiro_user_id = 53468