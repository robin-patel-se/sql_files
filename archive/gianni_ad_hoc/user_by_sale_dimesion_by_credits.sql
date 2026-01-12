USE WAREHOUSE pipe_large;

SELECT b.shiro_user_id,
       b.sale_id,
       b.check_in_date,
       s.sale_type
FROM data_vault_mvp.dwh.se_booking b
         LEFT JOIN se.data.dim_sale s ON b.sale_id = s.sale_id
WHERE b.booking_status = 'COMPLETE'
  AND s.sale_type != 'Hotel'
  AND s.sale_type != '3PP'
  AND b.check_in_date >= '2020-03-13'
  AND check_in_date <= '2020-04-30';