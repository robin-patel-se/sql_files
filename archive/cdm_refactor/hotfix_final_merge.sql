USE WAREHOUSE pipe_large;
UPDATE data_vault_mvp_dev_robin.customer_model.customer_model_full_uk_de target
SET target.member_age = batch.member_age
FROM data_vault_mvp_dev_robin.customer_model_full_uk_de_stg.static_member_calendar batch
WHERE target.member_id = batch.user_id
  AND target.calendar_date = batch.calendar_date;

CREATE OR REPLACE TABLE data_vault_mvp.customer_model.customer_model_full_uk_de CLONE data_vault_mvp_dev_robin.customer_model.customer_model_full_uk_de;

USE WAREHOUSE pipe_large;
--find a user with a booking before the cdm cut off
SELECT shiro_user_id,
       margin_gross_of_toms_gbp,
       booking_completed_date
FROM data_vault_mvp.dwh.se_booking
WHERE booking_completed_date <= '2018-06-01'
  AND shiro_user_id = 3576066;

--check that the data for this user is correct
SELECT *
FROM data_vault_mvp_dev_robin.customer_model.customer_model_full_uk_de
WHERE member_id = 3576066;


self_describing_task --include 'dv/customer_model_full_uk_de/100_final_customer_model'  --method 'run' --start '2020-03-23 00:00:00' --end '2020-03-23 00:00:00'