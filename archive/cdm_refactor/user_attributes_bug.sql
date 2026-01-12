

SELECT MAX(calendar_date)
FROM data_vault_mvp.customer_model.customer_model_full_uk_de cmfud;

USE WAREHOUSE pipe_xlarge;
SELECT  *
FROM data_vault_mvp.customer_model.customer_model_full_uk_de cmfud
WHERE cmfud.member_id = 62972247;

SELECT *
FROM data_vault_mvp.customer_model.customer_model_full_uk_de cmfud
WHERE cmfud.member_original_affiliate_classification IS NULL;

SELECT count(*)
FROM data_vault_mvp.customer_model_full_uk_de_stg.static_member_attributes
WHERE updated_at < '2020-06-01';

USE WAREHOUSE pipe_xlarge;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.customer_model.customer_model_full_uk_de CLONE data_vault_mvp.customer_model.customer_model_full_uk_de;

UPDATE data_vault_mvp_dev_robin.customer_model.customer_model_full_uk_de target
SET target.member_signup_date                       = batch.signup_date,
    target.member_cohort_id                         = batch.cohort_id,
    target.member_original_affiliate_classification = batch.member_original_affiliate_classification,
    target.member_cohort_year_month                 = batch.cohort_year_month,
    target.member_original_affiliate_territory      = batch.original_affiliate_territory,
    target.member_original_affiliate_name           = batch.original_affiliate_name,
    target.member_acquisition_platform              = batch.acquisition_platform,
    target.member_acquisition_method                = batch.acquisition_method,
    target.member_has_new_app                       = batch.has_new_app,
    target.member_first_app_spv                     = batch.first_app_spv
FROM data_vault_mvp.customer_model_full_uk_de_stg.static_member_attributes batch
WHERE target.member_id = batch.user_id
  AND batch.updated_at < '2020-06-01' --members who've data hasn't changed
  AND target.updated_at >= '2020-06-01' --new calendar records
  AND target.member_original_affiliate_territory IS NULL --where static member attributes aren't set;

--remove current date from run
DELETE FROM data_vault_mvp_dev_robin.customer_model.customer_model_full_uk_de cmfud WHERE cmfud.calendar_date = '2020-06-01';
ALTER TABLE data_vault_mvp_dev_robin.customer_model.customer_model_full_uk_de RENAME TO customer_model_full_uk_de_fixed;

SELECT updated_at, count(*) from data_vault_mvp.customer_model.customer_model_full_uk_de cmfud GROUP BY 1;


self_describing_task --include 'dv/customer_model_full_uk_de/100_final_customer_model'  --method 'run' --start '2020-06-01 00:00:00' --end '2020-06-01 00:00:00'

SELECT COUNT(*) FROM data_vault_mvp.customer_model.customer_model_full_uk_de WHERE CUSTOMER_MODEL_FULL_UK_DE.member_original_affiliate_territory IS NULL;

--run in prod
CREATE OR REPLACE TABLE data_vault_mvp.customer_model.customer_model_full_uk_de copy grants clone data_vault_mvp_dev_robin.customer_model.customer_model_full_uk_de
