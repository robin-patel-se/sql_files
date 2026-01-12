SELECT *
FROM data_vault_mvp.bi.chrt_fact_cohort_metrics;


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_attributes CLONE data_vault_mvp.dwh.user_attributes;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_calendar CLONE data_vault_mvp.dwh.se_calendar;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_activity CLONE data_vault_mvp.dwh.user_activity;
CREATE OR REPLACE TRANSIENT TABLE se_dev_robin.data.posa_category_from_territory CLONE se.data.posa_category_from_territory;


self_describing_task --include 'biapp/task_catalogue/dv/bi/tableau/cohort_model/chrt_fact_cohort_metrics.py'  --method 'run' --start '2022-11-09 00:00:00' --end '2022-11-09 00:00:00'

SELECT *
FROM se.data.fact_booking fb
WHERE fb.booking_completed_timestamp;


SELECT *
FROM data_vault_mvp_dev_robin.bi.chrt_fact_cohort_metrics;


------------------------------------------------------------------------------------------------------------------------
-- Made a workbook that matches steph's screen shot
-- https://eu-west-1a.online.tableau.com/t/secretescapes/authoring/cohort_model_reconciliation/Sheet1#1

-- write a query to show the same figures are the workbook
SELECT
    YEAR(c.sign_up_month) AS sign_up_year,
    YEAR(c.event_month)   AS event_year,
    SUM(c.net_margin_constant_currency)
FROM data_vault_mvp.bi.chrt_fact_cohort_metrics c
WHERE YEAR(c.sign_up_month) = 2011
  AND YEAR(c.event_month) = 2014
GROUP BY 1, 2;

SELECT *
FROM data_vault_mvp.bi.chrt_fact_cohort_metrics c
WHERE YEAR(c.sign_up_month) = 2011
  AND YEAR(c.event_month) = 2014;

-- write a query to replicate financials in fact_booking to the cohort model query
SELECT
    SUM(fb.margin_gross_of_toms_gbp_constant_currency)
FROM se.data.fact_booking fb
    INNER JOIN se.data.se_user_attributes sua ON fb.shiro_user_id = sua.shiro_user_id
WHERE fb.booking_status_type = 'live'
  AND YEAR(fb.booking_completed_timestamp) = 2014
  AND YEAR(sua.signup_tstamp) = 2011;


--investigate bookings in fact booking to see if there are duplications
SELECT *
FROM se.data.fact_booking fb
    INNER JOIN se.data.se_user_attributes sua ON fb.shiro_user_id = sua.shiro_user_id
WHERE fb.booking_status_type = 'live'
  AND YEAR(fb.booking_completed_timestamp) = 2014
  AND YEAR(sua.signup_tstamp) = 2011
ORDER BY fb.shiro_user_id;


-- a lot of tvl bookings that might be incorrectly attached to se users, might need to exclude from cohort model

SELECT
    SUM(fb.margin_gross_of_toms_gbp_constant_currency)
FROM se.data.fact_booking fb
    INNER JOIN se.data.se_user_attributes sua ON fb.shiro_user_id = sua.shiro_user_id
WHERE fb.booking_status_type = 'live'
  AND YEAR(fb.booking_completed_timestamp) = 2015
  AND YEAR(sua.signup_tstamp) = 2011
  AND fb.se_brand = 'SE Brand';


SELECT *
FROM data_vault_mvp.dwh.chiasma_external_booking ceb
WHERE ceb.tech_platform = 'CHIASMA_TRAVELIST';

SELECT DISTINCT
    ceb.tech_platform
FROM data_vault_mvp.dwh.chiasma_external_booking ceb;


CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.se_booking AS
SELECT *
FROM data_vault_mvp.dwh.se_booking;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.dim_sale AS
SELECT *
FROM data_vault_mvp.dwh.dim_sale;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.rebooking CLONE data_vault_mvp.dwh.rebooking;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tb_booking CLONE data_vault_mvp.dwh.tb_booking;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.chiasma_external_booking CLONE data_vault_mvp.dwh.chiasma_external_booking;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tvl_booking CLONE data_vault_mvp.dwh.tvl_booking;


SELECT
    se_sale_id,
    sale_start_date,
    sale_end_date,
    sale_active,
    dim_sale.tech_platform
FROM se.data.dim_sale
WHERE se_sale_id = 'A8919';


SELECT *
FROM data_vault_mvp.dwh.tb_offer t;


SELECT *
FROM se.data.tb_offer t
WHERE t.se_sale_id = 'A8919';


