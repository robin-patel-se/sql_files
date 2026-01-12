CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution CLONE data_vault_mvp.single_customer_view_stg.module_touch_attribution;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions CLONE data_vault_mvp.single_customer_view_stg.module_touched_transactions;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_attributes CLONE data_vault_mvp.dwh.user_attributes;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.fact_booking CLONE data_vault_mvp.dwh.fact_booking;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.dim_sale CLONE data_vault_mvp.dwh.dim_sale;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.event_grain CLONE data_vault_mvp.bi.event_grain;

DROP TABLE data_vault_mvp_dev_robin.bi.event_grain;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.bi.event_grain_20230505 CLONE data_vault_mvp.bi.event_grain;

self_describing_task --include 'biapp/task_catalogue/se/data/udfs/udf_functions.py'  --method 'run' --start '2023-04-26 00:00:00' --end '2023-04-26 00:00:00'

self_describing_task --include 'biapp/task_catalogue/dv/bi/tableau/demand_model/event_grain.py'  --method 'run' --start '2018-01-01 00:00:00' --end '2018-01-01 00:00:00'


self_describing_task --include 'biapp/task_catalogue/dv/bi/tableau/demand_model/event_grain.py'  --method 'run' --start '2023-05-04 00:00:00' --end '2023-05-04 00:00:00'


SELECT *
FROM collab.muse.snowflake_query_history_v2 qh
WHERE qh.query_id = '01ac1599-0202-7c64-0000-02dddcc000d6';


SELECT *
FROM snowflake.account_usage.query_history qh
WHERE qh.query_id = '01ac1599-0202-7c64-0000-02dddcc000d6';


-- checking dates that should be updated:

SELECT *
FROM data_vault_mvp_dev_robin.bi.event_grain__step01__booking_dates_batch;

-- taking a few of these dates and checking metrics on them


SELECT *
FROM data_vault_mvp_dev_robin.bi.event_grain eg
WHERE eg.date = '2023-04-30';


-- limiting to uk territory

SELECT
    eg.id,
    eg.member_recency_status,
    eg.current_affiliate_territory,
    eg.original_affiliate_territory,
    eg.date,
    eg.channel,
    eg.touch_experience,
    eg.platform,
    eg.posa_category,
    eg.product_configuration,
    eg.travel_type,
    eg.bookings,
    eg.bookings_last_paid,
    eg.margin_gbp_constant_currency,
    eg.margin_gbp_constant_currency_last_paid,
    eg.no_nights,
    eg.no_nights_last_paid,
    eg.rooms,
    eg.rooms_last_paid,
    eg.gross_rooms,
    eg.gross_rooms_last_paid,
    eg.gross_bookings,
    eg.gross_bookings_last_paid,
    eg.gross_margin_gbp_constant_currency,
    eg.gross_margin_gbp_constant_currency_last_paid,
    eg.gross_no_nights,
    eg.gross_no_nights_last_paid,
    eg.spvs,
    eg.spvs_last_paid,
    eg.sessions,
    eg.sessions_last_paid,
    eg.users,
    eg.users_last_paid
FROM data_vault_mvp_dev_robin.bi.event_grain eg
WHERE eg.date = '2023-04-30'
  AND current_affiliate_territory = 'UK';


-- checking against prod

SELECT
    eg.id,
    eg.member_recency_status,
    eg.current_affiliate_territory,
    eg.original_affiliate_territory,
    eg.date,
    eg.channel,
    eg.touch_experience,
    eg.platform,
    eg.posa_category,
    eg.product_configuration,
    eg.travel_type,
    eg.bookings,
    eg.bookings_last_paid,
    eg.margin_gbp_constant_currency,
    eg.margin_gbp_constant_currency_last_paid,
    eg.no_nights,
    eg.no_nights_last_paid,
    eg.rooms,
    eg.rooms_last_paid,
    eg.gross_rooms,
    eg.gross_rooms_last_paid,
    eg.gross_bookings,
    eg.gross_bookings_last_paid,
    eg.gross_margin_gbp_constant_currency,
    eg.gross_margin_gbp_constant_currency_last_paid,
    eg.gross_no_nights,
    eg.gross_no_nights_last_paid,
    eg.spvs,
    eg.spvs_last_paid,
    eg.sessions,
    eg.sessions_last_paid,
    eg.users,
    eg.users_last_paid
FROM data_vault_mvp.bi.event_grain eg
WHERE eg.date = '2023-04-30'
  AND current_affiliate_territory = 'UK';

------------------------------------------------------------------------------------------------------------------------

-- checking aggregations match tableau reconciliation dashboard
-- https://eu-west-1a.online.tableau.com/#/site/secretescapes/views/ModelReconcilliation/ComparingModels?:iid=2

--dev aggregate
SELECT
    YEAR(date),
    SUM(margin_gbp_constant_currency)
FROM data_vault_mvp_dev_robin.bi.event_grain eg
WHERE COALESCE(eg.posa_category, 'XX') NOT IN ('Other', 'Poland')
GROUP BY 1
ORDER BY 1;

--prod aggregate
SELECT
    YEAR(date),
    SUM(margin_gbp_constant_currency)
FROM data_vault_mvp.bi.event_grain eg
WHERE eg.posa_category NOT IN ('Other', 'Poland')
GROUP BY 1
ORDER BY 1;

--replicating transaction model
SELECT
    YEAR(fb.booking_completed_date),
    SUM(fb.margin_gross_of_toms_gbp_constant_currency)
FROM data_vault_mvp.dwh.fact_booking fb
WHERE se.data.posa_category_from_territory(fb.territory) NOT IN ('Other', 'Poland')
  AND fb.booking_status_type = 'live'
GROUP BY 1
ORDER BY 1;

SELECT DISTINCT
    tech_platform,
    se.data.posa_category_from_territory(fcb.territory) NOT IN ('Other', 'Poland')
FROM se.data.fact_complete_booking fcb;


------------------------------------------------------------------------------------------------------------------------
USE WAREHOUSE pipe_xlarge;

SELECT
    SUM(fb.margin_gross_of_toms_gbp_constant_currency)
FROM data_vault_mvp_dev_robin.dwh.fact_booking fb
    INNER JOIN data_vault_mvp_dev_robin.bi.event_grain__step01__booking_dates_batch batch
               ON fb.booking_completed_date::DATE = batch.dates
                   -- filter for batch of dates
    LEFT JOIN  data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions stt
               ON fb.booking_id = stt.booking_id
    LEFT JOIN  data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution sta
               ON stt.touch_id = sta.touch_id AND sta.attribution_model = 'last paid'
    LEFT JOIN  data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel stmc
               ON sta.attributed_touch_id = stmc.touch_id
    LEFT JOIN  data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes stba
               ON stt.touch_id = stba.touch_id
    LEFT JOIN  data_vault_mvp_dev_robin.dwh.user_attributes sua
               ON TRY_TO_NUMBER(stba.attributed_user_id) = sua.shiro_user_id
    LEFT JOIN  data_vault_mvp_dev_robin.dwh.dim_sale ds ON fb.se_sale_id = ds.se_sale_id
WHERE fb.booking_status_type = 'live'
  AND se.data.posa_category_from_territory(fb.territory) NOT IN ('Other', 'Poland')
  AND YEAR(fb.booking_completed_timestamp) = 2022;


SELECT *
FROM data_vault_mvp_dev_robin.bi.event_grain__step01__booking_dates_batch;


SELECT
    YEAR(date),
    SUM(margin_gbp_constant_currency)
FROM data_vault_mvp_dev_robin.bi.event_grain__step02__bookings_last_non_direct eg
WHERE posa_category NOT IN ('Other', 'Poland')
GROUP BY 1
ORDER BY 1;

DROP TABLE data_vault_mvp_dev_robin.bi.event_grain;

SELECT
    YEAR(date),
    SUM(margin_gbp_constant_currency)
FROM data_vault_mvp_dev_robin.bi.event_grain eg
WHERE posa_category NOT IN ('Other', 'Poland')
GROUP BY 1
ORDER BY 1;

------------------------------------------------------------------------------------------------------------------------
-- going to consolidate the code

-- create a backup of output data  to compare against refactoring changes
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.event_grain_20230510 CLONE data_vault_mvp_dev_robin.bi.event_grain;
DROP TABLE data_vault_mvp_dev_robin.bi.event_grain;

-- dev before refactoring demand model
SELECT
    YEAR(date),
    SUM(margin_gbp_constant_currency),
    SUM(bookings),
    SUM(spvs),
    SUM(sessions)
FROM data_vault_mvp_dev_robin.bi.event_grain_20230510 eg
WHERE posa_category NOT IN ('Other', 'Poland')
GROUP BY 1
ORDER BY 1;

-- refactored dev demand model
SELECT
    YEAR(date),
    SUM(margin_gbp_constant_currency),
    SUM(bookings),
    SUM(spvs),
    SUM(sessions)
FROM data_vault_mvp_dev_robin.bi.event_grain eg
WHERE posa_category NOT IN ('Other', 'Poland')
GROUP BY 1
ORDER BY 1;


-- prod demand model
SELECT
    YEAR(date),
    SUM(margin_gbp_constant_currency),
    SUM(bookings),
    SUM(spvs),
    SUM(sessions)
FROM data_vault_mvp.bi.event_grain eg
WHERE posa_category NOT IN ('Other', 'Poland')
GROUP BY 1
ORDER BY 1;

-- production fact booking/transaction model

SELECT
    YEAR(fb.booking_completed_date),
    SUM(fb.margin_gross_of_toms_gbp_constant_currency) AS margin_gbp_constant_currency,
    COUNT(DISTINCT fb.booking_id)                      AS bookings
FROM data_vault_mvp.dwh.fact_booking fb
WHERE se.data.posa_category_from_territory(fb.territory) NOT IN ('Other', 'Poland')
  AND fb.booking_status_type = 'live'
  AND fb.booking_completed_timestamp >= '2018-01-01'
GROUP BY 1
ORDER BY 1;


-- validating spvs against demand model prod
-- validating bookings and margin against transaction model

------------------------------------------------------------------------------------------------------------------------

-- checking bookings on month level

-- dev demand model
SELECT
    YEAR(date),
    MONTH(date),
    SUM(margin_gbp_constant_currency),
    SUM(bookings)
FROM data_vault_mvp_dev_robin.bi.event_grain eg
WHERE posa_category NOT IN ('Other', 'Poland')
GROUP BY 1, 2
ORDER BY 1, 2;


-- production fact booking/transaction model

SELECT
    YEAR(fb.booking_completed_date),
    MONTH(fb.booking_completed_date),
    SUM(fb.margin_gross_of_toms_gbp_constant_currency) AS margin_gbp_constant_currency,
    COUNT(DISTINCT fb.booking_id)                      AS bookings
FROM data_vault_mvp.dwh.fact_booking fb
WHERE se.data.posa_category_from_territory(fb.territory) NOT IN ('Other', 'Poland')
  AND fb.booking_status_type = 'live'
  AND fb.booking_completed_timestamp >= '2018-01-01'
GROUP BY 1, 2
ORDER BY 1, 2;

-- dev demand model spvs
SELECT
    YEAR(date),
    MONTH(date),
    SUM(spvs),
    SUM(sessions)
FROM data_vault_mvp_dev_robin.bi.event_grain eg
WHERE posa_category NOT IN ('Other', 'Poland')
GROUP BY 1, 2
ORDER BY 1, 2;


-- prod demand model
SELECT
    YEAR(date),
    MONTH(date),
    SUM(spvs),
    SUM(sessions)
FROM data_vault_mvp.bi.event_grain eg
WHERE posa_category NOT IN ('Other', 'Poland')
GROUP BY 1, 2
ORDER BY 1, 2;

------------------------------------------------------------------------------------------------------------------------
--checking demand model spv by channel changes

-- dev channel
SELECT
    eg.channel,
    SUM(spvs),
    SUM(sessions)
FROM data_vault_mvp_dev_robin.bi.event_grain eg
WHERE posa_category NOT IN ('Other', 'Poland')
GROUP BY 1
ORDER BY 1;


-- prod channel
SELECT
    eg.channel,
    SUM(spvs),
    SUM(sessions)
FROM data_vault_mvp.bi.event_grain eg
WHERE posa_category NOT IN ('Other', 'Poland')
GROUP BY 1
ORDER BY 1;


-- dev before refactoring demand model
SELECT
    eg.channel,
    SUM(spvs),
    SUM(sessions)
FROM data_vault_mvp_dev_robin.bi.event_grain_20230510 eg
WHERE posa_category NOT IN ('Other', 'Poland')
GROUP BY 1
ORDER BY 1;


SELECT *
FROM collab.muse.snowflake_query_history_v2 s
WHERE s.pipeline_script_path = 'dv/bi/tableau/demand_model/event_grain.py'
  AND s.start_time::DATE = CURRENT_DATE
  AND s.total_elapsed_time_sec > 1;



DELETE FROM data_vault_mvp_dev_robin.bi.event_grain WHERE schedule_tstamp::DATE = '2023-05-08';

CREATE OR REPLACE VIEW collab.muse.event_grain AS SELECT * FROM data_vault_mvp_dev_robin.bi.event_grain eg;

GRANT SELECT ON TABLE collab.muse.event_grain TO ROLE tableau
;
