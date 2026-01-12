/*
There are a few changes I want to make to the RFV tables in dbt. Essentially, we had the main rfv table which was the historical backfill (which was weekly) in the same table as the daily append happening now.

Essentially it's getting a bit confusing having weekly and daily views together, so I'm trying to split them into a daily and weekly table. I've mapped out how I'd do this below, but don't have the relevant permissions. Are you able to help?


1. Move weekly data from original table into the weekly_historical:
insert into  dbt.bi_customer_insight.ci_rfv_segments_historical_weekly

select * from
dbt.bi_customer_insight.ci_rfv_segments_historical
     inner join se.bi.calendar c on run_date = c.date_value
and c.day_of_week = 7
and run_date < ('2023-05-27')

2. Rename table dbt.bi_customer_insight.ci_rfv_segments_historical to be ci_rfv_segments_historical_daily *(how does this work with dbt? Do I just change the file name too?)

3. Delete the old weekly data from ci_rfv_segments_historical_daily so that it is just the daily
delete from
dbt.bi_customer_insight.ci_rfv_segments_historical(_daily now when re-named)
where run_date < ('2023-05-14')

4. Do the exact same 3 steps above with the ci_rfv_customer_base_historical and ci_rfv_customer_base_historical_weekly tables.

 */

USE ROLE pipelinerunner
;

USE WAREHOUSE pipe_xlarge
;

-- backup prod historical weekly table
CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_rfv_segments_historical_weekly_20230606 CLONE dbt.bi_customer_insight.ci_rfv_segments_historical_weekly
;


BEGIN TRANSACTION
;
-- move weekly data from historical daily table into weekly
INSERT INTO dbt.bi_customer_insight.ci_rfv_segments_historical_weekly
SELECT
	rfv.run_date,
	rfv.shiro_user_id,
	rfv.uuid,
	rfv.rfv_segment,
	rfv.segment_rank,
	rfv.lifecycle,
	rfv.mega_segment,
	rfv.micro_segment,
	rfv.mau_flag,
	rfv.wau_flag,
	rfv.booker_history,
	rfv.margin_segment,
	rfv.session_frequency_agg,
	rfv.session_recency_agg,
	rfv.converted_l30
FROM dbt.bi_customer_insight.ci_rfv_segments_historical_20230606 rfv
WHERE DAYNAME(rfv.run_date) = 'Sun'
  AND rfv.run_date < ('2023-05-27')
;

COMMIT
;

-- backup prod historical daily table
-- CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_rfv_segments_historical_20230606 CLONE dbt.bi_customer_insight.ci_rfv_segments_historical
-- ;

-- rename historical table to daily
ALTER TABLE dbt.bi_customer_insight.ci_rfv_segments_historical
	RENAME TO dbt.bi_customer_insight.ci_rfv_segments_historical_daily
;

BEGIN TRANSACTION
;
-- delete historic weekly data from daily table
DELETE
FROM dbt.bi_customer_insight.ci_rfv_segments_historical_daily
WHERE run_date < ('2023-05-14')
;

COMMIT
;



------------------------------------------------------------------------------------------------------------------------
-- Do the exact same 3 steps above with the ci_rfv_customer_base_historical and ci_rfv_customer_base_historical_weekly tables.


-- backup prod historical weekly table
-- CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_rfv_customer_base_historical_weekly_20230606 CLONE dbt.bi_customer_insight.ci_rfv_customer_base_historical_weekly
-- ;

BEGIN TRANSACTION
;

-- drop accidental columns
ALTER TABLE dbt.bi_customer_insight.ci_rfv_customer_base_historical_weekly
	DROP COLUMN date_value, day_name, YEAR, se_year, se_week, MONTH, month_name, day_of_month, day_of_week, WEEK_START
;

-- move weekly data from historical daily table into weekly
INSERT INTO dbt.bi_customer_insight.ci_rfv_customer_base_historical_weekly
SELECT
	rfv.shiro_user_id,
	rfv.run_date,
	rfv.uuid,
	rfv.signup_date,
	rfv.days_since_signup,
	rfv.used_app_in_ly,
	rfv.last_booking_date,
	rfv.days_since_last_booking,
	rfv.se_sale_id_last_live_booking,
	rfv.product_type_last_live_booking,
	rfv.posu_country_last_live_booking,
	rfv.se_sale_id_last_booking,
	rfv.product_type_last_booking,
	rfv.posu_country_last_booking,
	rfv.last_booking_was_cancelled,
	rfv.last_cancelled_booking_date_of_booking,
	rfv.days_since_last_cancelled_booking,
	rfv.last_cancelled_booking_cancellation_date,
	rfv.last_cancelled_booking_cancellation_tstamp,
	rfv.most_recent_booking_activity_tstamp,
	rfv.future_trip_state,
	rfv.past_trip_state,
	rfv.days_since_last_trip,
	rfv.days_til_next_trip,
	rfv.currently_on_trip,
	rfv.currently_have_trip_in_future,
	rfv.have_had_trip_in_past,
	rfv.total_live_bookings,
	rfv.converted_l30,
	rfv.total_cancelled_bookings,
	rfv.margin_of_cancelled_booking,
	rfv.total_margin,
	rfv.avg_margin_per_booking,
	rfv.total_revenue,
	rfv.total_live_bookings_l7,
	rfv.total_live_bookings_l30,
	rfv.total_live_bookings_l2m,
	rfv.total_live_bookings_l6m,
	rfv.total_live_bookings_ly,
	rfv.total_cancelled_bookings_l30,
	rfv.total_cancelled_bookings_l2m,
	rfv.total_cancelled_bookings_ly,
	rfv.last_session,
	rfv.days_since_last_session,
	rfv.second_last_session,
	rfv.days_since_second_last_session,
	rfv.last_session_days_since_previous,
	rfv.total_sessions,
	rfv.total_sessions_l7,
	rfv.total_sessions_l30,
	rfv.total_sessions_l2m,
	rfv.total_sessions_l2m_og,
	rfv.total_sessions_between_l30_l60,
	rfv.total_sessions_l6m,
	rfv.total_spvs,
	rfv.total_spvs_l7,
	rfv.total_spvs_l30,
	rfv.total_days_with_session_l30,
	rfv.total_days_with_spv_l30,
	rfv.total_different_spvs_viewed_post_booking,
	rfv.viewed_different_spv_post_booking,
	rfv.done_session_post_cancellation
FROM dbt.bi_customer_insight.ci_rfv_customer_base_historical rfv
WHERE DAYNAME(rfv.run_date) = 'Sun'
  AND rfv.run_date < ('2023-05-27')
;

COMMIT
;

-- backup prod historical daily table
CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_rfv_customer_base_historical_20230606 CLONE dbt.bi_customer_insight.ci_rfv_customer_base_historical
;


-- rename historical table to daily
ALTER TABLE dbt.bi_customer_insight.ci_rfv_customer_base_historical
	RENAME TO dbt.bi_customer_insight.ci_rfv_customer_base_historical_daily
;

BEGIN TRANSACTION
;
-- delete historic weekly data from daily table
DELETE
FROM dbt.bi_customer_insight.ci_rfv_customer_base_historical_daily
WHERE run_date < ('2023-05-14')
;

COMMIT
;


------------------------------------------------------------------------------------------------------------------------


SELECT
	COUNT(*)
FROM dbt.bi_customer_insight.ci_rfv_segments_historical_weekly
;

SELECT
	COUNT(*)
FROM dbt.bi_customer_insight.ci_rfv_segments_historical_weekly_20230606
;


SELECT
	rfv.run_date,
	rfv.shiro_user_id,
	rfv.uuid,
	rfv.rfv_segment,
	rfv.segment_rank,
	rfv.lifecycle,
	rfv.mega_segment,
	rfv.micro_segment,
	rfv.mau_flag,
	rfv.wau_flag,
	rfv.booker_history,
	rfv.margin_segment,
	rfv.session_frequency_agg,
	rfv.session_recency_agg,
	rfv.converted_l30
FROM dbt.bi_customer_insight.ci_rfv_segments_historical_20230606 rfv
WHERE DAYOFWEEK(rfv.run_date) = 1
--   AND rfv.run_date < ('2023-05-27')


SELECT DISTINCT
	run_date,
	DAYOFWEEK(run_date),
	DAYNAME(run_date)
FROM dbt.bi_customer_insight.ci_rfv_segments_historical_20230606
;

USE ROLE pipelinerunner
;


SELECT
	COUNT(*)
FROM dbt.bi_customer_insight.ci_rfv_segments_historical_weekly
;


SELECT
	run_date,
	COUNT(*)
FROM dbt.bi_customer_insight.ci_rfv_segments_historical_weekly
WHERE YEAR(run_date) = '2023'
GROUP BY 1
;


------------------------------------------------------------------------------------------------------------------------
USE ROLE pipelinerunner
;
-- data was missing some weeks at the beginning of 2023
INSERT INTO dbt.bi_customer_insight.ci_rfv_segments_historical_weekly
SELECT *
FROM dbt_dev.dbt_georgieagnew_customer_insight__intermediate.ci_rfv_backill_segments_final
WHERE run_date BETWEEN ('2022-12-25') AND ('2023-02-25')
;



INSERT INTO dbt.bi_customer_insight.ci_rfv_segments_historical_weekly
SELECT *
FROM dbt.bi_customer_insight.ci_rfv_segments_historical_daily
WHERE run_date = '2023-05-21'


------------------------------------------------------------------------------------------------------------------------
-- email on 18th July

/*
Table 1: Customer Base explorer
a) remove test weeks from dbt table
delete from DBT.BI_CUSTOMER_INSIGHT.CI_RFV_CUSTOMER_BASE_EXPLORER
where run_date < '2023-07-16'
b) add in back fill
insert into DBT.BI_CUSTOMER_INSIGHT.CI_RFV_CUSTOMER_BASE_EXPLORER
select * from customer_insight.temp.rfv_segmentation_kpis_base1
*/

USE ROLE pipelinerunner
;

USE WAREHOUSE pipe_xlarge
;

CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_rfv_customer_base_explorer_20230718 CLONE dbt.bi_customer_insight.ci_rfv_customer_base_explorer
;

DELETE
FROM dbt.bi_customer_insight.ci_rfv_customer_base_explorer
WHERE run_date < '2023-07-16'
;


INSERT INTO dbt.bi_customer_insight.ci_rfv_customer_base_explorer
SELECT *
FROM customer_insight.temp.rfv_segmentation_kpis_base1

/*
Table 2: KPI build
a) remove test weeks from dbt table
delete from DBT.BI_CUSTOMER_INSIGHT__INTERMEDIATE.CI_RFV_KPIS_BUILD
where run_date < '2023-07-16'
b) add in back fill
insert into DBT.BI_CUSTOMER_INSIGHT__INTERMEDIATE.CI_RFV_KPIS_BUILD
select * from customer_insight.temp.rfv_segmentation_kpis_base2
*/

CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight__intermediate.ci_rfv_kpis_build_20230718 CLONE dbt.bi_customer_insight__intermediate.ci_rfv_kpis_build
;

DELETE
FROM dbt.bi_customer_insight__intermediate.ci_rfv_kpis_build
WHERE run_date < '2023-07-16'
;


INSERT INTO dbt.bi_customer_insight__intermediate.ci_rfv_kpis_build
SELECT *
FROM customer_insight.temp.rfv_segmentation_kpis_base2
;
/*

Table 3: RFV KPIs
a) add in back fill (no test weeks to remove)
insert into  DBT.BI_CUSTOMER_INSIGHT.CI_RFV_KPIS
select * from customer_insight.temp.rfv_segmentation_kpis_final
b) column order change (so sorry, this is annoying so dont worry if not
At the moment the last 3 columns are total_members, uuid, total_members_4wk_benchmark. It meant to have uuid, total_members, total_members-4wk_benchmark. No worries if not on this one, just me being picky.
*/

CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_rfv_kpis_20230718 CLONE dbt.bi_customer_insight.ci_rfv_kpis
;

USE WAREHOUSE pipe_xlarge
;

CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_rfv_kpis_temp CLONE dbt.bi_customer_insight.ci_rfv_kpis
;

INSERT INTO dbt.bi_customer_insight.ci_rfv_kpis_temp
SELECT
	rfv_segmentation_kpis_final.week_commencing,
	rfv_segmentation_kpis_final.territory_grouped,
	rfv_segmentation_kpis_final.lifecycle,
	rfv_segmentation_kpis_final.rfv_segment,
	rfv_segmentation_kpis_final.wau_flag,
	rfv_segmentation_kpis_final.lifecycle_lw,
	rfv_segmentation_kpis_final.rfv_segment_lw,
	rfv_segmentation_kpis_final.wau_flag_lw,
	rfv_segmentation_kpis_final.lifecycle_2lw,
	rfv_segmentation_kpis_final.rfv_segment_2lw,
	rfv_segmentation_kpis_final.wau_flag_2lw,
	rfv_segmentation_kpis_final.uuid,
	rfv_segmentation_kpis_final.total_members,
	rfv_segmentation_kpis_final.total_members_4wk_benchmark
FROM customer_insight.temp.rfv_segmentation_kpis_final
;

CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_rfv_kpis AS
SELECT
	week_commencing,
	territory_grouped,
	lifecycle,
	rfv_segment,
	wau_flag,
	lifecycle_lw,
	rfv_segment_lw,
	wau_flag_lw,
	lifecycle_2lw,
	rfv_segment_2lw,
	wau_flag_2lw,
	uuid,
	total_members,
	total_members_4wk_benchmark
FROM dbt.bi_customer_insight.ci_rfv_kpis_temp
;

SELECT
	COUNT(*)
FROM dbt.bi_customer_insight.ci_rfv_kpis
;
/*
Table 4: KPIs for bookings/SPVs
a) remove test weeks from dbt table -- note this will leave the table empty
delete from DBT.BI_CUSTOMER_INSIGHT.CI_RFV_KPIS_BOOKINGS_SPVS
where run_date = '2023-07-10'
b) add in back fill
insert into DBT.BI_CUSTOMER_INSIGHT.CI_RFV_KPIS_BOOKINGS_SPVS
select * from customer_insight.temp.rfv_bookings_spvs_kpis_by_seg
*/

CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_rfv_kpis_bookings_spvs_20230718 CLONE dbt.bi_customer_insight.ci_rfv_kpis_bookings_spvs
;


DELETE
FROM dbt.bi_customer_insight.ci_rfv_kpis_bookings_spvs
WHERE week_start_date = '2023-07-10'
;

INSERT INTO dbt.bi_customer_insight.ci_rfv_kpis_bookings_spvs
SELECT *
FROM customer_insight.temp.rfv_bookings_spvs_kpis_by_seg
;

/*
Table 5: RFV Segment movement
a) add in back fill (no test weeks to remove)
insert into  DBT.BI_CUSTOMER_INSIGHT__INTERMEDIATE.CI_RFV_SEGMENT_MOVEMENT
select * from customer_insight.temp.rfv_segment_movements

 */

CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight__intermediate.ci_rfv_segment_movement_20230718 CLONE dbt.bi_customer_insight__intermediate.ci_rfv_segment_movement
;

INSERT INTO dbt.bi_customer_insight__intermediate.ci_rfv_segment_movement
SELECT *
FROM customer_insight.temp.rfv_segment_movements
;


DELETE
FROM dbt.bi_customer_insight.ci_rfv_kpis_bookings_spvs
WHERE week_start_date < '2019-01-01'


------------------------------------------------------------------------------------------------------------------------
-- email from the 21st Jul

USE ROLE pipelinerunner
;

USE WAREHOUSE pipe_xlarge
;

DELETE
FROM dbt.bi_customer_insight.ci_rfv_kpis
WHERE week_commencing = '2023-07-10'
;


INSERT INTO dbt.bi_customer_insight.ci_rfv_kpis
SELECT
	rfv_segmentation_kpis_final.week_commencing,
	rfv_segmentation_kpis_final.territory_grouped,
	rfv_segmentation_kpis_final.lifecycle,
	rfv_segmentation_kpis_final.rfv_segment,
	rfv_segmentation_kpis_final.wau_flag,
	rfv_segmentation_kpis_final.lifecycle_lw,
	rfv_segmentation_kpis_final.rfv_segment_lw,
	rfv_segmentation_kpis_final.wau_flag_lw,
	rfv_segmentation_kpis_final.lifecycle_2lw,
	rfv_segmentation_kpis_final.rfv_segment_2lw,
	rfv_segmentation_kpis_final.wau_flag_2lw,
	rfv_segmentation_kpis_final.uuid,
	rfv_segmentation_kpis_final.total_members,
	rfv_segmentation_kpis_final.total_members_4wk_benchmark
FROM customer_insight.temp.rfv_segmentation_kpis_final
WHERE week_commencing = '2023-07-10'
;

------------------------------------------------------------------------------------------------------------------------
-- email on 25th Jul
USE ROLE pipelinerunner
;

CREATE OR REPLACE TABLE dbt.bi_customer_insight.ci_rfv_kpis_bookings_spvs
AS
SELECT *
FROM customer_insight.temp.rfv_bookings_spvs_kpis_by_seg_v2
;

------------------------------------------------------------------------------------------------------------------------
-- slack on the 26th
USE ROLE pipelinerunner
;

CREATE OR REPLACE TABLE dbt.bi_customer_insight.ci_rfv_kpis_bookings_spvs
AS
SELECT
	ci_rfv_kpis_bookings_spvs.week_start_date,
	ci_rfv_kpis_bookings_spvs.lifecycle,
	ci_rfv_kpis_bookings_spvs.mega_segment,
	ci_rfv_kpis_bookings_spvs.rfv_segment,
	ci_rfv_kpis_bookings_spvs.territory_grouped,
	ci_rfv_kpis_bookings_spvs.email_opt_in,
	ci_rfv_kpis_bookings_spvs.total_customers,
	ci_rfv_kpis_bookings_spvs.total_waus,
	ci_rfv_kpis_bookings_spvs.total_bookings,
	ci_rfv_kpis_bookings_spvs.total_spvs,
	ci_rfv_kpis_bookings_spvs.total_sessions,
	ci_rfv_kpis_bookings_spvs.uuid,
	CONCAT(week_start_date, rfv_segment, territory_grouped, email_opt_in) AS uuid
FROM dbt.bi_customer_insight.ci_rfv_kpis_bookings_spvs
;

------------------------------------------------------------------------------------------------------------------------
-- slack on the 14th Sept:
USE ROLE pipelinerunner
;
// TABLE 1 -- BASE TABLE

CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_rfv_customer_base_historical_weekly_20230606 CLONE dbt.bi_customer_insight.ci_rfv_customer_base_historical_weekly
;

-- CREATE OR REPLACE TRANSIENT TABLE dbt_dev.dbt_georgieagnew_customer_insight__intermediate.ci_rfv_backill_customer_base_breakdown_5_final_20230606 CLONE dbt_dev.dbt_georgieagnew_customer_insight__intermediate.ci_rfv_backill_customer_base_breakdown_5_final
;
//do you want clones of both tables?
USE WAREHOUSE pipe_2xlarge
;

BEGIN TRANSACTION
;
//insert all the deleted users into main base table
INSERT INTO dbt.bi_customer_insight.ci_rfv_customer_base_historical_weekly
SELECT *
FROM dbt_dev.dbt_georgieagnew_customer_insight__intermediate.ci_rfv_backill_customer_base_breakdown_5_final rfv
;

COMMIT
;

-----------
//after its been committed - remove the dupes
-- CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_rfv_customer_base_historical_weekly_20230606_v2 CLONE dbt.bi_customer_insight.ci_rfv_customer_base_historical_weekly
;
//made the 2nd clone different - v2

USE WAREHOUSE pipe_2xlarge
;

BEGIN TRANSACTION
;

-- RP: removed dupe to issues
-- DELETE
-- FROM dbt.bi_customer_insight.ci_rfv_customer_base_historical_weekly
--
-- SELECT *
-- FROM (
-- 	SELECT *,
-- 		   ROW_NUMBER() OVER (PARTITION BY shiro_user_id,run_date ORDER BY 1) AS rn
-- 	FROM dbt.bi_customer_insight.ci_rfv_customer_base_historical_weekly
-- )
-- WHERE rn > 1
-- ;


CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_rfv_customer_base_historical_weekly AS (
	SELECT *
	FROM dbt.bi_customer_insight.ci_rfv_customer_base_historical_weekly_20230606_v2 bkup
	QUALIFY ROW_NUMBER() OVER (PARTITION BY bkup.shiro_user_id, bkup.run_date ORDER BY 1) = 1
)
;

COMMIT
;

// TABLE 2 -- RFV TABLE

CREATE OR REPLACE TRANSIENT TABLE dbt_dev.dbt_georgieagnew_customer_insight__intermediate.ci_rfv_backill_segments_final_20230606 CLONE dbt_dev.dbt_georgieagnew_customer_insight__intermediate.ci_rfv_backill_segments_final
;

CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_rfv_segments_historical_weekly_20230606 CLONE dbt.bi_customer_insight.ci_rfv_segments_historical_weekly
;
//do you want clones of both tables?

BEGIN TRANSACTION
;
//insert all the deleted users into main base table
INSERT INTO dbt.bi_customer_insight.ci_rfv_segments_historical_weekly
SELECT *
FROM dbt_dev.dbt_georgieagnew_customer_insight__intermediate.ci_rfv_backill_segments_final
;

COMMIT
;

-----------
//after its been committed - remove the dupes
-- CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_rfv_segments_historical_weekly_20230606_v2 CLONE dbt.bi_customer_insight.ci_rfv_segments_historical_weekly
-- ;
//made the 2nd clone different - v2

BEGIN TRANSACTION
;

-- RP: removed dupe to issues
-- DELETE
-- FROM dbt.bi_customer_insight.ci_rfv_segments_historical_weekly

-- SELECT *
-- FROM (
-- 	SELECT *,
-- 		   ROW_NUMBER() OVER (PARTITION BY shiro_user_id,run_date ORDER BY 1) AS rn
-- 	FROM dbt.bi_customer_insight.ci_rfv_segments_historical_weekly
-- )
-- WHERE rn > 1
-- ;
USE WAREHOUSE pipe_2xlarge
;

CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_rfv_segments_historical_weekly AS
SELECT *
FROM dbt.bi_customer_insight.ci_rfv_segments_historical_weekly_20230606_v2 bkup
QUALIFY ROW_NUMBER() OVER (PARTITION BY bkup.shiro_user_id, bkup.run_date ORDER BY 1) = 1
;

COMMIT
;


// RFV - Customer base explorer

// Create backups
CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_rfv_customer_base_explorer_20230606 CLONE dbt.bi_customer_insight.ci_rfv_customer_base_explorer
;

CREATE OR REPLACE TRANSIENT TABLE customer_insight.temp.rfv_segmentation_kpis_base1_20230606 CLONE customer_insight.temp.rfv_segmentation_kpis_base1
;

USE WAREHOUSE pipe_2xlarge
;

BEGIN TRANSACTION
;
//replace table with new one
CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_rfv_customer_base_explorer AS
SELECT *
FROM customer_insight.temp.rfv_segmentation_kpis_base1
;

COMMIT
;


// RFV - KPIs build

// Create backups
CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight__intermediate.ci_rfv_kpis_build_20230606 CLONE dbt.bi_customer_insight__intermediate.ci_rfv_kpis_build
;

CREATE OR REPLACE TRANSIENT TABLE customer_insight.temp.rfv_segmentation_kpis_base2_20230606 CLONE customer_insight.temp.rfv_segmentation_kpis_base2
;

BEGIN TRANSACTION
;
//replace table with new one
CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight__intermediate.ci_rfv_kpis_build AS
SELECT *
FROM customer_insight.temp.rfv_segmentation_kpis_base2
;

COMMIT
;



// RFV - KPIs final

// Create backups
CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_rfv_kpis_20230606 CLONE dbt.bi_customer_insight.ci_rfv_kpis
;

CREATE OR REPLACE TRANSIENT TABLE customer_insight.temp.rfv_segmentation_kpis_final_20230606 CLONE customer_insight.temp.rfv_segmentation_kpis_base2
;

BEGIN TRANSACTION
;
//replace table with new one
CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_rfv_kpis AS
SELECT *
FROM customer_insight.temp.rfv_segmentation_kpis_final
;

COMMIT
;

------------------------------------------------------------------------------------------------------------------------

USE ROLE pipelinerunner
;

DROP TABLE dbt.bi_customer_insight.ci_rfv_customer_base_historical_weekly_20230606
;

DROP TABLE dbt.bi_customer_insight.ci_rfv_customer_base_historical_weekly_20230606_v2
;

DROP TABLE dbt.bi_customer_insight.ci_rfv_segments_historical_weekly_20230606
;

DROP TABLE dbt.bi_customer_insight.ci_rfv_segments_historical_weekly_20230606_v2
;

DROP TABLE dbt.bi_customer_insight__intermediate.ci_rfv_kpis_build_20230606
;

DROP TABLE dbt.bi_customer_insight__intermediate.ci_rfv_kpis_build_20230718
;

------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_rfv_kpis_bookings_spvs_20230812 CLONE dbt.bi_customer_insight.ci_rfv_kpis_bookings_spvs
;


BEGIN TRANSACTION
;

ALTER TABLE dbt.bi_customer_insight.ci_rfv_kpis_bookings_spvs
	DROP COLUMN
		total_bookings_1m,
    total_bookers_1m,
    total_margin_1m,
    total_revenue_1m
;

COMMIT
;


------------------------------------------------------------------------------------------------------------------------
USE ROLE pipelinerunner
;

CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_rfv_kpis_bookings_spvs_20230812 CLONE dbt.bi_customer_insight.ci_rfv_kpis_bookings_spvs
;


BEGIN TRANSACTION
;

CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_rfv_kpis_bookings_spvs AS
SELECT *
FROM customer_insight.temp.rfv_bookings_spvs_kpis_by_seg_update
;

COMMIT
;


------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_rfv_kpis_bookings_spvs_20230904 CLONE dbt.bi_customer_insight.ci_rfv_kpis_bookings_spvs
;


BEGIN TRANSACTION
;

CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_rfv_kpis_bookings_spvs AS
SELECT *
FROM customer_insight.temp.rfv_bookings_spvs_kpis_by_seg
;

COMMIT
;

------------------------------------------------------------------------------------------------------------------------
USE ROLE pipelinerunner
;

CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_rfv_kpis_bookings_spvs_20230905 CLONE dbt.bi_customer_insight.ci_rfv_kpis_bookings_spvs
;


BEGIN TRANSACTION
;

CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_rfv_kpis_bookings_spvs AS
SELECT *
FROM customer_insight.temp.rfv_bookings_spvs_kpis_by_seg_update_again
;

COMMIT
;

SELECT *
FROM dbt.information_schema.tables t
WHERE t.table_schema IN ('BI_CUSTOMER_INSIGHT', 'BI_CUSTOMER_INSIGHT__INTERMEDIATE')
;

------------------------------------------------------------------------------------------------------------------------
--
-- DROP TABLE dbt.BI_CUSTOMER_INSIGHT.CI_BASE_TEST;
-- DROP TABLE dbt.BI_CUSTOMER_INSIGHT.CI_RFV_CUSTOMER_BASE_EXPLORER_20230606;
-- DROP TABLE dbt.BI_CUSTOMER_INSIGHT.CI_RFV_CUSTOMER_BASE_EXPLORER_20230718;
-- DROP TABLE dbt.BI_CUSTOMER_INSIGHT.CI_RFV_CUSTOMER_BASE_EXPLORER_V3;
-- DROP TABLE dbt.BI_CUSTOMER_INSIGHT.CI_RFV_KPIS_20230606;
-- DROP TABLE dbt.BI_CUSTOMER_INSIGHT.CI_RFV_KPIS_20230718;
-- DROP TABLE dbt.BI_CUSTOMER_INSIGHT.CI_RFV_KPIS_20230816;
-- DROP TABLE dbt.BI_CUSTOMER_INSIGHT.CI_RFV_KPIS_BOOKINGS_SPVS_20230718;
-- DROP TABLE dbt.BI_CUSTOMER_INSIGHT.CI_RFV_KPIS_BOOKINGS_SPVS_20230812;
-- DROP TABLE dbt.BI_CUSTOMER_INSIGHT.CI_RFV_KPIS_BOOKINGS_SPVS_20230816;
-- DROP TABLE dbt.BI_CUSTOMER_INSIGHT.CI_RFV_KPIS_BOOKINGS_SPVS_20230904;
-- DROP TABLE dbt.BI_CUSTOMER_INSIGHT.CI_RFV_KPIS_BOOKINGS_SPVS_20230905;
-- DROP TABLE dbt.BI_CUSTOMER_INSIGHT.CI_RFV_KPIS_TEMP;
-- DROP TABLE dbt.BI_CUSTOMER_INSIGHT__INTERMEDIATE.CI_RFV_SEGMENT_MOVEMENT_20230718;


DROP TABLE dbt.bi_customer_insight.ci_base_test
;

DROP TABLE dbt.bi_customer_insight.ci_rfv_customer_base_explorer_20230606
;

DROP TABLE dbt.bi_customer_insight.ci_rfv_customer_base_explorer_20230718
;

DROP TABLE dbt.bi_customer_insight.ci_rfv_customer_base_explorer_v3
;

DROP TABLE dbt.bi_customer_insight.ci_rfv_kpis_20230606
;

DROP TABLE dbt.bi_customer_insight.ci_rfv_kpis_20230718
;

DROP TABLE dbt.bi_customer_insight.ci_rfv_kpis_20230816
;

DROP TABLE dbt.bi_customer_insight.ci_rfv_kpis_bookings_spvs_20230718
;

DROP TABLE dbt.bi_customer_insight.ci_rfv_kpis_bookings_spvs_20230812
;

DROP TABLE dbt.bi_customer_insight.ci_rfv_kpis_bookings_spvs_20230816
;

DROP TABLE dbt.bi_customer_insight.ci_rfv_kpis_bookings_spvs_20230904
;

DROP TABLE dbt.bi_customer_insight.ci_rfv_kpis_bookings_spvs_20230905
;

DROP TABLE dbt.bi_customer_insight.ci_rfv_kpis_temp
;

DROP TABLE dbt.bi_customer_insight__intermediate.ci_rfv_segment_movement_20230718
;

------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_rfv_kpis_bookings_spvs_20230905 CLONE dbt.bi_customer_insight.ci_rfv_kpis_bookings_spvs
;


BEGIN TRANSACTION
;

CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_rfv_kpis_bookings_spvs AS
SELECT *
FROM customer_insight.temp.rfv_bookings_spvs_kpis_by_seg_update_again
;

COMMIT
;


------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_rfv_kpis_bookings_spvs_20230905 CLONE dbt.bi_customer_insight.ci_rfv_kpis_bookings_spvs
;


BEGIN TRANSACTION
;

CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_rfv_kpis_bookings_spvs AS
SELECT *
FROM customer_insight.temp.rfv_bookings_spvs_kpis_by_seg_update_again
;

COMMIT
;

USE ROLE pipelinerunner
;
------------------------------------------------------------------------------------------------------------------------


CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_waus_by_channel_20230918 CLONE dbt.bi_customer_insight.ci_waus_by_channel
;

BEGIN TRANSACTION
;

INSERT INTO dbt.bi_customer_insight.ci_waus_by_channel
SELECT *
FROM customer_insight.temp.waus_by_channel
;

COMMIT
;


CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_bookings_by_channel_20230918 CLONE dbt.bi_customer_insight.ci_bookings_by_channel
;

BEGIN TRANSACTION
;

INSERT INTO dbt.bi_customer_insight.ci_bookings_by_channel
SELECT *
FROM customer_insight.temp.bookings_by_channel
;

COMMIT
;

DELETE
FROM dbt.bi_customer_insight.ci_bookings_by_channel
WHERE week_starting = ('2023-09-18')

------------------------------------------------------------------------------------------------------------------------
USE ROLE pipelinerunner
;

CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_rfv_kpis_bookings_spvs_20230925 CLONE dbt.bi_customer_insight.ci_rfv_kpis_bookings_spvs
;


BEGIN TRANSACTION
;

CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_rfv_kpis_bookings_spvs AS
SELECT *
FROM customer_insight.temp.rfv_bookings_spvs_kpis_by_seg_update_again
;

COMMIT
;

------------------------------------------------------------------------------------------------------------------------

// TABLE 1
USE ROLE pipelinerunner
;

CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_rfv_segments_historical_daily_20230929 CLONE dbt.bi_customer_insight.ci_rfv_segments_historical_daily
;


BEGIN TRANSACTION
;

INSERT INTO dbt.bi_customer_insight.ci_rfv_segments_historical_daily
SELECT *
FROM dbt_dev.dbt_georgieagnew_customer_insight__intermediate.ci_rfv_backill_segments_final
;

COMMIT
;

// TABLE 2

CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_rfv_customer_base_historical_daily_20230929 CLONE dbt.bi_customer_insight.ci_rfv_customer_base_historical_daily
;


BEGIN TRANSACTION
;

INSERT INTO dbt.bi_customer_insight.ci_rfv_customer_base_historical_daily
SELECT *
FROM dbt_dev.dbt_georgieagnew_customer_insight__intermediate.ci_rfv_backill_customer_base_breakdown_5_final
;

COMMIT
;

------------------------------------------------------------------------------------------------------------------------
-- 10th October 2023 - Updating historical RFV data
/*
Georgie:
Got a few RFV updates coming up over the next few weeks to build in the pillar segments, and the first step is just
updating some of the definitions for our inactive customers so that we have greater flexibility for reactivation / retention splits and campaigns.
 */

USE ROLE personal_role__dbt_prod
;

USE WAREHOUSE customer_insight_dbt_xlarge
;

// TABLE 1: weekly backfill
CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_rfv_segments_historical_weekly_pre_inactive_change_20231010 CLONE dbt.bi_customer_insight.ci_rfv_segments_historical_weekly
;

BEGIN TRANSACTION
;

UPDATE dbt.bi_customer_insight.ci_rfv_segments_historical_weekly t1 // and weekly
SET t1.rfv_segment  = new.rfv_segment,
	t1.segment_rank = new.segment_rank,
	t1.mega_segment = new.mega_segment
FROM (
	SELECT
		a.run_date
			,
		a.shiro_user_id
			,
		a.uuid
			,
		CASE
			WHEN b.days_since_last_session BETWEEN (30 * 6) AND (365) THEN 'Mid-Term Lapsed'
			WHEN b.days_since_last_session BETWEEN (366) AND (365 * 2) THEN 'Long-Term Lapsed'
			WHEN b.days_since_last_session BETWEEN (365 * 2) AND (365 * 4) THEN 'Dormant'
			WHEN b.days_since_last_session > (365 * 4) THEN 'Sunset'
			ELSE
				a.rfv_segment
		END AS rfv_segment
			,
		CASE
			WHEN a.rfv_segment = 'Coolers' THEN 12 //fixing old thing that was wrong before
			WHEN a.rfv_segment = 'Newly Lapsed' THEN 13 // then short term lapsed is 14
			WHEN b.days_since_last_session BETWEEN (30 * 6) AND (365) THEN 15
			WHEN b.days_since_last_session BETWEEN (366) AND (365 * 2) THEN 16
			WHEN b.days_since_last_session BETWEEN (365 * 2) AND (365 * 4) THEN 17
			WHEN b.days_since_last_session > (365 * 4) THEN 18
			ELSE
				a.segment_rank
		END AS segment_rank
			,
		a.lifecycle
			,
		CASE
			WHEN lifecycle = 'Early Life Active' THEN 'Early Life'
			WHEN rfv_segment IN ('Single Returners', 'Coolers') THEN 'Single Actives'
			WHEN a.lifecycle IN ('Mature Active Base') THEN 'Non-Single Actives'
			ELSE 'Inactive'
		END AS mega_segment
			,
		a.micro_segment
			,
		a.mau_flag
			,
		a.wau_flag
			,
		a.booker_history,
		a.margin_segment,
		a.session_frequency_agg,
		a.session_recency_agg
			,
		a.converted_l30
	FROM dbt.bi_customer_insight.ci_rfv_segments_historical_weekly_pre_inactive_change_20231010 a
		LEFT JOIN dbt.bi_customer_insight.ci_rfv_customer_base_historical_weekly b
				  ON a.shiro_user_id = b.shiro_user_id AND a.run_date = b.run_date
) new
WHERE new.shiro_user_id = t1.shiro_user_id AND new.run_date = t1.run_date
;

COMMIT
;


// TABLE 2: daily backfill
CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_rfv_segments_historical_daily_pre_inactive_change_20231010 CLONE dbt.bi_customer_insight.ci_rfv_segments_historical_daily
;

BEGIN TRANSACTION
;

UPDATE dbt.bi_customer_insight.ci_rfv_segments_historical_daily t1
SET t1.rfv_segment  = new.rfv_segment,
	t1.segment_rank = new.segment_rank,
	t1.mega_segment = new.mega_segment
FROM (
	SELECT
		a.run_date
			,
		a.shiro_user_id
			,
		a.uuid
			,
		CASE
			WHEN b.days_since_last_session BETWEEN (30 * 6) AND (365) THEN 'Mid-Term Lapsed'
			WHEN b.days_since_last_session BETWEEN (366) AND (365 * 2) THEN 'Long-Term Lapsed'
			WHEN b.days_since_last_session BETWEEN (365 * 2) AND (365 * 4) THEN 'Dormant'
			WHEN b.days_since_last_session > (365 * 4) THEN 'Sunset'
			ELSE
				a.rfv_segment
		END AS rfv_segment
			,
		CASE
			WHEN a.rfv_segment = 'Coolers' THEN 12 //fixing old thing that was wrong before
			WHEN a.rfv_segment = 'Newly Lapsed' THEN 13 // then short term lapsed is 14
			WHEN b.days_since_last_session BETWEEN (30 * 6) AND (365) THEN 15
			WHEN b.days_since_last_session BETWEEN (366) AND (365 * 2) THEN 16
			WHEN b.days_since_last_session BETWEEN (365 * 2) AND (365 * 4) THEN 17
			WHEN b.days_since_last_session > (365 * 4) THEN 18
			ELSE
				a.segment_rank
		END AS segment_rank
			,
		a.lifecycle
			,
		CASE
			WHEN lifecycle = 'Early Life Active' THEN 'Early Life'
			WHEN rfv_segment IN ('Single Returners', 'Coolers') THEN 'Single Actives'
			WHEN a.lifecycle IN ('Mature Active Base') THEN 'Non-Single Actives'
			ELSE 'Inactive'
		END AS mega_segment
			,
		a.micro_segment
			,
		a.mau_flag
			,
		a.wau_flag
			,
		a.booker_history,
		a.margin_segment,
		a.session_frequency_agg,
		a.session_recency_agg
			,
		a.converted_l30
	FROM dbt.bi_customer_insight.ci_rfv_segments_historical_daily_pre_inactive_change_20231010 a
		LEFT JOIN dbt.bi_customer_insight.ci_rfv_customer_base_historical_daily b
				  ON a.shiro_user_id = b.shiro_user_id AND a.run_date = b.run_date
) new
WHERE new.shiro_user_id = t1.shiro_user_id AND new.run_date = t1.run_date
;

COMMIT
;

//fixing inactive old  - TABLE 1 WEEKLY
CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_rfv_segments_historical_weekly_post_change_pre_fix20231010 CLONE dbt.bi_customer_insight.ci_rfv_segments_historical_weekly
;

BEGIN TRANSACTION
;

UPDATE dbt.bi_customer_insight.ci_rfv_segments_historical_weekly t1 // and weekly
SET t1.rfv_segment  = new.rfv_segment,
	t1.segment_rank = new.segment_rank
FROM (
	WITH
		rfv_segment_update AS (
			SELECT
				a.shiro_user_id
					,
				a.run_date
					,
				CASE // fixing stiching issue
					WHEN a.lifecycle <> 'Inactive' AND a.rfv_segment IN
													   ('Mid-Term Lapsed', 'Long-Term Lapsed', 'Dormant',
														'Sunset') // when not meant to be inactive but given new seg, give them old segment back. else stick to new
						THEN b.rfv_segment
					ELSE a.rfv_segment
				END AS rfv_segment
			FROM dbt.bi_customer_insight.ci_rfv_segments_historical_weekly_post_change_pre_fix20231010 a
				LEFT JOIN dbt.bi_customer_insight.ci_rfv_segments_historical_weekly_pre_inactive_change_20231010 b
						  ON a.shiro_user_id = b.shiro_user_id AND a.run_date = b.run_date
		)
	SELECT *
			,
		   CASE
			   WHEN rfv_segment = 'Signup Singles' THEN 1
			   WHEN rfv_segment = 'Young Browsers' THEN 2
			   WHEN rfv_segment = 'Young Bookers' THEN 3
			   WHEN rfv_segment = 'Young Cancellers' THEN 4
			   WHEN rfv_segment = 'Single Returners' THEN 5
			   WHEN rfv_segment = 'Cold Prospects' THEN 6
			   WHEN rfv_segment = 'Warm Prospects' THEN 7
			   WHEN rfv_segment = 'Hot Prospects' THEN 8
			   WHEN rfv_segment = 'Superhot Prospects' THEN 9
			   WHEN rfv_segment = 'Recent Bookers' THEN 10
			   WHEN rfv_segment = 'Recent Cancellers' THEN 11
			   WHEN rfv_segment = 'Coolers' THEN 12
			   WHEN rfv_segment = 'Newly Lapsed' THEN 13
			   WHEN rfv_segment = 'Short-Term Lapsed' THEN 14
			   WHEN rfv_segment = 'Mid-Term Lapsed' THEN 15
			   WHEN rfv_segment = 'Long-Term Lapsed' THEN 16
			   WHEN rfv_segment = 'Dormant' THEN 17
			   WHEN rfv_segment = 'Sunset' THEN 18
		   END AS segment_rank
	FROM rfv_segment_update
) new
WHERE new.shiro_user_id = t1.shiro_user_id AND new.run_date = t1.run_date
;

COMMIT
;

------------------------------------------------------------------------------------------------------------------------
-- 11th October 2023
-- Hey Robin - Theres something going on with a rogue week in May that needs smoothing over. Could you run this ?

USE ROLE personal_role__dbt_prod
;

USE WAREHOUSE customer_insight_dbt_xlarge
;

CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_rfv_segments_historical_weekly_pre_rogue_week_fix_20231011 CLONE dbt.bi_customer_insight.ci_rfv_segments_historical_weekly
;

BEGIN TRANSACTION
;

DELETE
FROM dbt.bi_customer_insight.ci_rfv_segments_historical_weekly
WHERE lifecycle = 'Inactive'
;

INSERT INTO dbt.bi_customer_insight.ci_rfv_segments_historical_weekly (
	SELECT *
	FROM customer_insight.temp.ga_fixing_rfv_test
)
;

COMMIT
;

------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_rfv_segments_historical_weekly_pre_rogue_week_fix_v2_20231011 CLONE dbt.bi_customer_insight.ci_rfv_segments_historical_weekly
;

BEGIN TRANSACTION
;

DELETE
FROM dbt.bi_customer_insight.ci_rfv_segments_historical_weekly
WHERE lifecycle = 'Inactive' AND run_date = ('2023-05-28')
;

COMMIT
;

BEGIN TRANSACTION
;

INSERT INTO dbt.bi_customer_insight.ci_rfv_segments_historical_weekly (
	SELECT *
	FROM customer_insight.temp.ga_fixing_rfv_test
)
;

COMMIT
;


CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_rfv_segments_historical_weekly_pre_rogue_week_fix_v4_20231011 CLONE dbt.bi_customer_insight.ci_rfv_segments_historical_weekly
;

BEGIN TRANSACTION
;

INSERT INTO dbt.bi_customer_insight.ci_rfv_segments_historical_weekly (
	SELECT *
	FROM dbt.bi_customer_insight.ci_rfv_segments_historical_weekly_pre_rogue_week_fix_20231011
	WHERE lifecycle = 'Inactive' AND run_date <> ('2023-05-28')
)
;

COMMIT
;

CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_rfv_segments_historical_weekly_pre_rogue_week_fix_v5_20231011 CLONE dbt.bi_customer_insight.ci_rfv_segments_historical_weekly
;

BEGIN TRANSACTION
;

DELETE
FROM dbt.bi_customer_insight.ci_rfv_segments_historical_weekly
WHERE run_date = ('2023-05-28')
;

COMMIT
;


BEGIN TRANSACTION
;

INSERT INTO dbt.bi_customer_insight.ci_rfv_segments_historical_weekly
	(
		SELECT *
		FROM dbt_dev.dbt_georgieagnew_customer_insight__intermediate.ci_rfv_backill_segments_final
		WHERE run_date = ('2023-05-28')
	)
;

COMMIT
;

CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_rfv_customerbase_historical_weekly_pre_rogue_week_fix_v5_20231011 CLONE dbt.bi_customer_insight.ci_rfv_customer_base_historical_weekly
;

BEGIN TRANSACTION
;

DELETE
FROM dbt.bi_customer_insight.ci_rfv_customer_base_historical_weekly
WHERE run_date = ('2023-05-28')
;

COMMIT
;


BEGIN TRANSACTION
;

INSERT INTO dbt.bi_customer_insight.ci_rfv_customer_base_historical_weekly
	(
		SELECT *
		FROM dbt_dev.dbt_georgieagnew_customer_insight__intermediate.ci_rfv_backill_customer_base_breakdown_5_final
		WHERE run_date = ('2023-05-28')
	)
;

COMMIT
;

------------------------------------------------------------------------------------------------------------------------
-- 12th October 2023

USE ROLE personal_role__dbt_prod
;

USE WAREHOUSE customer_insight_dbt_xlarge
;


CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_rfv_customer_base_explorer_pre_seg_change_20231012 CLONE dbt.bi_customer_insight.ci_rfv_customer_base_explorer
;

BEGIN TRANSACTION
;


CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_rfv_customer_base_explorer
AS
SELECT *
FROM customer_insight.temp.rfv_segmentation_kpis_base1
;

COMMIT
;

CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight__intermediate.ci_rfv_kpis_build_pre_seg_change_20231012 CLONE dbt.bi_customer_insight__intermediate.ci_rfv_kpis_build
;

BEGIN TRANSACTION
;

CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight__intermediate.ci_rfv_kpis_build
AS
SELECT *
FROM customer_insight.temp.rfv_segmentation_kpis_base2
;

COMMIT
;


CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_rfv_kpis_pre_seg_change_20231012 CLONE dbt.bi_customer_insight.ci_rfv_kpis
;

BEGIN TRANSACTION
;

USE ROLE personal_role__dbt_prod
;

CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_rfv_kpis
AS
SELECT *
FROM customer_insight.temp.rfv_segmentation_kpis_final
;

COMMIT
;

------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_rfv_kpis_bookings_spvs_pre_seg_change_20231012 CLONE dbt.bi_customer_insight.ci_rfv_kpis_bookings_spvs
;

BEGIN TRANSACTION
;

USE ROLE pipelinerunner
;

DROP TABLE dbt.bi_customer_insight.ci_rfv_kpis_bookings_spvs
;

USE ROLE personal_role__dbt_prod
;

USE WAREHOUSE customer_insight_dbt_xlarge
;

CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_rfv_kpis_bookings_spvs
AS
SELECT *
FROM customer_insight.temp.rfv_bookings_spvs_kpis_by_seg_update_again
;

COMMIT
;


CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_rfv_kpis_mix_mag_pre_seg_change_20231012 CLONE dbt.bi_customer_insight.ci_rfv_kpis_mix_mag
;

BEGIN TRANSACTION
;

CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_rfv_kpis_mix_mag
AS
SELECT *
FROM customer_insight.temp.rfv_kpis_mix_mag_spvs_2
;

COMMIT
;

CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight__intermediate.ci_rfv_segment_movement_pre_seg_change_20231012 CLONE dbt.bi_customer_insight__intermediate.ci_rfv_segment_movement
;


BEGIN TRANSACTION
;

USE ROLE pipelinerunner
;

DROP TABLE dbt.bi_customer_insight__intermediate.ci_rfv_segment_movement
;

USE ROLE personal_role__dbt_prod
;

USE WAREHOUSE customer_insight_dbt_xlarge
;

SELECT *
FROM dbt.bi_customer_insight__intermediate.ci_rfv_segment_movement
;

CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight__intermediate.ci_rfv_segment_movement
AS
SELECT *
FROM customer_insight.temp.rfv_segment_movements
;

COMMIT
;

CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_rfv_customer_base_explorer_metrics_pre_seg_change_20231012 CLONE dbt.bi_customer_insight.ci_rfv_customer_base_explorer_metrics

BEGIN TRANSACTION
;

CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_rfv_customer_base_explorer_metrics
AS
SELECT *
FROM customer_insight.temp.rfv_bookings_spvs_kpis_by_seg_v2
;

COMMIT
;

------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_rfv_kpis_bookings_spvs_pre_seg_change__v2_20231012 CLONE dbt.bi_customer_insight.ci_rfv_kpis_bookings_spvs
;

BEGIN TRANSACTION
;

DELETE
FROM dbt.bi_customer_insight.ci_rfv_kpis_bookings_spvs
WHERE week_start_date = ('2023-10-09')

COMMIT
;

CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_rfv_kpis_mix_mag_pre_seg_change_v2_20231012 CLONE dbt.bi_customer_insight.ci_rfv_kpis_mix_mag
;

BEGIN TRANSACTION
;

DELETE
FROM dbt.bi_customer_insight.ci_rfv_kpis_mix_mag
WHERE week_start = ('2023-10-09')

COMMIT
;


CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_rfv_customer_base_explorer_metrics_pre_seg_change__v2_20231012 CLONE dbt.bi_customer_insight.ci_rfv_customer_base_explorer_metrics

BEGIN TRANSACTION
;

DELETE
FROM dbt.bi_customer_insight.ci_rfv_customer_base_explorer_metrics
WHERE week_start_date = ('2023-10-09')

COMMIT
;

------------------------------------------------------------------------------------------------------------------------
-- 17th October 2023
USE ROLE personal_role__dbt_prod
;

USE WAREHOUSE customer_insight_dbt_xlarge
;

CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_rfv_customer_base_historical_weekly_20231017 CLONE dbt.bi_customer_insight.ci_rfv_customer_base_historical_weekly
;

BEGIN TRANSACTION
;

INSERT INTO dbt.bi_customer_insight.ci_rfv_customer_base_historical_weekly
SELECT *
FROM dbt_dev.dbt_georgieagnew_customer_insight__intermediate.ci_rfv_backill_customer_base_breakdown_5_final
WHERE run_date BETWEEN ('2018-04-01') AND ('2018-12-29')
;

COMMIT
;



CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_rfv_segments_historical_weekly_20231017 CLONE dbt.bi_customer_insight.ci_rfv_segments_historical_weekly
;

BEGIN TRANSACTION
;

INSERT INTO dbt.bi_customer_insight.ci_rfv_segments_historical_weekly
SELECT *
FROM dbt_dev.dbt_georgieagnew_customer_insight__intermediate.ci_rfv_backill_segments_final
WHERE run_date BETWEEN ('2018-04-01') AND ('2018-12-29')
;

COMMIT
;


CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_rfv_customer_base_historical_weekly_v2_20231017 CLONE dbt.bi_customer_insight.ci_rfv_customer_base_historical_weekly
;

BEGIN TRANSACTION
;

DELETE
FROM dbt.bi_customer_insight.ci_rfv_customer_base_historical_weekly
WHERE run_date < ('2018-12-31')
;

BEGIN TRANSACTION
;

INSERT INTO dbt.bi_customer_insight.ci_rfv_customer_base_historical_weekly
SELECT *
FROM dbt_dev.dbt_georgieagnew_customer_insight__intermediate.ci_rfv_backill_customer_base_breakdown_5_final
WHERE run_date BETWEEN ('2018-04-01') AND ('2018-12-31')
;

COMMIT
;

------------------------------------------------------------------------------------------------------------------------
USE ROLE pipelinerunner
;

CREATE OR REPLACE TRANSIENT TABLE customer_insight.temp.ga_tr_acqui_cust20231113 CLONE customer_insight.temp.ga_tr_acqui_cust

CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_acqui_cust20231113 CLONE dbt.bi_customer_insight.ci_acqui_cust
;


;

BEGIN TRANSACTION
;

INSERT INTO dbt.bi_customer_insight.ci_acqui_cust
SELECT *
FROM customer_insight.temp.ga_tr_acqui_cust
;

COMMIT

------------------------------------------------------------------------------------------------------------------------
USE ROLE personal_role__dbt_prod
;

CREATE OR REPLACE TRANSIENT TABLE customer_insight.temp.ga_tr_acqui_cust20231114 CLONE customer_insight.temp.ga_tr_acqui_cust

CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_acqui_cust20231114 CLONE dbt.bi_customer_insight.ci_acqui_cust


;

USE WAREHOUSE customer_insight_dbt_large

BEGIN TRANSACTION
;

CREATE OR REPLACE TABLE dbt.bi_customer_insight.ci_acqui_cust AS
SELECT *
FROM customer_insight.temp.ga_tr_acqui_cust
;

COMMIT
;

------------------------------------------------------------------------------------------------------------------------

USE ROLE pipelinerunner
;

USE WAREHOUSE customer_insight_dbt_large
;
// TABLE 1

CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight__intermediate.ci_ga_acquisitions_base20231120 CLONE dbt.bi_customer_insight__intermediate.ci_ga_acquisitions_base
;

BEGIN TRANSACTION
;

INSERT INTO dbt.bi_customer_insight__intermediate.ci_ga_acquisitions_base
SELECT *
FROM customer_insight.temp.ga_rfv_acquisitions
WHERE signup_date < ('2023-11-05')
;

COMMIT
;

// TABLE 2


CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight__intermediate.ci_ga_acquisitions_1m20231120 CLONE dbt.bi_customer_insight__intermediate.ci_ga_acquisitions_1m
;

BEGIN TRANSACTION
;

INSERT INTO dbt.bi_customer_insight__intermediate.ci_ga_acquisitions_1m
SELECT *
FROM customer_insight.temp.ga_rfv_acquisitions_1m
;

COMMIT
;


// TABLE 3


CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight__intermediate.ci_ga_acquisitions_3m20231120 CLONE dbt.bi_customer_insight__intermediate.ci_ga_acquisitions_3m
;

BEGIN TRANSACTION
;

INSERT INTO dbt.bi_customer_insight__intermediate.ci_ga_acquisitions_3m
SELECT *
FROM customer_insight.temp.ga_rfv_acquisitions_3m
;

COMMIT
;

// TABLE 4


CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight__intermediate.ci_ga_acquisitions_6m20231120 CLONE dbt.bi_customer_insight__intermediate.ci_ga_acquisitions_6m
;

BEGIN TRANSACTION
;

INSERT INTO dbt.bi_customer_insight__intermediate.ci_ga_acquisitions_6m
SELECT *
FROM customer_insight.temp.ga_rfv_acquisitions_6m
;

COMMIT
;

------------------------------------------------------------------------------------------------------------------------

// TABLE 1

CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight__intermediate.ci_ga_reactivations_base20231120 CLONE dbt.bi_customer_insight__intermediate.ci_ga_reactivations_base
;

BEGIN TRANSACTION
;

INSERT INTO dbt.bi_customer_insight__intermediate.ci_ga_reactivations_base
SELECT *
FROM customer_insight.temp.ga_rfv_reactivators_base
;

COMMIT
;

// TABLE 2
CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight__intermediate.ci_ga_reactivations_1m20231120 CLONE dbt.bi_customer_insight__intermediate.ci_ga_reactivations_1m
;

BEGIN TRANSACTION
;

INSERT INTO dbt.bi_customer_insight__intermediate.ci_ga_reactivations_1m
SELECT *
FROM customer_insight.temp.ga_rfv_reactivators_kpis_1m
;

COMMIT
;


// TABLE 3
CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight__intermediate.ci_ga_reactivations_3m20231120 CLONE dbt.bi_customer_insight__intermediate.ci_ga_reactivations_3m
;

BEGIN TRANSACTION
;

INSERT INTO dbt.bi_customer_insight__intermediate.ci_ga_reactivations_3m
SELECT *
FROM customer_insight.temp.ga_rfv_reactivators_kpis_3m
;

COMMIT
;


// TABLE 4

CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight__intermediate.ci_ga_reactivations_6m20231120 CLONE dbt.bi_customer_insight__intermediate.ci_ga_reactivations_6m
;

BEGIN TRANSACTION
;

INSERT INTO dbt.bi_customer_insight__intermediate.ci_ga_reactivations_6m
SELECT *
FROM customer_insight.temp.ga_rfv_reactivators_kpis_6m
;

COMMIT
;


BEGIN TRANSACTION
;

DELETE
FROM dbt.bi_customer_insight__intermediate.ci_ga_reactivations_base
WHERE reactivation_week = ('2023-11-13')

COMMIT
;

------------------------------------------------------------------------------------------------------------------------

BEGIN TRANSACTION
;

DELETE
FROM dbt.bi_customer_insight__intermediate.ci_ga_reactivations_1m
WHERE reactivation_week > ('2023-10-01')

COMMIT
;

BEGIN TRANSACTION
;

INSERT INTO dbt.bi_customer_insight__intermediate.ci_ga_reactivations_1m
SELECT *
FROM customer_insight.temp.ga_rfv_reactivators_kpis_1m
WHERE reactivation_week = ('2023-10-02')

COMMIT
;

// TABLE 1
BEGIN TRANSACTION
;

DELETE
FROM dbt.bi_customer_insight__intermediate.ci_ga_reactivations_3m
WHERE reactivation_week > ('2023-08-06')

COMMIT
;

BEGIN TRANSACTION
;

INSERT INTO dbt.bi_customer_insight__intermediate.ci_ga_reactivations_3m
SELECT *
FROM customer_insight.temp.ga_rfv_reactivators_kpis_3m
WHERE reactivation_week = ('2023-08-07')

COMMIT
;

BEGIN TRANSACTION
;

DELETE
FROM dbt.bi_customer_insight__intermediate.ci_ga_reactivations_6m
WHERE reactivation_week > ('2023-05-22')

COMMIT
;

------------------------------------------------------------------------------------------------------------------------
USE ROLE personal_role__dbt_prod
;

CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight__intermediate.ci_ga_acquisitions_6m20231130 CLONE dbt.bi_customer_insight__intermediate.ci_ga_acquisitions_6m
;

CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight__intermediate.ci_ga_reactivations_6m20231130 CLONE dbt.bi_customer_insight__intermediate.ci_ga_reactivations_6m
;


USE WAREHOUSE customer_insight_dbt_medium

BEGIN TRANSACTION
;

INSERT INTO dbt.bi_customer_insight__intermediate.ci_ga_acquisitions_6m
SELECT *
FROM customer_insight.temp.ga_rfv_acquisitions_6m
WHERE DATE_TRUNC(WEEK, signup_date) IN ('2023-05-08', '2023-05-01', '2023-04-24', '2023-04-17')

COMMIT
;

BEGIN TRANSACTION
;

DELETE
FROM dbt.bi_customer_insight__intermediate.ci_ga_reactivations_6m
WHERE reactivation_week IN ('2023-05-22')

COMMIT
;

BEGIN TRANSACTION
;

INSERT INTO dbt.bi_customer_insight__intermediate.ci_ga_reactivations_6m
SELECT *
FROM customer_insight.temp.ga_rfv_reactivators_kpis_6m
WHERE reactivation_week IN ('2023-05-22', '2023-05-08', '2023-05-01', '2023-04-24', '2023-04-17', '2023-04-10')

COMMIT
;

------------------------------------------------------------------------------------------------------------------------
USE ROLE personal_role__dbt_prod
;

CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight__intermediate.ci_ga_acquisitions_base0231214 CLONE dbt.bi_customer_insight__intermediate.ci_ga_acquisitions_base
;

CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_acqui_cust20231214 CLONE dbt.bi_customer_insight.ci_acqui_cust
;

USE WAREHOUSE customer_insight_dbt_xlarge
;

BEGIN TRANSACTION
;

DELETE
FROM dbt.bi_customer_insight.ci_acqui_cust
WHERE signup_date > CURRENT_DATE()

COMMIT
;

BEGIN TRANSACTION
;

DELETE
FROM dbt.bi_customer_insight__intermediate.ci_ga_acquisitions_base
WHERE signup_date > CURRENT_DATE()

COMMIT
;

BEGIN TRANSACTION
;

CREATE OR REPLACE TABLE dbt.bi_customer_insight.ci_acqui_cust AS
SELECT
	a.shiro_user_id
		,
	a.signup_date
		,
	a.acquisition_channel_agg
		,
	a.acquisition_channel
		,
	b.email_opt_in_when_join
		,
	a.membership_account_status
		,
	a.current_affiliate_brand
		,
	a.territory_grouped
		,
	a.does_session_on_signup
		,
	a.sessions_on_signup_day
		,
	a.email_opt_in_1m_later
		,
	a.rfv_segment_1m
		,
	a.mega_segment_1m
		,
	a.mau_flag_1m
		,
	a.wau_flag_1m
		,
	a.bookings_1m
		,
	a.converted_1m
		,
	a.margin_1m
		,
	a.email_opt_in_3m_later
		,
	a.rfv_segment_3m
		,
	a.mega_segment_3m
		,
	a.mau_flag_3m
		,
	a.wau_flag_3m
		,
	a.bookings_3m
		,
	a.converted_3m
		,
	a.margin_3m
		,
	a.email_opt_in_6m_later
		,
	a.rfv_segment_6m
		,
	a.mega_segment_6m
		,
	a.mau_flag_6m
		,
	a.wau_flag_6m
		,
	a.bookings_6m
		,
	a.converted_6m
		,
	a.margin_6m
FROM dbt.bi_customer_insight.ci_acqui_cust a
	LEFT JOIN customer_insight.temp.ga_rfv_acquisitions b ON a.shiro_user_id = b.shiro_user_id
;

COMMIT
;

------------------------------------------------------------------------------------------------------------------------
USE ROLE personal_role__dbt_prod
;

CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight__intermediate.ci_mau_retention_1m20231227 CLONE dbt.bi_customer_insight__intermediate.ci_mau_retention_1m
;

USE WAREHOUSE customer_insight_dbt_large
;

BEGIN TRANSACTION
;

INSERT INTO dbt.bi_customer_insight__intermediate.ci_mau_retention_1m
SELECT *
FROM customer_insight.temp.ga_mau_retention_1m
WHERE week_starting = ('2023-10-30')

COMMIT
;
------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight__intermediate.ci_ga_acquisitions_base20231228 CLONE dbt.bi_customer_insight__intermediate.ci_ga_acquisitions_base
;

BEGIN TRANSACTION
;

CREATE OR REPLACE TABLE dbt.bi_customer_insight__intermediate.ci_ga_acquisitions_base AS
SELECT
	a.shiro_user_id
		,
	a.signup_date
		,
	a.acquisition_channel_agg
		,
	a.acquisition_channel
		,
	COALESCE(a.email_opt_in_when_join, b.email_opt_in_when_join) AS email_opt_in_when_join
		,
	a.membership_account_status
		,
	a.current_affiliate_brand
		,
	a.territory_grouped
		,
	a.does_session_on_signup
		,
	a.sessions_on_signup_day
FROM dbt.bi_customer_insight__intermediate.ci_ga_acquisitions_base a
	LEFT JOIN customer_insight.temp.ga_rfv_acquisitions b ON a.shiro_user_id = b.shiro_user_id
;

COMMIT
;

SELECT
	stba.stitched_identity_type,
	COUNT(*)
FROM se.data.scv_touch_basic_attributes stba
WHERE stba.touch_start_tstamp >= CURRENT_DATE - 30
GROUP BY 1
;


SELECT *
FROM se.data.scv_touch_basic_attributes stba
WHERE stba.touch_start_tstamp >= CURRENT_DATE - 30
  AND stba.stitched_identity_type = 'booking_id'
;


SELECT
	sc.date_value,
	sc.se_week,
	year,
	sc.se_year
FROM se.data.se_calendar sc
WHERE sc.date_value BETWEEN CURRENT_DATE - 7 AND CURRENT_DATE
;


SELECT *
FROM collab.muse.warehouse_metering wm
;

SELECT GET_DDL('table', 'collab.muse.warehouse_metering')
;


CREATE OR REPLACE VIEW warehouse_metering
			(
			 credits_used,
			 cost,
			 credits_used_cloud_services,
			 credits_used_compute,
			 end_time,
			 start_time,
			 warehouse_id,
			 warehouse_name,
			 warehouse_group
				)
AS
(
SELECT
	wmh.credits_used,
	wmh.credits_used * 2.08 AS cost,
	wmh.credits_used_cloud_services,
	wmh.credits_used_compute,
	wmh.end_time,
	wmh.start_time,
	wmh.warehouse_id,
	wmh.warehouse_name,
	CASE
		WHEN wmh.warehouse_name LIKE 'DATA_SCIENCE%' THEN 'DATA_SCIENCE'
		WHEN wmh.warehouse_name LIKE 'MARKETING_PIPE%' THEN 'MARKETING'
		WHEN wmh.warehouse_name = 'SNOWPLOW_WH' THEN 'SNOWPLOW'
		WHEN wmh.warehouse_name LIKE '%DBT%' THEN 'DBT'
		WHEN wmh.warehouse_name LIKE 'CUSTOMER_INSIGHT%'
			THEN 'CUSTOMER_INSIGHT' --note below DBT because CI have dbt warehouses
		WHEN wmh.warehouse_name LIKE 'SCV_%' THEN 'SINGLE_CUSTOMER_VIEW'
		WHEN wmh.warehouse_name LIKE 'TABLEAU%' THEN 'TABLEAU'
		WHEN wmh.warehouse_name
			IN ('PIPE_4XLARGE',
				'PIPE_2XLARGE',
				'PIPE_XLARGE',
				'PIPE_LARGE',
				'PIPE_MEDIUM') THEN 'DATA_PLATFORM_MODELLING'
		WHEN wmh.warehouse_name LIKE 'PIPE_HYGIENE%' THEN 'DATA_PLATFORM_HYGIENE'
		WHEN wmh.warehouse_name = 'PIPE_XSMALL' THEN 'DATA_PLATFORM_DEFAULT'
		WHEN wmh.warehouse_name = 'PIPE_DEFAULT' THEN 'DATA_PLATFORM_DEFAULT'
		WHEN wmh.warehouse_name = 'CLOUD_SERVICES_ONLY' THEN 'CLOUD_SERVICES_ONLY'
		ELSE 'DATA_PLATFORM_OTHERS'
	END                     AS warehouse_group
FROM snowflake.account_usage.warehouse_metering_history wmh
	)
;

SELECT *
FROM latest_vault.hotjar.survey_responses_browse srb
;


------------------------------------------------------------------------------------------------------------------------

USE ROLE personal_role__dbt_prod
//table 1
CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_rfv_kpis_20240215 CLONE dbt.bi_customer_insight.ci_rfv_kpis
;

USE WAREHOUSE customer_insight_dbt_2xlarge
;

SELECT *
FROM customer_insight.temp.rfv_segmentation_kpis_final_add_bh_with_removals_pre_tw
;

BEGIN TRANSACTION
;

CREATE OR REPLACE TABLE dbt.bi_customer_insight.ci_rfv_kpis
AS
SELECT *
FROM customer_insight.temp.rfv_segmentation_kpis_final_add_bh_with_removals_pre_tw
;

COMMIT
;


//table 2
CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight__intermediate.ci_rfv_kpis_build_20240215 CLONE dbt.bi_customer_insight__intermediate.ci_rfv_kpis_build
;

SELECT *
FROM customer_insight.temp.rfv_segmentation_kpis_base_adding_bh
;

BEGIN TRANSACTION
;

CREATE OR REPLACE TABLE dbt.bi_customer_insight.ci_rfv_kpis
AS
SELECT *
FROM customer_insight.temp.rfv_segmentation_kpis_base_adding_bh
;

COMMIT
;

------------------------------------------------------------------------------------------------------------------------

//table 1
CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_rfv_kpis_20240215 CLONE dbt.bi_customer_insight.ci_rfv_kpis
;

SELECT *
FROM customer_insight.temp.rfv_segmentation_kpis_final_add_bh_with_removals_pre_tw
;

BEGIN TRANSACTION
;

CREATE OR REPLACE TABLE dbt.bi_customer_insight.ci_rfv_kpis
AS
SELECT *
FROM customer_insight.temp.rfv_segmentation_kpis_final_add_bh_with_removals_pre_tw
;

COMMIT
;


//table 2
CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight__intermediate.ci_rfv_kpis_build_20240215 CLONE dbt.bi_customer_insight__intermediate.ci_rfv_kpis_build
;

SELECT *
FROM customer_insight.temp.rfv_segmentation_kpis_base_adding_bh
;

BEGIN TRANSACTION
;

CREATE OR REPLACE TABLE dbt.bi_customer_insight__intermediate.ci_rfv_kpis_build AS
SELECT *
FROM customer_insight.temp.rfv_segmentation_kpis_base_adding_bh
;

COMMIT
;

------------------------------------------------------------------------------------------------------------------------
USE ROLE personal_role__dbt_prod
;

CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_waus_by_channel20240221 CLONE dbt.bi_customer_insight.ci_waus_by_channel
;

USE WAREHOUSE customer_insight_dbt_large
;

BEGIN TRANSACTION
;

CREATE OR REPLACE TABLE dbt.bi_customer_insight.ci_waus_by_channel AS
SELECT *
FROM customer_insight.temp.waus_by_channel
;

COMMIT
;

------------------------------------------------------------------------------------------------------------------------
USE ROLE personal_role__dbt_prod
;

USE WAREHOUSE customer_insight_dbt_large
;

CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_rfv_segments_historical_daily20240221 CLONE dbt.bi_customer_insight.ci_rfv_segments_historical_daily
;

BEGIN TRANSACTION
;

INSERT INTO dbt.bi_customer_insight.ci_rfv_segments_historical_daily
SELECT *
FROM dbt_dev.dbt_georgieagnew_customer_insight__intermediate.ci_rfv_backill_segments_final
WHERE run_date = ('2024-02-23')

COMMIT
;

CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_rfv_customer_base_historical_daily20240221 CLONE dbt.bi_customer_insight.ci_rfv_customer_base_historical_daily
;

BEGIN TRANSACTION
;

INSERT INTO dbt.bi_customer_insight.ci_rfv_customer_base_historical_daily
SELECT *
FROM dbt_dev.dbt_georgieagnew_customer_insight__intermediate.ci_rfv_backill_customer_base_breakdown_5_final
WHERE run_date = ('2024-02-23')

COMMIT
;

DELETE
FROM dbt.bi_customer_insight.ci_waus_by_channel
WHERE week_start >= ('2024-02-19')
;

SELECT *
FROM se.data.email_performance ep
;

------------------------------------------------------------------------------------------------------------------------

INSERT INTO dbt.bi_customer_insight__intermediate.ci_waus_by_pillar_base
SELECT
	week_starting,
	year,
	week,
	pillar_segment,
	territory,
	CONCAT(week_starting, pillar_segment, territory) AS uuid,
	total_members
FROM dbt.bi_customer_insight.ci_waus_by_pillar
;

USE ROLE personal_role__dbt_prod
;

USE WAREHOUSE customer_insight_dbt_large
;

CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_waus_by_channel_20240405 CLONE dbt.bi_customer_insight.ci_waus_by_channel
;

BEGIN TRANSACTION
;

CREATE OR REPLACE TABLE dbt.bi_customer_insight.ci_waus_by_channel AS

SELECT *
FROM customer_insight.temp.waus_by_channel_updated_with_pillar_grouped_by_agg
;

COMMIT
;


BEGIN TRANSACTION
;
create or replace table dbt.bi_customer_insight.ci_waus_by_channel as

    select *
from   customer_insight.temp.waus_by_channel_updated_with_pillar_grouped_by_agg
COMMIT
;

USE ROLE personal_role__dbt_prod;
USE warehouse customer_insight_dbt_large;
CREATE OR REPLACE TRANSIENT TABLE  dbt.bi_customer_insight.ci_pillar_segments_20240408 CLONE dbt.bi_customer_insight.ci_pillar_segments
;

DELETE
FROM dbt.bi_customer_insight.ci_pillar_segments AS target
	USING (
		 SELECT *
		 FROM se.data_pii.se_user_attributes
		 WHERE current_affiliate_territory = 'IE'
	 ) AS batch
WHERE target.shiro_user_id = batch.shiro_user_id
  AND target.run_date IN ('2024-04-07')


DELETE
FROM dbt.bi_customer_insight.ci_pillar_segments_historical_weekly AS target
	USING (
		 SELECT *
		 FROM se.data_pii.se_user_attributes
		 WHERE current_affiliate_territory = 'IE'
	 ) AS batch
WHERE target.shiro_user_id = batch.shiro_user_id
  AND target.run_date IN ('2024-04-07');

select * from se.data.scv_touched_spvs sts where sts.event_category = 'screen views';

SELECT MAX(collector_tstamp) FROm snowplow.atomic.events

;
------------------------------------------------------------------------------------------------------------------------

Use role personal_role__dbt_prod;
CREATE OR REPLACE TRANSIENT TABLE dbt.bi_customer_insight.ci_pillar_segments_historical_weekly_20240507 CLONE dbt.bi_customer_insight.ci_pillar_segments_historical_weekly;
USE WAREHOUSE customer_insight_dbt_medium;

DELETE
FROM dbt.bi_customer_insight.ci_pillar_segments_historical_weekly AS target
   USING (
       SELECT *
       FROM se.data_pii.se_user_attributes
       WHERE current_affiliate_territory = 'IE'
    ) AS batch
WHERE target.shiro_user_id = batch.shiro_user_id
  AND target.run_date IN ('2024-05-05');