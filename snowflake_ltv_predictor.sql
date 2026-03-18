/*

To build a high-performing LTV model for a curated travel brand like Secret Escapes, your training set needs to be
structured around the "Snapshot" approach.

You essentially want to show the model a "Picture of a Member" at a specific
point in the past, and tell it how much that member spent in the following 12 months.

1. The Dataset Structure
Your LTV_TRAINING_SET should be a "wide" table where each row is a Member, and the columns are their behavior up to a
"Snapshot Date.


"Column Category	Feature Name			Description
Identity			MEMBER_ID				Unique identifier.
Target (Label)		ACTUAL_NEXT_12M_VALUE	What you're trying to predict. Total spend in the 365 days after the snapshot.
Recency				DAYS_SINCE_LAST_BOOKING	How long since they last stayed? (Critical indicator).
Frequency			LIFETIME_BOOKINGS		Total volume of bookings before the snapshot.
Monetary			HISTORICAL_LTV			Total spend before the snapshot.
Averages			AVG_ADR					Average Daily Rate of past stays (identifies "Budget" vs "Luxury" seekers).
Engagement			EMAIL_OPEN_RATE_90D		Member's open rate in the 90 days before the snapshot.
Interest			SEARCH_INTENT_TYPE		e.g., 'Long-haul', 'Staycation', 'Spa'.
Static				SIGNUP_CHANNEL			Did they come from an expensive Facebook lead or organic search?



--- others
opt in status

*/


------------------------------------------------------------------------------------------------------------------------
-- creating dataset for users who signed up before 13m ago and a snapshot of their information up to that date.
USE WAREHOUSE analyst_xlarge
;

SET snapshot_date = '2025-01-01'
;

-- shortlist of members with history and future at time of snapshot:
-- SHIRO_USER_ID
-- 43531163
-- 22671217
-- 57219324
-- 9218285
-- 83004656
-- 52177526
-- 68561092
-- 1638950
-- 22992289

CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.ltv_prediction_training_set AS

WITH
	member_list AS (

		SELECT
			sua.shiro_user_id,
			sua.signup_tstamp,
			sua.original_affiliate_name,
			sua.member_original_affiliate_classification,
			sua.original_affiliate_territory,
			sua.acquisition_platform
		FROM se.data.se_user_attributes sua
		WHERE sua.membership_account_status = 'FULL_ACCOUNT'
		  -- members more than 13 months old
		  AND sua.signup_tstamp <= $snapshot_date
-- 		  AND sua.shiro_user_id IN ( -- TODO REMOVE
-- 									'43531163',
-- 									'22671217',
-- 									'57219324',
-- 									'9218285',
-- 									'83004656',
-- 									'52177526',
-- 									'68561092',
-- 									'1638950',
-- 									'22992289'
-- 			)
	),
	target_variable AS (
		SELECT
			fb.shiro_user_id,
			COUNT(DISTINCT fb.booking_id)               AS actual_next_13m_bookings,
			SUM(fb.gross_revenue_gbp_constant_currency) AS actual_next_13m_value -- maybe this should be based on margin?
		FROM se.data.fact_booking fb
		INNER JOIN member_list ml
			ON fb.shiro_user_id = ml.shiro_user_id
		WHERE fb.booking_status_type IN ('live', 'cancelled')
		  -- bookings that occurred after snapshot date
		  AND fb.booking_completed_date BETWEEN $snapshot_date AND DATEADD(MONTH, 13, $snapshot_date)
		  -- only include bookings that aren't cancelled or that weren't cancelled within the 13m period
		  AND COALESCE(fb.cancellation_date, '9999-12-31') > DATEADD(MONTH, 13, $snapshot_date)
		GROUP BY 1
	),
	snapshot_ltv AS (
		SELECT
			fb.shiro_user_id,
			COUNT(DISTINCT fb.booking_id)                                   AS lifetime_bookings,
			SUM(fb.gross_revenue_gbp_constant_currency)                     AS historical_ltv, -- maybe this should be based on margin?
			DATEDIFF('day', MAX(fb.booking_completed_date), $snapshot_date) AS days_since_last_booking,
			AVG(fb.price_per_night)                                         AS avg_ppn
		FROM se.data.fact_booking fb
		INNER JOIN member_list ml
			ON fb.shiro_user_id = ml.shiro_user_id
		WHERE fb.booking_status_type IN ('live', 'cancelled')
		  -- bookings that occurred after snapshot date
		  AND fb.booking_completed_date < $snapshot_date
		  -- only include bookings that aren't cancelled or that weren't cancelled at the time of cancellation date
		  AND COALESCE(fb.cancellation_date, '9999-12-31') > $snapshot_date
		GROUP BY 1
	),
	snapshot_engagement AS (
		SELECT
			sm.attributed_user_id       AS shiro_user_id,
			COUNT(DISTINCT sm.touch_id) AS sessions_within_90d,
			COUNT(IFF(sm.touch_start_tstamp BETWEEN DATEADD(DAY, -30, $snapshot_date) AND $snapshot_date,
					  sm.touch_id,
					  NULL))            AS sessions_within_30d,
			COUNT(IFF(sm.touch_start_tstamp BETWEEN DATEADD(DAY, -30, $snapshot_date) AND $snapshot_date,
					  sm.touch_id,
					  NULL))            AS sessions_within_7d,
			SUM(sm.spvs)                AS spvs_within_90d,
			SUM(
					IFF(sm.touch_start_tstamp BETWEEN DATEADD(DAY, -30, $snapshot_date) AND $snapshot_date,
						sm.spvs,
						NULL))          AS spvs_within_30d,
			SUM(
					IFF(sm.touch_start_tstamp BETWEEN DATEADD(DAY, -7, $snapshot_date) AND $snapshot_date,
						sm.spvs,
						NULL))          AS spvs_within_7d,
			SUM(sm.booking_form_views)  AS booking_form_views_within_90d,
			SUM(
					IFF(sm.touch_start_tstamp BETWEEN DATEADD(DAY, -30, $snapshot_date) AND $snapshot_date,
						sm.booking_form_views,
						NULL))          AS booking_form_views_within_30d,
			SUM(
					IFF(sm.touch_start_tstamp BETWEEN DATEADD(DAY, -7, $snapshot_date) AND $snapshot_date,
						sm.booking_form_views,
						NULL))          AS booking_form_views_within_7d,
			SUM(sm.user_searches)       AS user_searches_within_90d,
			SUM(
					IFF(sm.touch_start_tstamp BETWEEN DATEADD(DAY, -30, $snapshot_date) AND $snapshot_date,
						sm.user_searches,
						NULL))          AS user_searches_within_30d,
			SUM(
					IFF(sm.touch_start_tstamp BETWEEN DATEADD(DAY, -7, $snapshot_date) AND $snapshot_date,
						sm.user_searches,
						NULL))          AS user_searches_within_7d,
		FROM se.bi.session_metrics sm
		INNER JOIN member_list ml
			ON sm.attributed_user_id = ml.shiro_user_id::VARCHAR
		WHERE sm.stitched_identity_type = 'se_user_id'
		  -- recent engagement in the 90 days before snapshot
		  AND sm.touch_start_tstamp::DATE BETWEEN DATEADD(DAY, -90, $snapshot_date) AND $snapshot_date
		GROUP BY 1
	)

SELECT
	$snapshot_date AS snapshot_date,
	ml.shiro_user_id,
	ml.signup_tstamp,
	ml.original_affiliate_name,
	ml.member_original_affiliate_classification,
	ml.original_affiliate_territory,
	ml.acquisition_platform,
	tv.actual_next_13m_bookings,
	tv.actual_next_13m_value,
	sl.lifetime_bookings,
	sl.historical_ltv,
	sl.days_since_last_booking,
	sl.avg_ppn,
	se.sessions_within_90d,
	se.sessions_within_30d,
	se.sessions_within_7d,
	se.spvs_within_90d,
	se.spvs_within_30d,
	se.spvs_within_7d,
	se.booking_form_views_within_90d,
	se.booking_form_views_within_30d,
	se.booking_form_views_within_7d,
	se.user_searches_within_90d,
	se.user_searches_within_30d,
	se.user_searches_within_7d
FROM member_list ml
LEFT JOIN target_variable tv
	ON ml.shiro_user_id = tv.shiro_user_id
LEFT JOIN snapshot_ltv sl
	ON ml.shiro_user_id = sl.shiro_user_id
LEFT JOIN snapshot_engagement se
	ON ml.shiro_user_id = se.shiro_user_id
;

SELECT
	COUNT(*)
FROM scratch.robinpatel.ltv_prediction_training_set lpts 28,743,665
;


-- Use the database and schema where you saved the model
USE DATABASE scratch
;

USE SCHEMA robinpatel
;

SELECT
	shiro_user_id,
	-- This calls the 'predict' method of your model
	model(member_ltv_pred)!PREDICT(
        OBJECT_CONSTRUCT(*) -- Passes all columns from the table to the model
    ):PREDICTED_LTV::FLOAT AS predicted_future_value
FROM your_active_member_table
LIMIT 100
;

------------------------------------------------------------------------------------------------------------------------
--calling model

SET prediction_date = '2026-01-01';

WITH
    member_list AS (
        SELECT
            sua.shiro_user_id,
            sua.signup_tstamp,
            sua.original_affiliate_name,
            sua.member_original_affiliate_classification,
            sua.original_affiliate_territory,
            sua.acquisition_platform
        FROM se.data.se_user_attributes sua
        WHERE sua.membership_account_status = 'FULL_ACCOUNT'
          AND sua.signup_tstamp <= $prediction_date
        LIMIT 100
    ),
    snapshot_ltv AS (
        SELECT
            fb.shiro_user_id,
            COUNT(DISTINCT fb.booking_id) AS lifetime_bookings,
            SUM(fb.gross_revenue_gbp_constant_currency) AS historical_ltv,
            DATEDIFF('day', MAX(fb.booking_completed_date), $prediction_date) AS days_since_last_booking,
            AVG(fb.price_per_night) AS avg_ppn
        FROM se.data.fact_booking fb
        INNER JOIN member_list ml ON fb.shiro_user_id = ml.shiro_user_id
        WHERE fb.booking_status_type IN ('live', 'cancelled')
          AND fb.booking_completed_date < $prediction_date
          AND COALESCE(fb.cancellation_date, '9999-12-31') > $prediction_date
        GROUP BY 1
    ),
    snapshot_engagement AS (
        SELECT
            sm.attributed_user_id AS shiro_user_id,
            COUNT(DISTINCT sm.touch_id) AS sessions_within_90d,
            COUNT(IFF(sm.touch_start_tstamp BETWEEN DATEADD(DAY, -30, $prediction_date) AND $prediction_date, sm.touch_id, NULL)) AS sessions_within_30d,
            COUNT(IFF(sm.touch_start_tstamp BETWEEN DATEADD(DAY, -7, $prediction_date) AND $prediction_date, sm.touch_id, NULL)) AS sessions_within_7d,
            SUM(sm.spvs) AS spvs_within_90d,
            SUM(IFF(sm.touch_start_tstamp BETWEEN DATEADD(DAY, -30, $prediction_date) AND $prediction_date, sm.spvs, NULL)) AS spvs_within_30d,
            SUM(IFF(sm.touch_start_tstamp BETWEEN DATEADD(DAY, -7, $prediction_date) AND $prediction_date, sm.spvs, NULL)) AS spvs_within_7d,
            SUM(sm.booking_form_views) AS booking_form_views_within_90d,
            SUM(IFF(sm.touch_start_tstamp BETWEEN DATEADD(DAY, -30, $prediction_date) AND $prediction_date, sm.booking_form_views, NULL)) AS booking_form_views_within_30d,
            SUM(IFF(sm.touch_start_tstamp BETWEEN DATEADD(DAY, -7, $prediction_date) AND $prediction_date, sm.booking_form_views, NULL)) AS booking_form_views_within_7d,
            SUM(sm.user_searches) AS user_searches_within_90d,
            SUM(IFF(sm.touch_start_tstamp BETWEEN DATEADD(DAY, -30, $prediction_date) AND $prediction_date, sm.user_searches, NULL)) AS user_searches_within_30d,
            SUM(IFF(sm.touch_start_tstamp BETWEEN DATEADD(DAY, -7, $prediction_date) AND $prediction_date, sm.user_searches, NULL)) AS user_searches_within_7d
        FROM se.bi.session_metrics sm
        INNER JOIN member_list ml ON sm.attributed_user_id = ml.shiro_user_id::VARCHAR
        WHERE sm.stitched_identity_type = 'se_user_id'
          AND sm.touch_start_tstamp::DATE BETWEEN DATEADD(DAY, -90, $prediction_date) AND $prediction_date
        GROUP BY 1
    )
SELECT
    ml.shiro_user_id,
    SCRATCH.ROBINPATEL.MEMBER_LTV_PRED!PREDICT(
        $prediction_date::VARCHAR,                                              -- SNAPSHOT_DATE
        ml.shiro_user_id::NUMBER,                                               -- SHIRO_USER_ID
        ml.signup_tstamp::TIMESTAMP_NTZ,                                        -- SIGNUP_TSTAMP
        COALESCE(ml.original_affiliate_name, 'UNKNOWN'),                        -- ORIGINAL_AFFILIATE_NAME
        COALESCE(ml.member_original_affiliate_classification, 'UNKNOWN'),       -- MEMBER_ORIGINAL_AFFILIATE_CLASSIFICATION
        COALESCE(ml.original_affiliate_territory, 'UNKNOWN'),                   -- ORIGINAL_AFFILIATE_TERRITORY
        COALESCE(ml.acquisition_platform, 'UNKNOWN'),                           -- ACQUISITION_PLATFORM
        0::NUMBER,                                                              -- ACTUAL_NEXT_13M_BOOKINGS (placeholder)
        COALESCE(sl.lifetime_bookings, 0)::NUMBER,                              -- LIFETIME_BOOKINGS
        COALESCE(sl.days_since_last_booking, 3650)::NUMBER,                     -- DAYS_SINCE_LAST_BOOKING
        COALESCE(se.sessions_within_90d, 0)::NUMBER,                            -- SESSIONS_WITHIN_90D
        COALESCE(se.sessions_within_30d, 0)::NUMBER,                            -- SESSIONS_WITHIN_30D
        COALESCE(se.sessions_within_7d, 0)::NUMBER,                             -- SESSIONS_WITHIN_7D
        COALESCE(se.spvs_within_90d, 0)::NUMBER,                                -- SPVS_WITHIN_90D
        COALESCE(se.spvs_within_30d, 0)::NUMBER,                                -- SPVS_WITHIN_30D
        COALESCE(se.spvs_within_7d, 0)::NUMBER,                                 -- SPVS_WITHIN_7D
        COALESCE(se.booking_form_views_within_90d, 0)::NUMBER,                  -- BOOKING_FORM_VIEWS_WITHIN_90D
        COALESCE(se.booking_form_views_within_30d, 0)::NUMBER,                  -- BOOKING_FORM_VIEWS_WITHIN_30D
        COALESCE(se.booking_form_views_within_7d, 0)::NUMBER,                   -- BOOKING_FORM_VIEWS_WITHIN_7D
        COALESCE(se.user_searches_within_90d, 0)::NUMBER,                       -- USER_SEARCHES_WITHIN_90D
        COALESCE(se.user_searches_within_30d, 0)::NUMBER,                       -- USER_SEARCHES_WITHIN_30D
        COALESCE(se.user_searches_within_7d, 0)::NUMBER,                        -- USER_SEARCHES_WITHIN_7D
        0::FLOAT,                                                               -- ACTUAL_NEXT_13M_VALUE (placeholder)
        COALESCE(sl.historical_ltv, 0)::FLOAT,                                  -- HISTORICAL_LTV
        COALESCE(sl.avg_ppn, 0)::FLOAT                                          -- AVG_PPN
    ):PREDICTED_LTV::FLOAT AS predicted_ltv,
    sl.historical_ltv,
    sl.lifetime_bookings
FROM member_list ml
LEFT JOIN snapshot_ltv sl ON ml.shiro_user_id = sl.shiro_user_id
LEFT JOIN snapshot_engagement se ON ml.shiro_user_id = se.shiro_user_id;