SELECT *
FROM se.data.crm_events_clicks cec
WHERE cec.crm_platform = 'iterable'
  AND cec.event_tstamp::DATE = CURRENT_DATE - 1;

USE WAREHOUSE pipe_xlarge;

SELECT *
FROM se.data.scv_touched_spvs sts
    INNER JOIN se.data_pii.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
WHERE TRY_TO_NUMBER(stba.attributed_user_id) = 1269269
  AND sts.event_tstamp >= CURRENT_DATE - 1;

-- 1269269 user didn't have an spv but a click was tracked via iterable on 2022-07-25 12:03:38.000000000

SELECT *
FROM se.data_pii.scv_touch_basic_attributes stba
WHERE TRY_TO_NUMBER(stba.attributed_user_id) = 1269269;

SELECT *
FROM snowplow.atomic.events e
WHERE e.collector_tstamp >= CURRENT_DATE - 1
  AND e.page_url LIKE '%5-star-central-lisbon-stay-in-an-historic-and-palatial-setting-fully-refundable-hotel-real-palacio-portugal%';


SELECT *
FROM se.data_pii.scv_event_stream ses
WHERE ses.event_tstamp >= CURRENT_DATE - 1
  AND ses.se_user_id = 1269269

USE WAREHOUSE pipe_xlarge;
------------------------------------------------------------------------------------------------------------------------
--get a list of users that have apparently clicks but no spv generated
SELECT DISTINCT
    cec.shiro_user_id
FROM se.data.crm_events_clicks cec
WHERE cec.crm_platform = 'iterable'
  AND cec.event_tstamp::DATE = CURRENT_DATE - 1 -- click events that occurred yday

EXCEPT

SELECT DISTINCT
    stba.attributed_user_id
FROM se.data.scv_touched_spvs sts
    INNER JOIN se.data_pii.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
WHERE stba.stitched_identity_type = 'se_user_id'
  AND sts.event_tstamp >= CURRENT_DATE - 1
-- spv events that may have occurred between yesterday and today


--check the iterable click event
SELECT *
FROM se.data.crm_events_clicks cec
WHERE cec.crm_platform = 'iterable'
  AND cec.event_tstamp::DATE = CURRENT_DATE - 1 -- click events that occurred yday
  AND cec.shiro_user_id = 68383443;

-- User clicked on this: https://www.secretescapes.de/mallorcas-entspannte-seite-im-sommer-geniessen-kostenfrei-stornierbar-tacande-portals-bendinat-mallorcxa-balearen-spanien/sale-hotel?userId=68383443&timestamp=1658642649933&noPasswordSignIn=true&authHash=e26d766ea4fb59b4bae036da72163a89dc4d2d39&utm_medium=email&utm_source=newsletter&utm_campaign=4720029&utm_platform=ITERABLE&utm_content=SEGMENT_CORE_DE_ACT_01M_ATHENA_PoC_A&sale_id=A23052&landing-page=sale-page
-- at 2022-07-24 10:58:38.000000000

-- check spvs for that user
SELECT *
FROM se.data.scv_touched_spvs sts
    INNER JOIN se.data_pii.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
WHERE stba.stitched_identity_type = 'se_user_id'
  AND stba.attributed_user_id = '68383443'
  AND sts.event_tstamp >= CURRENT_DATE - 1;
-- no event found in spvs

-- Checking event stream
SELECT *
FROM se.data_pii.scv_event_stream ses
WHERE ses.event_tstamp >= CURRENT_DATE - 1
  AND ses.se_user_id = 68383443;
-- No events for this individual.

-- Check event stream for the url
SELECT *
FROM se.data_pii.scv_event_stream ses
WHERE ses.event_tstamp >= CURRENT_DATE - 1
  AND ses.page_url LIKE '%mallorcas-entspannte-seite-im-sommer-geniessen-kostenfrei-stornierbar-tacande-portals-bendinat-mallorcxa-balearen-spanien/%'
-- searching through these, cannot find anything that ties it to that user, 27,627 rows

-- check sessions for this user ever
SELECT *
FROM se.data_pii.scv_touch_basic_attributes stba
WHERE stba.attributed_user_id = '68383443'
-- 62 sessions, most recent on the 10th of July


-- Check event stream for the url for events that might not be attributed to a user and match email campaign
SELECT *
FROM se.data_pii.scv_event_stream ses
WHERE ses.event_tstamp >= CURRENT_DATE - 1
  AND ses.page_url LIKE '%mallorcas-entspannte-seite-im-sommer-geniessen-kostenfrei-stornierbar-tacande-portals-bendinat-mallorcxa-balearen-spanien/%'
  AND ses.se_user_id IS NULL
  AND ses.mkt_campaign = '4720029'
-- 73 rows

-- Found two events that were at a similar time, event hashes
-- EVENT_HASH
-- 632ecc67a9a9a65357f982138b21138b02cde2b3df534a5aef808c03e8246a68
-- efef05e0517f0baf209fec690324e9d8194ad23b0b72da358c2e3d61e39c1281

-- Check these hashes are in spvs
SELECT *
FROM se.data.scv_touched_spvs sts
WHERE sts.event_tstamp >= CURRENT_DATE - 1
  AND sts.event_hash IN (
                         '632ecc67a9a9a65357f982138b21138b02cde2b3df534a5aef808c03e8246a68',
                         'efef05e0517f0baf209fec690324e9d8194ad23b0b72da358c2e3d61e39c1281'
    )
--both spvs found in scv

------------------------------------------------------------------------------------------------------------------------
--repeating for a different user

--get a list of users that have apparently clicks but no spv generated
SELECT DISTINCT
    cec.shiro_user_id
FROM se.data.crm_events_clicks cec
WHERE cec.crm_platform = 'iterable'
  AND cec.event_tstamp::DATE = CURRENT_DATE - 2     -- click events that occurred yday
  AND PARSE_URL(url):parameters:sale_id IS NOT NULL -- spv definition by the url

EXCEPT

SELECT DISTINCT
    stba.attributed_user_id
FROM se.data.scv_touched_spvs sts
    INNER JOIN se.data_pii.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
WHERE stba.stitched_identity_type = 'se_user_id'
  AND sts.event_tstamp >= CURRENT_DATE - 2
-- spv events that may have occurred between yesterday and today

--check the iterable click event
SELECT *
FROM se.data.crm_events_clicks cec
WHERE cec.crm_platform = 'iterable'
  AND cec.event_tstamp::DATE = CURRENT_DATE - 2 -- click events that occurred yday
  AND cec.shiro_user_id = 10937967;

-- User 10937967 clicked on this: https://www.secretescapes.com/traditional-lake-district-stay-on-the-shores-of-lake-windermere-fully-refundable-macdonald-old-england-hotel-and-spa-windermere/sale-hotel?userId=10937967&timestamp=1658646304521&noPasswordSignIn=true&authHash=45e9addc760116b7f721aab741877bdf15981272&utm_medium=email&utm_source=newsletter&utm_campaign=4719670&utm_platform=ITERABLE&utm_content=SEGMENT_CORE_UK_ACT_WKLY&sale_id=A11077&landing-page=sale-page
-- at 2022-07-24 07:26:41.000000000

-- check spvs for that user
SELECT *
FROM se.data.scv_touched_spvs sts
    INNER JOIN se.data_pii.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
WHERE stba.stitched_identity_type = 'se_user_id'
  AND stba.attributed_user_id = '10937967'
  AND sts.event_tstamp >= CURRENT_DATE - 2;
-- no event found in spvs


-- Checking event stream
SELECT *
FROM se.data_pii.scv_event_stream ses
WHERE ses.event_tstamp >= CURRENT_DATE - 2
  AND ses.se_user_id = 10937967;
-- No events for this individual.

SELECT *
FROM se.data_pii.scv_event_stream ses
WHERE ses.contexts_com_secretescapes_content_context_1 IS NULL
  AND PARSE_URL(ses.page_url):parameters:sale_id IS NOT NULL
  AND ses.event_tstamp >= CURRENT_DATE - 1
  AND ses.event_name IN ('page_view', 'screen_view');


SELECT
    ses.event_name,
    ses.v_tracker,
    COUNT(*)
FROM se.data_pii.scv_event_stream ses
WHERE ses.event_tstamp >= CURRENT_DATE - 1
  AND ses.v_tracker LIKE 'ios%'
GROUP BY 1;

CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.yesterday_spvs AS (
    SELECT *
    FROM se.data_pii.scv_event_stream ses
    WHERE ses.event_tstamp >= CURRENT_DATE - 1
);

SELECT *
FROM scratch.robinpatel.yesterday_spvs ys
WHERE PARSE_URL(ys.page_url)['parameters']:sale_id IS NOT NULL
  AND ys.contexts_com_secretescapes_content_context_1 IS NULL
  AND ys.event_name = 'page_view'
;



SELECT *
FROM se.data.crm_events_clicks cec
WHERE cec.crm_platform = 'iterable'
  AND cec.event_tstamp::DATE = CURRENT_DATE - 1 -- click events that occurred yday
  AND cec.shiro_user_id = 10018385;


SELECT *
FROM se.data_pii.se_user_attributes sua
WHERE sua.shiro_user_id = 10018385;

SELECT *
FROM data_vault_mvp.dwh.iterable__user_profile iup;

------------------------------------------------------------------------------------------------------------------------
--repeating for a different user
USE WAREHOUSE pipe_xlarge;
--get a list of users that have apparently clicks but no spv generated
SELECT
    cec.shiro_user_id::INT
FROM se.data.crm_events_clicks cec
WHERE cec.crm_platform = 'iterable'
  AND cec.event_tstamp::DATE = CURRENT_DATE - 2     -- click events that occurred yday
  AND PARSE_URL(url):parameters:sale_id IS NOT NULL -- spv definition by the url

EXCEPT

SELECT DISTINCT
    stba.attributed_user_id::INT
FROM se.data.scv_touched_spvs sts
    INNER JOIN se.data_pii.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
WHERE stba.stitched_identity_type = 'se_user_id'
  AND sts.event_tstamp >= CURRENT_DATE - 2 -- spv events that may have occurred between yesterday and today
;


SELECT *
FROM se.data.crm_events_clicks cec
WHERE cec.crm_platform = 'iterable'
  AND cec.event_tstamp::DATE = CURRENT_DATE - 2 -- click events that occurred yday
  AND cec.shiro_user_id = 57784467;

SELECT *
FROM se.data_pii.scv_event_stream ses
WHERE ses.event_tstamp >= CURRENT_DATE - 2
  AND ses.page_url = '%kroatien-grandhotel-mit-spa-and-infinity-pool-kostenfrei-stornierbar-grand-hotel-adriatic-opatija-istrien-kroatien%'



SELECT *
FROM se.data.scv_touch_marketing_channel stmc;

SELECT *
FROM se.data.user_emails ue;


-- this will give you all matches from the CSV
WITH users AS (
    SELECT
        e.shiro_user_id,
        e.email,
        e.original_affiliate_id,
        e.original_affiliate_name,
        e.original_affiliate_territory_id,
        e.original_affiliate_territory,
        e.original_affiliate_brand,
        e.original_affiliate_domain,
        e.member_original_affiliate_classification,
        e.cohort_id,
        e.cohort_year_month,
        e.signup_tstamp,
        e.acquisition_platform,
        e.email_opt_in,
        e.email_opt_in_status,
        e.email_receive_weekly_offers,
        e.email_receive_sales_reminders,
        e.email_receive_hand_picked_offers,
        e.last_email_open_tstamp,
        e.last_email_click_tstamp,
        e.last_pageview_tstamp,
        e.last_sale_pageview_tstamp,
        e.last_abandoned_booking_tstamp,
        e.last_purchase_tstamp,
        e.last_complete_booking_tstamp
    FROM se.data_pii.se_user_attributes e
        INNER JOIN scratch.dominicpitt.downtonemails
                   ON scratch.dominicpitt.downtonemails.email = e.email
),
     bookings_before AS (
         SELECT
             shiro_user_id,
             MAX(booking_completed_date)                     AS last_booking_date_custom,
             COUNT(*)                                        AS bookings,
             SUM(margin_gross_of_toms_gbp_constant_currency) AS margin
         FROM se.data.fact_booking
         WHERE booking_status_type IN ('live', 'cancelled')
           AND booking_completed_date <= '2022-04-20'
         GROUP BY 1
     ),
     bookings_after AS (
         SELECT
             shiro_user_id,
             MAX(booking_completed_date)                     AS last_booking_date_custom,
             COUNT(*)                                        AS bookings,
             SUM(margin_gross_of_toms_gbp_constant_currency) AS margin
         FROM se.data.fact_booking
         WHERE booking_status_type IN ('live', 'cancelled')
           AND booking_completed_date > '2022-04-20'
         GROUP BY 1
     ),
     spvs_before AS
         (
             SELECT
                 tba.attributed_user_id,
                 MAX(spv.event_tstamp::DATE)              AS last_spv_date_before_custom_date,
                 COUNT(DISTINCT (spv.event_tstamp::DATE)) AS days_of_spvs_before_custom_date,
                 COUNT(*)                                 AS spvs_before_custom_date
             FROM se.data_pii.scv_touch_basic_attributes tba
                 INNER JOIN se.data.scv_touched_spvs spv ON tba.touch_id = spv.touch_id
             WHERE event_tstamp::DATE <= '2022-04-20'
             GROUP BY 1
         ),
     spvs_after AS
         (
             SELECT
                 tba.attributed_user_id,
                 MAX(spv.event_tstamp::DATE)              AS last_spv_date_after_custom_date,
                 COUNT(DISTINCT (spv.event_tstamp::DATE)) AS days_of_spvs_after_custom_date,
                 COUNT(*)                                 AS spvs_after_custom_date
             FROM se.data_pii.scv_touch_basic_attributes tba
                 INNER JOIN se.data.scv_touched_spvs spv ON tba.touch_id = spv.touch_id
             WHERE event_tstamp::DATE > '2022-04-20'
             GROUP BY 1
         ),

     email_open_dates AS (
         SELECT
             u.shiro_user_id,
             MAX(ue.date) AS last_email_open_date
         FROM users u
             LEFT JOIN se.data.user_emails ue ON u.shiro_user_id::VARCHAR = ue.shiro_user_id::VARCHAR
         WHERE ue.date <= '2022-04-20'
         GROUP BY 1
         HAVING SUM(ue.unique_opens) >= 1
     )

SELECT
    u.shiro_user_id,
    u.email,
    u.original_affiliate_id,
    u.original_affiliate_name,
    u.original_affiliate_territory_id,
    u.original_affiliate_territory,
    u.original_affiliate_brand,
    u.original_affiliate_domain,
    u.member_original_affiliate_classification,
    u.cohort_id,
    u.cohort_year_month,
    u.signup_tstamp,
    u.acquisition_platform,
    u.email_opt_in,
    u.email_opt_in_status,
    u.email_receive_weekly_offers,
    u.email_receive_sales_reminders,
    u.email_receive_hand_picked_offers,
    u.last_email_open_tstamp,
    u.last_email_click_tstamp,
    u.last_pageview_tstamp,
    u.last_sale_pageview_tstamp,
    u.last_abandoned_booking_tstamp,
    u.last_purchase_tstamp,
    u.last_complete_booking_tstamp,
    us.member_recency_status, -- on '2022-04-20'
    us.gross_bookings,        -- on '2022-04-20'
    us.booker_segment,        -- on '2022-04-20'
    us.engagement_segment,    -- on '2022-04-20'
    eo.last_email_open_date     AS last_email_open_before_custom_date,
    bb.bookings                 AS bookings_before_custom_date,
    bb.margin                   AS margin_before_custom_date,
    bb.last_booking_date_custom AS last_booking_date_before_custom_date,
    ba.bookings                 AS bookings_after_custom_date,
    ba.margin                   AS margin_after_custom_date,
    ba.last_booking_date_custom AS last_booking_date_after_custom_date,
    sb.last_spv_date_before_custom_date,
    sb.days_of_spvs_before_custom_date,
    sb.spvs_before_custom_date,
    sa.last_spv_date_after_custom_date,
    sa.days_of_spvs_after_custom_date,
    sa.spvs_after_custom_date
FROM users u
    LEFT JOIN se.data.user_segmentation us
              ON u.shiro_user_id = us.shiro_user_id AND us.date = '2022-04-20' -- reference stamp point for previous behaviour
    LEFT JOIN email_open_dates eo ON eo.shiro_user_id::VARCHAR = u.shiro_user_id::VARCHAR
    LEFT JOIN bookings_before bb ON bb.shiro_user_id::VARCHAR = u.shiro_user_id::VARCHAR
    LEFT JOIN bookings_after ba ON ba.shiro_user_id::VARCHAR = u.shiro_user_id::VARCHAR
    LEFT JOIN spvs_before sb ON sb.attributed_user_id::VARCHAR = u.shiro_user_id::VARCHAR
    LEFT JOIN spvs_after sa ON sa.attributed_user_id::VARCHAR = u.shiro_user_id::VARCHAR
