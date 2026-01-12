USE WAREHOUSE pipe_xlarge;

USE SCHEMA data_vault_mvp.single_customer_view_stg;
SELECT *
FROM data_vault_mvp.information_schema.tables
WHERE table_schema = 'SINGLE_CUSTOMER_VIEW_STG';

------------------------------------------------------------------------------------------------------------------------
--clean event stream

SELECT * FROM hygiene_vault_mvp.snowplow.event_stream limit 50;

SELECT e.event_tstamp,
       e.event_name,
       e.event_hash,
       e.device_platform,
       e.page_url,
       e.page_referrer,
       e.se_user_id,
       e.se_sale_id,
       e.booking_id
FROM module_touchification t
         INNER JOIN hygiene_vault_mvp.snowplow.event_stream e ON e.event_hash = t.event_hash
    AND t.touch_id = '51d04f5e776e5204425aa0655761eb3005eac1ed95852dc2633ad4c536862ded'
AND e.event_tstamp::DATE = '2020-02-24';


------------------------------------------------------------------------------------------------------------------------

SELECT touch_id,                 --< session id
       attributed_user_id,       --< user id following identity stitching, this is the user id via the best method of stitching (e.g. se_user_id, cookie_id)
       stitched_identity_type,   --< identify the method of stitching that was used to assign the
       touch_start_tstamp,       --< first event tstamp in session
       touch_end_tstamp,         --< last event tstamp in session
       touch_duration_seconds,   --< difference between first and last events in seconds
       touch_posa_territory,     --< territory as assigned by snowplow (found this to be unreliable)
       touch_hostname_territory, --< territory assigned via mapping the hostname to a territory
       touch_experience,         --< device experience that the touch occurred on (native app, web, mweb etc)
       touch_landing_page,       --< the full page url for the first page view in the touch
       touch_landing_pagepath,   --< page path of the first page view in touch
       touch_hostname,           --< the hostname the touch occurred on
       touch_exit_pagepath,      --< the full page url the touch exited on
       touch_referrer_url,       --< the referrer url the touch started with
       touch_event_count,        --< how many events are included in this touch
       touch_has_booking         --< whether one of the events in the touch is a confirmed booking event or not.
FROM module_touch_basic_attributes;

------------------------------------------------------------------------------------------------------------------------
--events of interest
--spvs
SELECT s.event_hash,
       s.touch_id,
       s.event_tstamp,
       s.se_sale_id,
       s.event_category,
       s.event_subcategory
FROM module_touched_spvs s;

--bookings
SELECT
       t.event_hash,
       t.touch_id,
       t.event_tstamp,
       t.booking_id,
       t.event_category,
       t.event_subcategory
FROM module_touched_transactions t;

--spvs by device type
SELECT b.touch_experience,
       count(*) AS spvs
FROM module_touched_spvs s
         INNER JOIN module_touch_basic_attributes b ON b.touch_id = s.touch_id
WHERE DATE_TRUNC(MONTH, s.event_tstamp) = '2019-12-01'
GROUP BY 1;

------------------------------------------------------------------------------------------------------------------------
--channelling
SELECT
       c.touch_id,
       c.touch_mkt_channel,
       c.touch_landing_page,
       c.touch_hostname,
       c.touch_hostname_territory,
       c.attributed_user_id,
       c.utm_campaign,
       c.utm_medium,
       c.utm_source,
       c.utm_term,
       c.utm_content,
       c.click_id,
       c.sub_affiliate_name,
       c.affiliate,
       c.touch_affiliate_territory,
       c.awadgroupid,
       c.awcampaignid,
       c.referrer_hostname,
       c.referrer_medium
FROM module_touch_marketing_channel c;


------------------------------------------------------------------------------------------------------------------------
--attribution table
SELECT
       touch_id,
       attributed_touch_id,
       attribution_model,
       attributed_weight
FROM module_touch_attribution;

SELECT
       touch_id,
       attributed_touch_id,
       attribution_model,
       attributed_weight
FROM module_touch_attribution
WHERE touch_id = '51d04f5e776e5204425aa0655761eb3005eac1ed95852dc2633ad4c536862ded';

--spvs by channel
SELECT b.touch_mkt_channel,
       count(*) AS spvs
FROM module_touched_spvs s
         INNER JOIN module_touch_marketing_channel b ON b.touch_id = s.touch_id
WHERE DATE_TRUNC(MONTH, s.event_tstamp) = '2019-12-01'
GROUP BY 1;

--spvs by attributed channel
SELECT c.touch_mkt_channel,
       count(*) AS spvs
FROM module_touched_spvs s
         INNER JOIN module_touch_attribution a ON a.touch_id = s.touch_id AND a.attribution_model = 'last non direct'
            INNER JOIN module_touch_marketing_channel c ON a.attributed_touch_id = c.touch_id
WHERE DATE_TRUNC(MONTH, s.event_tstamp) = '2019-12-01'
GROUP BY 1;

