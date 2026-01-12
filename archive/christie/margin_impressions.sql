--impressions if its been in an email that has been opened
--timeframe: october snapshot


USE WAREHOUSE pipe_xlarge;

WITH impression_data AS (
    SELECT aer.se_sale_id,
           aer.event_date,
           aer.mapped_territory AS territory,
           SUM(aer.impressions) AS impressions,
           SUM(aer.clicks)      AS clicks
    FROM se.data.athena_email_reporting aer
    GROUP BY 1, 2, 3
),
     spvs AS (
         SELECT sts.se_sale_id,
                sts.event_tstamp::DATE         AS spv_date,
                stmc.touch_affiliate_territory AS territory,
                COUNT(*)                       AS spvs
         FROM se.data.scv_touched_spvs sts
                  INNER JOIN se.data.scv_touch_marketing_channel stmc ON sts.touch_id = stmc.touch_id
         WHERE  sts.event_tstamp::DATE >= '2020-11-20' AND sts.event_tstamp::DATE <= current_date
         GROUP BY 1, 2, 3
     ),
     margin AS (
         SELECT fcb.se_sale_id,
                fcb.booking_completed_date,
                fcb.territory,
                SUM(fcb.margin_gross_of_toms_gbp_constant_currency) AS margin
         FROM se.data.fact_complete_booking fcb
         WHERE fcb.booking_completed_date >= '2020-11-20'
         GROUP BY 1, 2, 3
     )
SELECT COALESCE(imd.se_sale_id, m.se_sale_id, s.se_sale_id)           AS se_sale_id,
       COALESCE(imd.event_date, m.booking_completed_date, s.spv_date) AS date,
       COALESCE(imd.territory, m.territory, s.territory)              AS territory,
       imd.impressions,
       imd.clicks,
       s.spvs,
       m.margin
FROM impression_data imd
         FULL OUTER JOIN margin m ON imd.se_sale_id = m.se_sale_id
    AND imd.event_date = m.booking_completed_date
    AND imd.territory = m.territory
         FULL OUTER JOIN spvs s ON COALESCE(imd.se_sale_id, m.se_sale_id) = s.se_sale_id
    AND COALESCE(imd.event_date, m.booking_completed_date) = s.spv_date
    AND COALESCE(imd.territory, m.territory) = s.territory
;

