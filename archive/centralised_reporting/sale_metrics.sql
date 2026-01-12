USE WAREHOUSE pipe_xlarge;

--sales by date
CREATE OR REPLACE TABLE scratch.robinpatel.kpi_report_grain AS
--list of sale ids
WITH sale_list AS (
    --create a list of sale ids that have either had an spv or have a start date this year
    SELECT DISTINCT se_sale_id
    FROM se.data.scv_touched_spvs sts
    WHERE sts.event_tstamp >= DATE_TRUNC(YEAR, current_date)

    UNION

    SELECT se_sale_id
    FROM se.data.dim_sale ds
    WHERE ds.sale_start_date >= DATE_TRUNC(YEAR, current_date)
    OR (--was live over jan 1st
        ds.sale_start_date <= DATE_TRUNC(YEAR, current_date) AND
        ds.sale_end_date >= DATE_TRUNC(YEAR, current_date)
        )
)
SELECT s.se_sale_id,
       ds.sale_product,
       ds.sale_type,
       ds.product_type,
       ds.product_configuration,
       ds.product_line,
       ds.data_model,
       ds.sale_start_date,
       ds.sale_end_date,
       ds.sale_active,
       ds.tech_platform,
FROM sale_list s
    LEFT JOIN se.data.dim_sale ds ON s.se_sale_id = ds.se_sale_id
;

--spvs
SELECT sts.se_sale_id,
       sts.event_tstamp::DATE         AS date,
       stmc.touch_mkt_channel         AS channel, --last click channel
       stba.touch_experience          AS platform,
       COUNT(DISTINCT sts.event_hash) AS spvs,
       COUNT(DISTINCT sts.touch_id)   AS sessions
FROM se.data.scv_touched_spvs sts
         INNER JOIN se.data.scv_touch_marketing_channel stmc ON sts.touch_id = stmc.touch_id
         INNER JOIN se.data.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
WHERE sts.event_tstamp >= date_trunc(YEAR, current_date)
GROUP BY 1, 2, 3, 4;

--booking data
SELECT fcb.sale_id                         AS se_sale_id,
       fcb.booking_completed_date          AS date,
       stmc.touch_mkt_channel              AS channel, --last click channel
       stba.touch_experience               AS platform,
       COUNT(1)                            AS trx,
       SUM(fcb.margin_gross_of_toms_gbp)   AS margin,
       SUM(fcb.gross_booking_value_gbp)    AS gross_revenue,
       AVG(fcb.price_per_night)            AS appn,
       AVG(fcb.price_per_person_per_night) AS appppn,
       SUM(fcb.no_nights)                  AS nights,
       AVG(fcb.adult_guests
           + fcb.child_guests
           + fcb.infant_guests)            AS avg_guests
FROM se.data.fact_complete_booking fcb
         INNER JOIN se.data.scv_touched_transactions stt ON fcb.booking_id = stt.booking_id
         INNER JOIN se.data.scv_touch_marketing_channel stmc ON stt.touch_id = stmc.touch_id
         INNER JOIN se.data.scv_touch_basic_attributes stba ON stt.touch_id = stba.touch_id
WHERE fcb.booking_completed_date >= date_trunc(YEAR, CURRENT_DATE)
GROUP BY 1, 2, 3, 4;

USE WAREHOUSE pipe_xlarge;

--columns names
--company name
--order the  columns appear in
--absolute last week
--absolute variances
--change currency back to 2 decimals
--add posu division, posu country
--posa territory


