WITH spvs AS (
--spvs
    SELECT sts.se_sale_id,
           sts.event_tstamp::DATE         AS date,
           CASE
               WHEN stmc.touch_mkt_channel IN ('Other', 'Partner', 'Email - Other') THEN 'Other'
               WHEN stmc.touch_mkt_channel IN ('Blog', 'Direct', 'Organic Search', 'Organic Social') THEN 'Free'
               WHEN stmc.touch_mkt_channel IN ('Email - Other', 'Media', 'Other', 'Partner', 'YouTube') THEN 'Other'
               WHEN stmc.touch_mkt_channel IN
                    ('Affiliate Program', 'Display', 'Paid Social', 'PPC - Brand', 'PPC - Non Brand CPA', 'PPC - Non Brand CPL',
                     'PPC - Undefined') THEN 'Paid'
               ELSE stmc.touch_mkt_channel
               END                        AS channel, --last click channel
           CASE
               WHEN stba.touch_experience IN ('mobile wrap android', 'mobile wrap ios') THEN 'Wrap App'
               ELSE INITCAP(stba.touch_experience)
               END                        AS platform,
           COUNT(DISTINCT sts.event_hash) AS spvs,
           COUNT(DISTINCT sts.touch_id)   AS sessions
    FROM se.data.scv_touched_spvs sts
             INNER JOIN se.data.scv_touch_marketing_channel stmc ON sts.touch_id = stmc.touch_id
             INNER JOIN se.data.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
    WHERE sts.event_tstamp >= '2020-01-01'
    GROUP BY 1, 2, 3, 4
),
     bookings AS (
         --bookings
         SELECT fcb.sale_id                         AS se_sale_id,
                fcb.booking_completed_date          AS date,
                CASE
                    WHEN stmc.touch_mkt_channel IN ('Other', 'Partner', 'Email - Other') THEN 'Other'
                    WHEN stmc.touch_mkt_channel IN ('Blog', 'Direct', 'Organic Search', 'Organic Social') THEN 'Free'
                    WHEN stmc.touch_mkt_channel IN ('Email - Other', 'Media', 'Other', 'Partner', 'YouTube') THEN 'Other'
                    WHEN stmc.touch_mkt_channel IN
                         ('Affiliate Program', 'Display', 'Paid Social', 'PPC - Brand', 'PPC - Non Brand CPA',
                          'PPC - Non Brand CPL',
                          'PPC - Undefined') THEN 'Paid'
                    ELSE stmc.touch_mkt_channel
                    END                             AS channel, --last click channel
                CASE
                    WHEN stba.touch_experience IN ('mobile wrap android', 'mobile wrap ios') THEN 'Wrap App'
                    ELSE INITCAP(stba.touch_experience)
                    END                             AS platform,
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
         WHERE fcb.booking_completed_date >= '2020-01-01'
         GROUP BY 1, 2, 3, 4
     )

SELECT s.se_sale_id,
       CASE
           WHEN ssa.posa_territory = 'UK' THEN 'UK'
           WHEN ssa.posa_territory IN ('DE', 'CH') THEN 'DACH'
           WHEN ssa.posa_territory IN ('SE', 'DK', 'NO') THEN 'Scandi'
           WHEN ssa.posa_territory = 'BE' THEN 'Belgium'
           WHEN ssa.posa_territory = 'NL' THEN 'Netherlands'
           WHEN ssa.posa_territory = 'FR' THEN 'France'
           WHEN ssa.posa_territory = 'IT' THEN 'Italy'
           WHEN ssa.posa_territory = 'SE' THEN 'Spain'
           WHEN ssa.posa_territory IN ('SG', 'HK', 'MY', 'ID') THEN 'Asia'
           END
                                     AS posa_category,
       ssa.posa_territory,
       ssa.posu_division,
       ssa.posu_country,
       ssa.posu_city,
       ssa.sale_active,
       ssa.start_date,
       ssa.sale_name,
       ssa.salesforce_opportunity_id AS global_sale_id,
       cs.name                       AS company_name,
       s.date,
       sc.date_value,
       sc.day_name,
       sc.year,
       sc.se_year,
       sc.se_week,
       sc.month,
       sc.month_name,
       sc.day_of_month,
       sc.day_of_week,
       sc.week_start,
       sc.yesterday,
       sc.yesterday_last_week,
       sc.this_week_wtd              AS this_week,
       sc.last_week_wtd              AS wtd_last_week,
       sc.last_week                  AS last_week,
       s.channel,
       s.platform,
       s.spvs,
       s.sessions,
       b.trx,
       b.margin,
       b.gross_revenue,
       b.appn,
       b.appppn,
       b.nights,
       b.avg_guests

FROM spvs s
         LEFT JOIN bookings b ON
        s.se_sale_id = b.se_sale_id
        AND s.date = b.date
        AND s.channel = b.channel
        AND s.platform = b.platform
         INNER JOIN se.data.se_sale_attributes ssa ON s.se_sale_id = ssa.se_sale_id
         LEFT JOIN se.data.se_calendar sc ON s.date = sc.date_value
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.company_snapshot cs ON ssa.company_id = cs.id
WHERE ssa.product_configuration = 'Hotel'
  AND ssa.data_model = 'New Data Model';

------------------------------------------------------------------------------------------------------------------------

SELECT sts.se_sale_id,
       sts.event_tstamp::DATE                                        AS date,
       se.data.channel_category(stmc.touch_mkt_channel)              AS channel,
--        CASE
--            WHEN stmc.touch_mkt_channel IN ('Other', 'Partner', 'Email - Other') THEN 'Other'
--            WHEN stmc.touch_mkt_channel IN ('Blog', 'Direct', 'Organic Search', 'Organic Social') THEN 'Free'
--            WHEN stmc.touch_mkt_channel IN ('Email - Other', 'Media', 'Other', 'Partner', 'YouTube') THEN 'Other'
--            WHEN stmc.touch_mkt_channel IN
--                 ('Affiliate Program', 'Display', 'Paid Social', 'PPC - Brand', 'PPC - Non Brand CPA', 'PPC - Non Brand CPL',
--                  'PPC - Undefined') THEN 'Paid'
--            ELSE stmc.touch_mkt_channel
--            END                        AS channel, --last click channel
       se.data.platform_from_touch_experience(stba.touch_experience) AS platform,
--        CASE
--            WHEN stba.touch_experience IN ('mobile wrap android', 'mobile wrap ios') THEN 'Wrap App'
--            ELSE INITCAP(stba.touch_experience)
--            END                        AS platform,
       COUNT(DISTINCT sts.event_hash)                                AS spvs,
       COUNT(DISTINCT sts.touch_id)                                  AS sessions
FROM se.data.scv_touched_spvs sts
         INNER JOIN se.data.scv_touch_marketing_channel stmc ON sts.touch_id = stmc.touch_id
         INNER JOIN se.data.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
WHERE sts.event_tstamp >= '2020-01-01'
GROUP BY 1, 2, 3, 4

------------------------------------------------------------------------------------------------------------------------
SELECT fcb.sale_id                                                   AS se_sale_id,
       fcb.booking_completed_date::DATE                              AS date,
       se.data.channel_category(stmc.touch_mkt_channel)              AS channel, --last click channel
       se.data.platform_from_touch_experience(stba.touch_experience) AS platform,
       COUNT(1)                                                      AS bookings,
       SUM(fcb.margin_gross_of_toms_gbp)                             AS margin_gross_of_toms_gbp,
       SUM(fcb.gross_booking_value_gbp)                              AS gross_booking_value_gbp,
       AVG(fcb.price_per_night)                                      AS appn,
       AVG(fcb.price_per_person_per_night)                           AS appppn,
       SUM(fcb.no_nights)                                            AS nights,
       AVG(fcb.adult_guests
           + fcb.child_guests
           + fcb.infant_guests)                                      AS avg_guests
FROM se.data.fact_complete_booking fcb
         INNER JOIN se.data.scv_touched_transactions stt ON fcb.booking_id = stt.booking_id
         INNER JOIN se.data.scv_touch_marketing_channel stmc ON stt.touch_id = stmc.touch_id
         INNER JOIN se.data.scv_touch_basic_attributes stba ON stt.touch_id = stba.touch_id
WHERE fcb.booking_completed_date >= '2020-01-01'
GROUP BY 1, 2, 3, 4;

------------------------------------------------------------------------------------------------------------------------


SELECT ssa.se_sale_id,
       ssa.base_sale_id,
       ssa.sale_id,
       ssa.salesforce_opportunity_id,
       ssa.sale_name,
       ssa.sale_name_object,
       ssa.sale_active,
       ssa.class,
       ssa.has_flights_available,
       ssa.default_preferred_airport_code,
       ssa.type,
       ssa.hotel_chain_link,
       ssa.closest_airport_code,
       ssa.is_team20package,
       ssa.sale_able_to_sell_flights,
       ssa.sale_product,
       ssa.sale_type,
       ssa.product_type,
       ssa.product_configuration,
       ssa.product_line,
       ssa.data_model,
       ssa.hotel_location_info_id,
       ssa.active,
       ssa.default_hotel_offer_id,
       ssa.commission,
       ssa.commission_type,
       ssa.original_contractor_id,
       ssa.original_contractor_name,
       ssa.original_joint_contractor_id,
       ssa.original_joint_contractor_name,
       ssa.current_contractor_id,
       ssa.current_contractor_name,
       ssa.current_joint_contractor_id,
       ssa.current_joint_contractor_name,
       ssa.date_created,
       ssa.destination_type,
       ssa.start_date,
       ssa.end_date,
       ssa.hotel_id,
       ssa.base_currency,
       ssa.city_district_id,
       ssa.company_id,
       ssa.company_name,
       ssa.hotel_code,
       ssa.latitude,
       ssa.longitude,
       ssa.location_info_id,
       ssa.posa_territory,
       se.data.posa_category_from_territory(ssa.posa_territory) AS posa_category,
       ssa.posa_country,
       ssa.posa_currency,
       ssa.posu_division,
       ssa.posu_country,
       ssa.posu_city,
       ssa.supplier_id,
       ssa.supplier_name
FROM se.data.se_sale_attributes ssa
WHERE ssa.data_model = 'New Data Model'
AND ssa.sale_active;

SELECT ssa.product_configuration,
       count(*)
FROM se.data.se_sale_attributes ssa
WHERE ssa.data_model = 'New Data Model'
AND ssa.sale_active
GROUP BY 1
ORDER BY 2 DESC;

