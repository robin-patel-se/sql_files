-- cohort model

SET (from_date, to_date)= ('2011-01-01', '2021-12-31');

WITH bookings AS (
    SELECT ac.affiliate_category,
           sua.original_affiliate_territory_id,
           DATE_TRUNC('MONTH', fcb.booking_completed_timestamp::DATE) AS booking_month,
           DATE_TRUNC('MONTH', sua.signup_tstamp)                     AS sign_up_month,
           DATEDIFF('month', sua.signup_tstamp, booking_month)        AS age,
           SUM(fcb.margin_gross_of_toms_gbp_constant_currency)        AS margin

    FROM se.data.se_user_attributes sua -- changed the join to user being the main table
        LEFT JOIN se.data.fact_complete_booking fcb ON sua.shiro_user_id = fcb.shiro_user_id -- changed to complete bookings
        LEFT JOIN collab.performance_analytics.affiliate_categories ac ON sua.original_affiliate_id = ac.id
    WHERE fcb.booking_completed_timestamp >= $from_date
      AND fcb.booking_completed_timestamp <= $to_date
      AND fcb.booking_status_type_net_of_covid = 'live' -- do we want to remove this after?
    GROUP BY 1, 2, 3, 4, 5
),
     user_signup_counts AS (
         SELECT ac.affiliate_category,
                sua.original_affiliate_territory_id,
                DATE_TRUNC('MONTH', sua.signup_tstamp)::DATE AS sign_up_month,
                COUNT(sua.shiro_user_id)                     AS users_signed_up
         FROM se.data.se_user_attributes sua
             LEFT JOIN collab.performance_analytics.affiliate_categories ac ON sua.original_affiliate_id = ac.id
         GROUP BY 1, 2, 3
     ),

     grain AS (
         --manufacture a grain of
         SELECT DATE_TRUNC('MONTH', sc.date_value::DATE) AS month,
                ac.affiliate_category,
                ac.territory_id
         FROM se.data.se_calendar sc
             CROSS JOIN collab.performance_analytics.affiliate_categories ac
         WHERE DATE_TRUNC('MONTH', sc.date_value::DATE) BETWEEN $from_date AND $to_date
         GROUP BY 1, 2, 3
     ),

     age AS (
         SELECT DATE_TRUNC('MONTH', sc.date_value::DATE)           AS month,
                DATE_TRUNC('MONTH', sc2.date_value::DATE)          AS event_month,
                DATEDIFF('month', DATE_TRUNC('MONTH', sc2.date_value::DATE),
                         DATE_TRUNC('MONTH', sc.date_value::DATE)) AS age

         FROM se.data.se_calendar sc
             JOIN se.data.se_calendar sc2

         WHERE DATE_TRUNC('MONTH', sc.date_value::DATE) >= '2011-01-01'
           AND DATE_TRUNC('MONTH', sc.date_value::DATE) <= getdate()
           AND DATE_TRUNC('MONTH', sc2.date_value::DATE) >= '2011-01-01'
           AND DATE_TRUNC('MONTH', sc2.date_value::DATE) <= getdate()
           AND DATE_TRUNC('MONTH', sc2.date_value::DATE) <= DATE_TRUNC('MONTH', sc.date_value::DATE)

         GROUP BY 1, 2, 3
     ) -- added the AGE CTE based only of the SE_calendar

SELECT g.affiliate_category,
       g.territory_id,
       g.month AS month_grain,
       b.booking_month,
       -- b.age AS Booking_age,
       usc.users_signed_up,
       ua.age  AS age,
       ua.event_month,
       b.margin

FROM age ua
    JOIN      grain g ON g.month = ua.month
    LEFT JOIN user_signup_counts usc
              ON ua.event_month = usc.sign_up_month AND g.affiliate_category = usc.affiliate_category AND
                 g.territory_id = usc.original_affiliate_territory_id
    LEFT JOIN bookings b
              ON b.booking_month = g.month AND b.age = ua.age AND g.affiliate_category = b.affiliate_category AND
                 g.territory_id = b.original_affiliate_territory_id

WHERE g.territory_id IN (4, 1, 2, 11, 12)
  AND g.affiliate_category IS NOT NULL

GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
;


SELECT dcs.*,
       das.*
FROM data_vault_mvp.chiasma_sql_server_snapshots.fact_bookings_snapshot fbs
    LEFT JOIN data_vault_mvp.chiasma_sql_server_snapshots.dim_bookings_snapshot dbs ON fbs.key_booking = dbs.key_booking
    LEFT JOIN data_vault_mvp.chiasma_sql_server_snapshots.dim_customers_snapshot dcs ON fbs.key_customer = dcs.key_customer
    LEFT JOIN data_vault_mvp.chiasma_sql_server_snapshots.dim_affiliates_snapshot das ON dcs.key_first_affiliate = das.key_affiliate



SELECT *
FROM se.data.se_territory st;

------------------------------------------------------------------------------------------------------------------------
-- cube users
CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.chiasma_cohort_bookings AS (
    WITH users AS (
        SELECT dcs.key_customer,
               dcs.customer_id,
               dcs.user_join_date,
               dcs.key_original_business_unit_id,
               bus.business_unit
        FROM data_vault_mvp.chiasma_sql_server_snapshots.dim_customers_snapshot dcs
            INNER JOIN data_vault_mvp.chiasma_sql_server_snapshots.dim_affiliates_snapshot das ON dcs.key_first_affiliate = das.key_affiliate
            INNER JOIN data_vault_mvp.chiasma_sql_server_snapshots.business_units_snapshot bus ON dcs.key_original_business_unit_id = bus.business_unit_id
        WHERE bus.business_unit = 'DE'
    ),
         model_data AS (
             SELECT fbvs.key_date_booked,
                    ((fbvs.margin_gross_of_toms * derived_exchange_rate) / dcu.divide_by_to_get_constant_currency) AS margin_gross_of_toms_constant_currency,
                    u.customer_id,
                    u.user_join_date,
                    u.business_unit,
                    dbs.transaction_id,
                    oss.source_name
             FROM data_vault_mvp.chiasma_sql_server_snapshots.fact_bookings_snapshot fbvs
                 INNER JOIN data_vault_mvp.chiasma_sql_server_snapshots.dim_currencies_snapshot dcu
                            ON dcu.key_currency = fbvs.key_currency
                 INNER JOIN data_vault_mvp.chiasma_sql_server_snapshots.dim_bookings_snapshot dbs ON fbvs.key_booking = dbs.key_booking
                 INNER JOIN data_vault_mvp.chiasma_sql_server_snapshots.original_sources_snapshot oss ON dbs.source_id = oss.source_id
                 INNER JOIN users u ON fbvs.key_customer = u.key_customer
             WHERE dbs.key_status = 1 --booked
               AND oss.source_name IN ('Secret Escapes')
         )
-- SELECT YEAR(md.key_date_booked),
--        SUM(md.margin_gross_of_toms_constant_currency)
-- FROM model_data md
-- GROUP BY 1
-- ORDER BY 1

    SELECT md.*
    FROM model_data md
)
;


SELECT COUNT(*)
FROM se.data.se_user_attributes sua
WHERE sua.original_affiliate_territory = 'DE';


SELECT YEAR(fcb.booking_completed_timestamp)               AS year,
       SUM(fcb.margin_gross_of_toms_gbp_constant_currency) AS margin
FROM se.data.fact_complete_booking fcb
    INNER JOIN se.data.se_user_attributes sua ON fcb.shiro_user_id = sua.shiro_user_id
WHERE fcb.booking_status_type_net_of_covid = 'live'
  AND sua.original_affiliate_territory = 'DE'
  AND fcb.se_brand = 'SE Brand'
GROUP BY 1
ORDER BY 1;

CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.snowflake_cohort_bookings AS (
    SELECT fcb.booking_completed_date,
           fcb.margin_gross_of_toms_gbp_constant_currency,
           fcb.shiro_user_id,
           sua.signup_tstamp,
           sua.original_affiliate_territory,
           fcb.transaction_id,
           fcb.se_brand

    FROM se.data.fact_complete_booking fcb
        INNER JOIN se.data.se_user_attributes sua ON fcb.shiro_user_id = sua.shiro_user_id
    WHERE fcb.booking_status_type_net_of_covid = 'live'
      AND sua.original_affiliate_territory = 'DE'
      AND fcb.se_brand = 'SE Brand'
)
;

WITH differences AS (
    SELECT ccb.transaction_id AS transaction_id
    FROM scratch.robinpatel.chiasma_cohort_bookings ccb
        EXCEPT
    SELECT scb.transaction_id AS transaction_id
    FROM scratch.robinpatel.snowflake_cohort_bookings scb
),
     model_data AS (
         SELECT d.transaction_id,
                COALESCE(ccb.key_date_booked, scb.booking_completed_date) AS date_booked,
                ccb.business_unit                                         AS chiasma_territory,
                scb.original_affiliate_territory                          AS snowflake_territory,
                sua.original_affiliate_territory,
                sb.affiliate_user_id,
                aus.affiliate_id                                          AS affiliate_user_affiliate_id,
                a.affiliate_name,
                a.category                                                AS affiliate_category,
                a.territory_id                                            AS affiliate_user_affiliate_territory_id,
                t.name                                                    AS affiliate_user_affiliate_territory,
                sb.margin_gross_of_toms_gbp_constant_currency
         FROM differences d
             LEFT JOIN scratch.robinpatel.chiasma_cohort_bookings ccb ON d.transaction_id = ccb.transaction_id
             LEFT JOIN scratch.robinpatel.snowflake_cohort_bookings scb ON d.transaction_id = scb.transaction_id
             LEFT JOIN se.data.fact_booking fb ON d.transaction_id = fb.transaction_id
             LEFT JOIN se.data.se_user_attributes sua ON fb.shiro_user_id = sua.shiro_user_id
             LEFT JOIN se.data.se_booking sb ON d.transaction_id = sb.transaction_id
             LEFT JOIN data_vault_mvp.cms_mysql_snapshots.affiliate_user_snapshot aus ON sb.affiliate_user_id = aus.id
             LEFT JOIN latest_vault.cms_mysql.affiliate a ON aus.affiliate_id = a.id
             LEFT JOIN hygiene_snapshot_vault_mvp.cms_mysql.territory t ON a.territory_id = t.id
     )
SELECT md.affiliate_name,
       COUNT(DISTINCT md.transaction_id)                  AS bookings,
       SUM(md.margin_gross_of_toms_gbp_constant_currency) AS margin
FROM model_data md
GROUP BY 1
;



WITH differences AS (
    SELECT ccb.transaction_id AS transaction_id
    FROM scratch.robinpatel.chiasma_cohort_bookings ccb
        EXCEPT
    SELECT scb.transaction_id AS transaction_id
    FROM scratch.robinpatel.snowflake_cohort_bookings scb
)
SELECT YEAR(sb.booking_completed_timestamp)                       AS year,
       IFF(sb.is_affiliate_booking, 'affilaite', 'non affiliate') AS booking_type,
       SUM(sb.margin_gross_of_toms_gbp_constant_currency)         AS margin,
       COUNT(DISTINCT sb.transaction_id)                          AS bookings
FROM differences d
    LEFT JOIN se.data.se_booking sb ON d.transaction_id = sb.transaction_id
GROUP BY 1, 2