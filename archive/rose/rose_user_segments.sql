USE WAREHOUSE pipe_xlarge;
CREATE SCHEMA IF NOT EXISTS collab.user_eng_segments;

CREATE OR REPLACE TRANSIENT TABLE collab.user_eng_segments.post2018activity COPY GRANTS AS (
    SELECT member_id,
           member_age,
           member_cohort_year_month,
           customer_age,
           calendar_date,
           spv_count,
           member_original_affiliate_classification,
           member_original_affiliate_territory,
           member_subscription_status                                                AS last_subscription_status,
           LAG(spv_count, 1, 0) OVER (PARTITION BY member_id ORDER BY calendar_date) AS last_spvcount,

           LAG(CASE WHEN spv_count > 0 THEN calendar_date END, 1) IGNORE NULLS
               OVER (PARTITION BY member_id ORDER BY calendar_date)                  AS prev_spv_date,
           email_clicks_count,
           LAG(CASE WHEN email_clicks_count > 0 THEN calendar_date END, 1) IGNORE NULLS
               OVER (PARTITION BY member_id ORDER BY calendar_date)                  AS prev_emailclick_date,
           email_opens_count,
           LAG(CASE WHEN email_opens_count > 0 THEN calendar_date END, 1) IGNORE NULLS
               OVER (PARTITION BY member_id ORDER BY calendar_date)                  AS prev_emailopen_date,
           booking_completed_count,
           LAG(CASE WHEN booking_completed_count > 0 THEN calendar_date END, 1) IGNORE NULLS
               OVER (PARTITION BY member_id ORDER BY calendar_date)                  AS prev_booking_date,
           booking_margin,
           booking_cumulative_count,
           CURRENT_TIMESTAMP()::TIMESTAMP                                            AS updated_at
    FROM data_vault_mvp.customer_model.customer_model_full_uk_de
    WHERE calendar_date >= '2019-01-01' --was set to 2018-01-01
);

CREATE OR REPLACE TRANSIENT TABLE collab.user_eng_segments.aggregate_post2018activity AS (
    SELECT member_id,
           member_age,
           member_cohort_year_month,
           customer_age,
           calendar_date,
           spv_count,
           email_opens_count,
           email_clicks_count,
           booking_completed_count,
           booking_margin,
           booking_cumulative_count,
           last_subscription_status,
           member_original_affiliate_classification,
           member_original_affiliate_territory    AS territory,
           (calendar_date - prev_spv_date)        AS days_since_last_spv,
           (calendar_date - prev_emailopen_date)  AS days_since_last_emailopen,
           (calendar_date - prev_emailclick_date) AS days_since_last_emailclick,
           (calendar_date - prev_booking_date)    AS days_since_last_booking,

           CURRENT_TIMESTAMP()::TIMESTAMP         AS updated_at

    FROM collab.user_eng_segments.post2018activity
);

CREATE OR REPLACE TRANSIENT TABLE collab.user_eng_segments.engagement_classification COPY GRANTS AS (
    SELECT member_id,
           member_age,
           member_cohort_year_month,
           customer_age,
           calendar_date,
           spv_count,
           email_opens_count,
           email_clicks_count,
           booking_completed_count,
           booking_margin,
           booking_cumulative_count,
           last_subscription_status,
           territory,
           CASE
               WHEN days_since_last_spv < 30 OR days_since_last_booking < 30 OR days_since_last_emailclick < 30 OR
                    days_since_last_emailopen < 30 THEN 'Active 1M'
               WHEN (days_since_last_spv >= 30 AND days_since_last_spv < 90) OR
                    (days_since_last_booking >= 30 AND days_since_last_booking < 90) OR
                    (days_since_last_emailclick >= 30 AND days_since_last_emailclick < 90) OR
                    (days_since_last_emailopen >= 30 AND days_since_last_emailopen < 90) THEN 'Active 3M'
               WHEN (days_since_last_spv >= 90 AND days_since_last_spv < 180) OR
                    (days_since_last_booking >= 90 AND days_since_last_booking < 180) OR
                    (days_since_last_emailclick >= 90 AND days_since_last_emailclick < 180) OR
                    (days_since_last_emailopen >= 90 AND days_since_last_emailopen < 180) THEN 'Active 6M'
               WHEN (days_since_last_spv >= 180 AND days_since_last_spv < 270) OR
                    (days_since_last_booking >= 180 AND days_since_last_booking < 270) OR
                    (days_since_last_emailclick >= 180 AND days_since_last_emailclick < 270) OR
                    (days_since_last_emailopen >= 180 AND days_since_last_emailopen < 270) THEN 'Active 9M'
               WHEN (days_since_last_spv >= 270 AND days_since_last_spv < 450) OR
                    (days_since_last_booking >= 270 AND days_since_last_booking < 450) OR
                    (days_since_last_emailclick >= 270 AND days_since_last_emailclick < 450) OR
                    (days_since_last_emailopen >= 270 AND days_since_last_emailopen < 450) THEN 'Active 15M'
               WHEN (days_since_last_spv >= 450 AND days_since_last_spv < 720) OR
                    (days_since_last_booking >= 450 AND days_since_last_booking < 720) OR
                    (days_since_last_emailclick >= 450 AND days_since_last_emailclick < 720) OR
                    (days_since_last_emailopen >= 450 AND days_since_last_emailopen < 720) THEN 'Active 24M'
               ELSE 'Dead' END                      AS engagement_group,
           CASE
               WHEN days_since_last_spv <= 7 OR days_since_last_booking <= 7 OR days_since_last_emailclick <= 7 OR
                    days_since_last_emailopen <= 7 THEN 'Active 7d'
               WHEN (days_since_last_spv > 7 AND days_since_last_spv <= 14) OR
                    (days_since_last_booking > 7 AND days_since_last_booking <= 14) OR
                    (days_since_last_emailclick > 7 AND days_since_last_emailclick <= 14) OR
                    (days_since_last_emailopen > 7 AND days_since_last_emailopen <= 14) THEN 'Active 7-14d'
               WHEN (days_since_last_spv > 14 AND days_since_last_spv < 30) OR
                    (days_since_last_booking > 14 AND days_since_last_booking < 30) OR
                    (days_since_last_emailclick > 14 AND days_since_last_emailclick < 30) OR
                    (days_since_last_emailopen > 14 AND days_since_last_emailopen < 30) THEN 'Active 14-30d'
               WHEN days_since_last_spv >= 30 OR days_since_last_booking >= 30 OR days_since_last_emailclick >= 30 OR
                    days_since_last_emailopen >= 30 THEN 'Active 1M+'
               ELSE 'Other' END                     AS group_breakdown,
           member_original_affiliate_classification AS original_affiliate

    FROM collab.user_eng_segments.aggregate_post2018activity
);

CREATE OR REPLACE TRANSIENT TABLE collab.user_eng_segments.engagement_source AS (
    SELECT *,
           LAG(engagement_group, 1) OVER (PARTITION BY member_id ORDER BY calendar_date) AS last_engagement,
           CASE WHEN member_age < 30 THEN 'New' ELSE 'Old' END                           AS new
    FROM collab.user_eng_segments.engagement_classification
);

CREATE OR REPLACE TRANSIENT TABLE collab.user_eng_segments.engagement_source_grouped COPY GRANTS AS (
    SELECT *,
           CASE
               WHEN last_engagement = 'Active 3M' OR last_engagement = 'Active 6M' THEN '3/6M'
               WHEN last_engagement = 'Active 9M' OR last_engagement = 'Active 15M' OR last_engagement = 'Active 24M'
                   THEN '9M+'
               WHEN last_engagement = 'Dead' THEN 'Dead'
               WHEN new = 'New' THEN 'New_member'
               ELSE '1M' END AS reactivation_source
    FROM collab.user_eng_segments.engagement_source
);

CREATE OR REPLACE TRANSIENT TABLE collab.user_eng_segments.active_base_segments COPY GRANTS AS (
    SELECT calendar_date,
           engagement_group,
           group_breakdown,
           reactivation_source,
           last_subscription_status,
           territory,
           original_affiliate,
           new,
           count(DISTINCT member_id)    AS members,
           avg(member_age)              AS average_member_age,
           sum(spv_count)               AS spvs,
           sum(email_opens_count)       AS emailopens,
           sum(email_clicks_count)      AS emailclicks,
           sum(booking_completed_count) AS bookings,
           sum(booking_margin)          AS margins
    FROM collab.user_eng_segments.engagement_source_grouped
    WHERE extract(YEAR FROM calendar_date) >= 2019
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
);



GRANT USAGE ON SCHEMA collab.user_eng_segments TO ROLE personal_role__roseyin;
GRANT USAGE ON SCHEMA collab.user_eng_segments TO ROLE personal_role__kirstengrieve;
GRANT USAGE ON SCHEMA collab.user_eng_segments TO ROLE personal_role__gianniraftis;
GRANT USAGE ON SCHEMA collab.user_eng_segments TO ROLE personal_role__richardkunert;
GRANT USAGE ON SCHEMA collab.user_eng_segments TO ROLE personal_role__niroshanbalakumar;

GRANT SELECT ON ALL TABLES IN SCHEMA collab.user_eng_segments TO ROLE personal_role__roseyin;
GRANT SELECT ON ALL TABLES IN SCHEMA collab.user_eng_segments TO ROLE personal_role__kirstengrieve;
GRANT SELECT ON ALL TABLES IN SCHEMA collab.user_eng_segments TO ROLE personal_role__gianniraftis;
GRANT SELECT ON ALL TABLES IN SCHEMA collab.user_eng_segments TO ROLE personal_role__richardkunert;
GRANT SELECT ON ALL TABLES IN SCHEMA collab.user_eng_segments TO ROLE personal_role__niroshanbalakumar;