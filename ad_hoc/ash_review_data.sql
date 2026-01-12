--1. review level - customer information
SELECT ubr.se_sale_id,
       ubr.territory,
       ubr.booking_id,
       ubr.review_date,
       COALESCE(ssa.company_name, t.concept_name) AS company_concept_name,
       ubr.customer_score,
       ubr.review_type,
       ubr.follow_up_answer,
       sua.first_name,
       sua.email,
       fb.gross_revenue_gbp,
       fb.margin_gross_of_toms_gbp,
       us.margin_segment
FROM se.data.user_booking_review ubr
    INNER JOIN se.data.fact_booking fb ON ubr.booking_id = fb.booking_id
    INNER JOIN se.data.dim_sale ds ON ubr.se_sale_id = ds.se_sale_id
    LEFT JOIN  se.data.se_sale_attributes ssa ON ubr.se_sale_id = ssa.se_sale_id
    LEFT JOIN  se.data.tb_offer t ON ubr.se_sale_id = t.se_sale_id
    INNER JOIN se.data_pii.se_user_attributes sua ON fb.shiro_user_id = sua.shiro_user_id
    INNER JOIN se.data.user_segmentation us ON sua.shiro_user_id = us.shiro_user_id AND us.date = CURRENT_DATE - 1
WHERE ubr.survey_source = 'survey_sparrow'
  AND review_type = 'detractor'
;


-- --agg to company name, review month, se_sale_id, territory,
-- SELECT COALESCE(ssa.company_name, t.concept_name)                                     AS company_concept_name,
--        DATE_TRUNC('month', ubr.review_date)                                           AS review_month,
--        ubr.se_sale_id,
--        ubr.territory,
--        COALESCE(ssa.nps_score, t.nps_score)                                           AS lifetime_nps_score,
--        COUNT(*)                                                                       AS total_reviews,
--        SUM(IFF(ubr.review_type = 'detractor', 1, 0))                                  AS no_detractor_reviews,
--        SUM(IFF(ubr.review_type = 'passive', 1, 0))                                    AS no_passive_reviews,
--        SUM(IFF(ubr.review_type = 'promoter', 1, 0))                                   AS no_promoter_reviews,
--        (no_promoter_reviews / total_reviews) - (no_detractor_reviews / total_reviews) AS nps
-- FROM se.data.user_booking_review ubr
--     LEFT JOIN se.data.se_sale_attributes ssa ON ubr.se_sale_id = ssa.se_sale_id
--     LEFT JOIN se.data.tb_offer t ON ubr.se_sale_id = t.se_sale_id
-- GROUP BY 1, 2, 3, 4, 5
-- ;

------------------------------------------------------------------------------------------------------------------------

--top 20 for current month, anything that scored 0 to -100
SET month_filter = '2021-10-01';
WITH summary AS (
    SELECT COALESCE(ssa.company_name, t.concept_name)                                     AS company_concept_name,
           DATE_TRUNC('month', ubr.review_date)                                           AS review_month,
           ubr.se_sale_id,
           ubr.territory,
           COALESCE(ssa.nps_score, t.nps_score)                                           AS lifetime_nps_score,
           COUNT(*)                                                                       AS total_reviews,
           SUM(IFF(ubr.review_type = 'detractor', 1, 0))                                  AS no_detractor_reviews,
           SUM(IFF(ubr.review_type = 'passive', 1, 0))                                    AS no_passive_reviews,
           SUM(IFF(ubr.review_type = 'promoter', 1, 0))                                   AS no_promoter_reviews,
           (no_promoter_reviews / total_reviews) - (no_detractor_reviews / total_reviews) AS nps
    FROM se.data.user_booking_review ubr
        LEFT JOIN se.data.se_sale_attributes ssa ON ubr.se_sale_id = ssa.se_sale_id
        LEFT JOIN se.data.tb_offer t ON ubr.se_sale_id = t.se_sale_id
    GROUP BY 1, 2, 3, 4, 5
),
     top_20 AS (
         --filter results to work out top 20
         SELECT *
         FROM summary s
         WHERE s.review_month = $month_filter --insert first of month
           AND s.total_reviews > 10
           AND s.nps <= 0
         ORDER BY nps ASC
         LIMIT 20 --top 20
     ),
     margin_aggs AS (
         --create aggregated margin for the top 20 filter
         SELECT COALESCE(ssa.company_name, t.concept_name)                                                                         AS company_concept_name,
                SUM(fcb.margin_gross_of_toms_gbp)                                                                                  AS lifetime_margin,
                SUM(IFF(DATE_TRUNC('month', fcb.booking_completed_timestamp) = $month_filter, fcb.margin_gross_of_toms_gbp, NULL)) AS month_margin
         FROM se.data.fact_complete_booking fcb
             LEFT JOIN se.data.se_sale_attributes ssa ON fcb.se_sale_id = ssa.se_sale_id
             LEFT JOIN se.data.tb_offer t ON fcb.se_sale_id = t.se_sale_id
         WHERE COALESCE(ssa.company_name, t.concept_name) IN (
             SELECT company_concept_name
             FROM top_20
         )
         GROUP BY 1
     )
SELECT t.company_concept_name,
       t.review_month,
       t.se_sale_id,
       t.territory,
       t.lifetime_nps_score,
       t.total_reviews,
       t.no_detractor_reviews,
       t.no_passive_reviews,
       t.no_promoter_reviews,
       t.nps,
       m.lifetime_margin,
       m.month_margin
FROM top_20 t
    INNER JOIN margin_aggs m ON t.company_concept_name = m.company_concept_name
;

--list of reviews for companies with lowest nps score and have more than 10 reviews
WITH summary AS (
    SELECT COALESCE(ssa.company_name, t.concept_name)                                     AS company_concept_name,
           DATE_TRUNC('month', ubr.review_date)                                           AS review_month,
           ubr.se_sale_id,
           ubr.territory,
           COALESCE(ssa.nps_score, t.nps_score)                                           AS lifetime_nps_score,
           COUNT(*)                                                                       AS total_reviews,
           SUM(IFF(ubr.review_type = 'detractor', 1, 0))                                  AS no_detractor_reviews,
           SUM(IFF(ubr.review_type = 'passive', 1, 0))                                    AS no_passive_reviews,
           SUM(IFF(ubr.review_type = 'promoter', 1, 0))                                   AS no_promoter_reviews,
           (no_promoter_reviews / total_reviews) - (no_detractor_reviews / total_reviews) AS nps
    FROM se.data.user_booking_review ubr
        LEFT JOIN se.data.se_sale_attributes ssa ON ubr.se_sale_id = ssa.se_sale_id
        LEFT JOIN se.data.tb_offer t ON ubr.se_sale_id = t.se_sale_id
    GROUP BY 1, 2, 3, 4, 5
),
     top_20 AS (
         --filter results to work out top 20
         SELECT *
         FROM summary s
         WHERE s.review_month = $month_filter --insert first of month
           AND s.total_reviews > 10
           AND s.nps <= 0
         ORDER BY nps ASC
         LIMIT 20 --top 20
     )
SELECT COALESCE(ssa.company_name, t.concept_name) AS company_concept_name,
       *
FROM se.data.user_booking_review ubr
    LEFT JOIN se.data.se_sale_attributes ssa ON ubr.se_sale_id = ssa.se_sale_id
    LEFT JOIN se.data.tb_offer t ON ubr.se_sale_id = t.se_sale_id
WHERE COALESCE(ssa.company_name, t.concept_name) IN (
    SELECT company_concept_name
    FROM top_20
)
  AND DATE_TRUNC('month', ubr.review_date) = $month_filter;

------------------------------------------------------------------------------------------------------------------------
--top 20 summary based on company revenue
SET month_filter = '2021-10-01';
WITH summary AS (
    SELECT COALESCE(ssa.company_name, t.concept_name)                                     AS company_concept_name,
           DATE_TRUNC('month', ubr.review_date)                                           AS review_month,
           ubr.se_sale_id,
           COALESCE(ssa.nps_score, t.nps_score)                                           AS lifetime_nps_score,
           COUNT(*)                                                                       AS total_reviews,
           SUM(IFF(ubr.review_type = 'detractor', 1, 0))                                  AS no_detractor_reviews,
           SUM(IFF(ubr.review_type = 'passive', 1, 0))                                    AS no_passive_reviews,
           SUM(IFF(ubr.review_type = 'promoter', 1, 0))                                   AS no_promoter_reviews,
           (no_promoter_reviews / total_reviews) - (no_detractor_reviews / total_reviews) AS nps
    FROM se.data.user_booking_review ubr
        LEFT JOIN se.data.se_sale_attributes ssa ON ubr.se_sale_id = ssa.se_sale_id
        LEFT JOIN se.data.tb_offer t ON ubr.se_sale_id = t.se_sale_id
    GROUP BY 1, 2, 3, 4
),
     margin_aggs AS (
         --create aggregated margin for the top 20 filter
         SELECT COALESCE(ssa.company_name, t.concept_name)                                                                         AS company_concept_name,
                SUM(fcb.margin_gross_of_toms_gbp)                                                                                  AS lifetime_margin,
                SUM(IFF(DATE_TRUNC('month', fcb.booking_completed_timestamp) = $month_filter, fcb.margin_gross_of_toms_gbp, NULL)) AS month_margin
         FROM se.data.fact_complete_booking fcb
             LEFT JOIN se.data.se_sale_attributes ssa ON fcb.se_sale_id = ssa.se_sale_id
             LEFT JOIN se.data.tb_offer t ON fcb.se_sale_id = t.se_sale_id
         WHERE COALESCE(ssa.company_name, t.concept_name) IN (
             SELECT company_concept_name
             FROM summary
         )
         GROUP BY 1
         HAVING month_margin > 0
         ORDER BY month_margin DESC
         LIMIT 20
     )

SELECT s.company_concept_name,
       s.review_month,
       s.se_sale_id,
       s.lifetime_nps_score,
       s.total_reviews,
       s.no_detractor_reviews,
       s.no_passive_reviews,
       s.no_promoter_reviews,
       s.nps,
       m.lifetime_margin,
       m.month_margin
FROM margin_aggs m
    INNER JOIN summary s ON m.company_concept_name = s.company_concept_name
WHERE s.review_month = $month_filter
;

--list of reviews for companies with highest revenue within month
WITH summary AS (
    SELECT COALESCE(ssa.company_name, t.concept_name)                                     AS company_concept_name,
           DATE_TRUNC('month', ubr.review_date)                                           AS review_month,
           ubr.se_sale_id,
           COALESCE(ssa.nps_score, t.nps_score)                                           AS lifetime_nps_score,
           COUNT(*)                                                                       AS total_reviews,
           SUM(IFF(ubr.review_type = 'detractor', 1, 0))                                  AS no_detractor_reviews,
           SUM(IFF(ubr.review_type = 'passive', 1, 0))                                    AS no_passive_reviews,
           SUM(IFF(ubr.review_type = 'promoter', 1, 0))                                   AS no_promoter_reviews,
           (no_promoter_reviews / total_reviews) - (no_detractor_reviews / total_reviews) AS nps
    FROM se.data.user_booking_review ubr
        LEFT JOIN se.data.se_sale_attributes ssa ON ubr.se_sale_id = ssa.se_sale_id
        LEFT JOIN se.data.tb_offer t ON ubr.se_sale_id = t.se_sale_id
    GROUP BY 1, 2, 3, 4
),
     margin_aggs AS (
         --create aggregated margin for the top 20 filter
         SELECT COALESCE(ssa.company_name, t.concept_name)                                                                         AS company_concept_name,
                SUM(fcb.margin_gross_of_toms_gbp)                                                                                  AS lifetime_margin,
                SUM(IFF(DATE_TRUNC('month', fcb.booking_completed_timestamp) = $month_filter, fcb.margin_gross_of_toms_gbp, NULL)) AS month_margin
         FROM se.data.fact_complete_booking fcb
             LEFT JOIN se.data.se_sale_attributes ssa ON fcb.se_sale_id = ssa.se_sale_id
             LEFT JOIN se.data.tb_offer t ON fcb.se_sale_id = t.se_sale_id
         WHERE COALESCE(ssa.company_name, t.concept_name) IN (
             SELECT company_concept_name
             FROM summary
         )
         GROUP BY 1
         HAVING month_margin > 0
         ORDER BY month_margin DESC
         LIMIT 20
     )

SELECT COALESCE(ssa.company_name, t.concept_name) AS company_concept_name,
       *
FROM se.data.user_booking_review ubr
    LEFT JOIN se.data.se_sale_attributes ssa ON ubr.se_sale_id = ssa.se_sale_id
    LEFT JOIN se.data.tb_offer t ON ubr.se_sale_id = t.se_sale_id
WHERE COALESCE(ssa.company_name, t.concept_name) IN (
    SELECT company_concept_name
    FROM margin_aggs
)
  AND DATE_TRUNC('month', ubr.review_date) = $month_filter
;