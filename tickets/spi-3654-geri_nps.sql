WITH company_review AS (
    SELECT
        ssa.company_name,
        ssa.posa_territory,
        COUNT(DISTINCT ubr.booking_id)                    AS no_of_reviews,
        COUNT_IF(ubr.customer_score >= 9)                 AS no_promoter_reviews,
        COUNT_IF(ubr.customer_score BETWEEN 7 AND 8)      AS no_passive_reviews,
        COUNT_IF(ubr.customer_score <= 6)                 AS no_detractor_reviews,
        AVG(customer_score)                               AS avg_review_score,
        --https://www.qualtrics.com/uk/experience-management/customer/measure-nps/?rid=ip&prevsite=en&newsite=uk&geo=GB&geomatch=uk
        (no_promoter_reviews / NULLIF(no_of_reviews, 0)) -
        (no_detractor_reviews / NULLIF(no_of_reviews, 0)) AS nps_score
    FROM se.data.se_sale_attributes ssa
        LEFT JOIN se.data.user_booking_review ubr ON ssa.se_sale_id = ubr.se_sale_id
    WHERE ssa.sale_active
      AND ssa.product_configuration IN (
                                        'Hotel',
                                        'Hotel Plus'
        )
    GROUP BY 1, 2
)
SELECT
    cr.posa_territory,
    COUNT(*)
FROM company_review cr
WHERE cr.nps_score > 0.8
GROUP BY 1;

SELECT
    ssa.posa_territory,
    COUNT(DISTINCT ssa.company_name)
FROM se.data.se_sale_attributes ssa
WHERE ssa.sale_active
  AND ssa.product_configuration IN (
                                    'Hotel',
                                    'Hotel Plus'
    )
GROUP BY 1;



SELECT
    ssa.company_name,
    COUNT(DISTINCT ubr.booking_id)                    AS no_of_reviews,
    COUNT_IF(ubr.customer_score >= 9)                 AS no_promoter_reviews,
    COUNT_IF(ubr.customer_score BETWEEN 7 AND 8)      AS no_passive_reviews,
    COUNT_IF(ubr.customer_score <= 6)                 AS no_detractor_reviews,
    AVG(customer_score)                               AS avg_review_score,
    --https://www.qualtrics.com/uk/experience-management/customer/measure-nps/?rid=ip&prevsite=en&newsite=uk&geo=GB&geomatch=uk
    (no_promoter_reviews / NULLIF(no_of_reviews, 0)) -
    (no_detractor_reviews / NULLIF(no_of_reviews, 0)) AS nps_score
FROM se.data.se_sale_attributes ssa
    LEFT JOIN se.data.user_booking_review ubr ON ssa.se_sale_id = ubr.se_sale_id
WHERE ssa.sale_active
  AND ssa.product_configuration IN (
                                    'Hotel',
                                    'Hotel Plus'
    )
GROUP BY 1