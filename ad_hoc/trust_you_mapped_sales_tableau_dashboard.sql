SELECT
	m.hotel_code,
	m.trust_you_id,
	s.se_sale_id,
	s.score,
	s.one_star_reviews,
	s.two_star_reviews,
	s.three_star_reviews,
	s.four_star_reviews,
	s.five_star_reviews,
	s.reviews_count,
	s.review_strapline,
	s.good_to_know_list,
	s.good_to_know_pos_sentiments,
	s.good_to_know_neutral_sentiments,
	s.good_to_know_mixed_sentiments,
	s.good_to_know_neg_sentiments,
	s.category_list,
	s.sentiment_list,
	s.sentiments,
	s.pos_sentiments,
	s.neutral_sentiments,
	s.mixed_sentiments,
	s.neg_sentiments
FROM dbt.bi_data_platform__intermediate.dp_reviews_03_meta_review_score_modelling s
	INNER JOIN dbt.bi_data_platform__intermediate.dp_reviews_02_trust_you_mapped_sales m
			   ON s.se_sale_id = m.se_sale_id
				   AND m.mapping_version = 'v6'
;

WITH
	input AS (
		SELECT
			m.hotel_code,
			m.trust_you_id,
			m.start_date AS mapped_start_date,
-- 			s.se_sale_id,
-- 			s.score,
-- 			s.one_star_reviews,
-- 			s.two_star_reviews,
-- 			s.three_star_reviews,
-- 			s.four_star_reviews,
-- 			s.five_star_reviews,
-- 			s.reviews_count,
-- 			s.review_strapline,
-- 			s.good_to_know_list,
-- 			s.good_to_know_pos_sentiments,
-- 			s.good_to_know_neutral_sentiments,
-- 			s.good_to_know_mixed_sentiments,
-- 			s.good_to_know_neg_sentiments,
-- 			s.category_list,
-- 			s.sentiment_list,
-- 			s.sentiments,
-- 			s.pos_sentiments,
-- 			s.neutral_sentiments,
-- 			s.mixed_sentiments,
-- 			s.neg_sentiments,
			m.se_sale_id,
			m.start_date = (
				SELECT
					MAX(ms.start_date)
				FROM dbt.bi_data_platform__intermediate.dp_reviews_02_trust_you_mapped_sales ms
			)            AS current_mapping
		FROM dbt.bi_data_platform__intermediate.dp_reviews_02_trust_you_mapped_sales m
		-- 			INNER JOIN dbt.bi_data_platform__intermediate.dp_reviews_03_meta_review_score_modelling s
-- 					   ON s.se_sale_id = m.se_sale_id
		WHERE m.mapping_version = 'v6'
	)
SELECT *
FROM input i
WHERE i.current_mapping AND i.se_sale_id = 'A32677'
;


SELECT *
FROM dbt.bi_data_platform__intermediate.dp_reviews_03_meta_review_score_modelling
WHERE se_sale_id = 'A32677'


SELECT *
FROM dbt_dev.dbt_robinpatel_data_platform__intermediate.dp_reviews_03_meta_review_score_modelling
WHERE se_sale_id = 'A32677'

SELECT *
FROM dbt.bi_data_platform__intermediate.dp_reviews_02_trust_you_mapped_sales m
WHERE m.se_sale_id = 'A32677' AND m.start_date = CURRENT_DATE - 1

WITH
	input AS (
		SELECT DISTINCT
			m.trust_you_id,
			m.se_sale_id
		FROM dbt.bi_data_platform__intermediate.dp_reviews_02_trust_you_mapped_sales m
		WHERE m.start_date = (
			SELECT MAX(m2.start_date) FROM dbt.bi_data_platform__intermediate.dp_reviews_02_trust_you_mapped_sales m2
		)
	)
SELECT *
FROM input
WHERE input.se_sale_id = 'A32677'

------------------------------------------------------------------------------------------------------------------------
WITH
	input AS (
		SELECT
			m.hotel_code,
			m.trust_you_id,
			m.start_date AS mapped_start_date,
			s.se_sale_id,
			s.score / 10 AS score,
			s.one_star_reviews,
			s.two_star_reviews,
			s.three_star_reviews,
			s.four_star_reviews,
			s.five_star_reviews,
			s.reviews_count,
			s.review_strapline,
			s.good_to_know_list,
			s.good_to_know_pos_sentiments,
			s.good_to_know_neutral_sentiments,
			s.good_to_know_mixed_sentiments,
			s.good_to_know_neg_sentiments,
			s.category_list,
			s.sentiment_list,
			s.sentiments,
			s.pos_sentiments,
			s.neutral_sentiments,
			s.mixed_sentiments,
			s.neg_sentiments,
			ds.sale_active,
			ds.sale_start_date
		FROM dbt.bi_data_platform__intermediate.dp_reviews_02_trust_you_mapped_sales m
			INNER JOIN dbt.bi_data_platform__intermediate.dp_reviews_03_meta_review_score_modelling s
					   ON s.se_sale_id = m.se_sale_id
			LEFT JOIN  se.data.dim_sale ds ON m.se_sale_id = ds.se_sale_id
-- 	LEFT JOIN  data_vault_mvp.dwh.sfsc__account sa ON
		WHERE m.mapping_version = 'v6' AND
			  m.start_date = (
				  SELECT
					  MAX(ms.start_date)
				  FROM dbt.bi_data_platform__intermediate.dp_reviews_02_trust_you_mapped_sales ms
			  )
	)
SELECT
	input.hotel_code,
	COUNT(input.sale_active)
FROM input
GROUP BY 1
HAVING COUNT(input.sale_active) = 0
;


SELECT
	m.hotel_code,
	m.trust_you_id,
	m.start_date AS mapped_start_date,
	gsa.account_name,
	gsa.stage_name,
	IFF(ds.data_model = 'New Data Model', ds.posa_territory, NULL) AS posa_territory,
	s.se_sale_id,
	s.score / 10 AS score,
	s.one_star_reviews,
	s.two_star_reviews,
	s.three_star_reviews,
	s.four_star_reviews,
	s.five_star_reviews,
	s.reviews_count,
	s.review_strapline,
	s.good_to_know_list,
	s.good_to_know_pos_sentiments,
	s.good_to_know_neutral_sentiments,
	s.good_to_know_mixed_sentiments,
	s.good_to_know_neg_sentiments,
	s.category_list,
	s.sentiment_list,
	s.sentiments,
	s.pos_sentiments,
	s.neutral_sentiments,
	s.mixed_sentiments,
	s.neg_sentiments,
	ds.sale_active,
	ds.sale_start_date
FROM dbt.bi_data_platform__intermediate.dp_reviews_02_trust_you_mapped_sales m
	INNER JOIN dbt.bi_data_platform__intermediate.dp_reviews_03_meta_review_score_modelling s
			   ON s.se_sale_id = m.se_sale_id
	LEFT JOIN  se.data.dim_sale ds ON m.se_sale_id = ds.se_sale_id
	LEFT JOIN  se.data.global_sale_attributes gsa ON ds.salesforce_opportunity_id = gsa.global_sale_id
WHERE m.mapping_version = 'v6' AND
	  m.start_date = (
		  SELECT MAX(ms.start_date) FROM dbt.bi_data_platform__intermediate.dp_reviews_02_trust_you_mapped_sales ms
	  )
;


SELECT *
FROM se.data.se_sale_attributes ssa
WHERE ssa.sale_active
;

SELECT *
FROM data_vault_mvp.dwh.sfsc__account sa
WHERE id = '001w000001N8Y46AAF'
;

SELECT *
FROM se.data.dim_sale ds
;

SELECT *
FROM se.data.global_sale_attributes gsa
WHERE gsa.global_sale_id = '0066900001iQgQG'
;

SELECT *
FROM se.data.se_sale_attributes ssa
WHERE se_sale_id = 'A61181'
;

SELECT *
FROM se.data.dim_sale
WHERE se_sale_id = 'A61181'