USE ROLE tableau
;

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
			   ON s.se_sale_id = m.se_sale_id AND m.mapping_version = 'v4'
;

SELECT *
FROM dbt.bi_data_platform__intermediate.dp_reviews_02_trust_you_mapped_sales


SELECT DISTINCT
	m.hotel_code,
	m.trust_you_id,
	s.score
FROM dbt.bi_data_platform__intermediate.dp_reviews_03_meta_review_score_modelling s
	INNER JOIN dbt.bi_data_platform__intermediate.dp_reviews_02_trust_you_mapped_sales m
			   ON s.se_sale_id = m.se_sale_id AND m.mapping_version = 'v4'
WHERE s.score >= 81
;


SELECT *
FROM se.data_pii.scv_page_screen_enrichment spse
;

SELECT mr.ty_id,
       mr.summary[0]['score']::NUMBER AS score
FROM latest_vault.trustyou.meta_review mr