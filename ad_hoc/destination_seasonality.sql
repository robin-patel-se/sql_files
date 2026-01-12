WITH
	month_aggs AS (
		SELECT
			DATE_TRUNC(MONTH, fsm.date) AS month,
			dst.posu_country,
			dst.posa_territory,
			SUM(fsm.spvs)               AS spvs
		FROM se.bi.fact_sale_metrics fsm
			LEFT JOIN se.bi.dim_sale_territory dst
					  ON fsm.se_sale_id = dst.se_sale_id AND fsm.posa_territory = dst.posa_territory
		WHERE fsm.date BETWEEN '2023-01-01' AND '2023-10-31'

		GROUP BY 1, 2, 3
	),
	top_ten_countries_by_month AS (
		SELECT
			ma.month,
			ma.posu_country,
			ma.spvs,
			ma.posa_territory,
			ROW_NUMBER() OVER (PARTITION BY ma.month, ma.posa_territory ORDER BY ma.spvs DESC) < 10 AS top_ten
		FROM month_aggs ma
	),
	top_ten_countries AS (
		SELECT
			ttcbm.posu_country,
			ttcbm.posa_territory,
			MAX(top_ten) AS is_top_ten
		FROM top_ten_countries_by_month ttcbm
		GROUP BY 1, 2
	),
	-- athena logic
	------------------------------------------------------------------------------------------------------------------------
	sale_logic AS (
		SELECT
			ds.se_sale_id,
			ds.posu_country,
			gsa.deal_category,
			ds.posa_territory
		FROM se.data.dim_sale ds
			INNER JOIN se.data.global_sale_attributes gsa
					   ON ds.salesforce_opportunity_id = gsa.global_sale_id
	),
	athena_planning_users AS (
		SELECT
			dds.planning_date,
			sl.posu_country,
			sl.deal_category,
			sl.posa_territory,
			dds.deal_id,
			COUNT(DISTINCT user_id) AS athena_users
		FROM data_science.operational_output.daily_deals_selections dds
			INNER JOIN sale_logic sl ON sl.se_sale_id = dds.deal_id
		WHERE dds.planning_date BETWEEN '2023-01-01' AND '2023-10-31'
		  AND dds.planning_position <= 9
		GROUP BY 1, 2, 3, 4, 5
	)

------------------------------------------------------------------------------------------------------------------------
		,
	artemis_planning_users AS (
		SELECT
			dds.last_modified_ts::DATE AS planning_date,
			sl.posu_country,
			sl.deal_category,
			sl.posa_territory,
			dds.rec_deal_id            AS deal_id,
			COUNT(DISTINCT user_id)    AS artemis_users
		FROM data_science.operational_output.selections_conversion dds
			INNER JOIN sale_logic sl ON sl.se_sale_id = dds.rec_deal_id
		-- artemis went live in March 23
		WHERE dds.last_modified_ts::DATE BETWEEN '2023-03-01' AND '2023-10-31'
		  AND dds.planning_position <= 9
		GROUP BY 1, 2, 3, 4, 5
	)

SELECT
	fsm.se_sale_id,
	fsm.date,
	fsm.posa_territory,
	dst.deal_category,
	dst.posu_country,
	fsm.spvs,
	fsm.trx,
	fsm.margin_constant_currency,
	dst.travel_type,
	ttc.is_top_ten,
	apu.athena_users  AS athena_recommended_users,
	aru.artemis_users AS artemis_recommended_users
FROM se.bi.fact_sale_metrics fsm
	LEFT JOIN se.bi.dim_sale_territory dst
			  ON fsm.se_sale_id = dst.se_sale_id AND fsm.posa_territory = dst.posa_territory
	LEFT JOIN top_ten_countries ttc ON dst.posu_country = ttc.posu_country AND dst.posa_territory = ttc.posa_territory
	LEFT JOIN athena_planning_users apu ON fsm.se_sale_id = apu.deal_id AND fsm.posa_territory = apu.posa_territory AND
										   fsm.date = apu.planning_date
	LEFT JOIN artemis_planning_users aru ON fsm.se_sale_id = aru.deal_id AND fsm.posa_territory = aru.posa_territory AND
											fsm.date = aru.planning_date
WHERE fsm.date BETWEEN '2023-01-01' AND '2023-10-31'
  AND dst.posa_territory IS DISTINCT FROM 'PL'
;


USE WAREHOUSE pipe_xlarge
;


CREATE OR REPLACE TRANSIENT TABLE se.bi.
	sale_logic AS (
	SELECT
		ds.se_sale_id,
		ds.posu_country,
		gsa.deal_category,
		ds.posa_territory
	FROM se.data.dim_sale ds
		INNER JOIN se.data.global_sale_attributes gsa
				   ON ds.salesforce_opportunity_id = gsa.global_sale_id
),
	athena_planning_users AS (
		SELECT
			dds.planning_date,
			sl.posu_country,
			sl.deal_category,
			sl.posa_territory,
			dds.deal_id,
			COUNT(DISTINCT user_id) AS USERS
		FROM data_science.operational_output.daily_deals_selections dds
			INNER JOIN sale_logic sl ON sl.se_sale_id = dds.deal_id
		WHERE dds.last_modified_ts::DATE + 1 = dds.planning_date -- plan for the next day only (athena plans for 3 days)
		  AND dds.planning_date BETWEEN '2023-01-01' AND '2023-10-31'
		GROUP BY 1, 2, 3, 4, 5
	)
;

SELECT *
FROM se.bi.dim_sale_territory dst
WHERE dst.deal_category = 'Combi'
;



WITH
	sale_logic AS (
		SELECT
			ds.se_sale_id,
			ds.posu_country,
			gsa.deal_category,
			ds.posa_territory
		FROM se.data.dim_sale ds
			INNER JOIN se.data.global_sale_attributes gsa
					   ON ds.salesforce_opportunity_id = gsa.global_sale_id
	)
SELECT
	dds.last_modified_ts::DATE AS planning_date,
	sl.posu_country,
	sl.deal_category,
	sl.posa_territory,
	dds.rec_deal_id,
	COUNT(DISTINCT user_id)    AS users
FROM data_science.operational_output.selections_conversion dds
	INNER JOIN sale_logic sl ON sl.se_sale_id = dds.rec_deal_id
WHERE dds.last_modified_ts::DATE BETWEEN '2023-01-01' AND '2023-10-31'
GROUP BY 1, 2, 3, 4, 5
;



WITH
	sale_logic AS (
		SELECT
			ds.se_sale_id,
			ds.posu_country,
			gsa.deal_category,
			ds.posa_territory
		FROM se.data.dim_sale ds
			INNER JOIN se.data.global_sale_attributes gsa
					   ON ds.salesforce_opportunity_id = gsa.global_sale_id
	)
SELECT
	dds.planning_date,
	sl.posu_country,
	sl.deal_category,
	sl.posa_territory,
	dds.deal_id,
	COUNT(DISTINCT user_id) AS athena_users
FROM data_science.operational_output.daily_deals_selections dds
	INNER JOIN sale_logic sl ON sl.se_sale_id = dds.deal_id
WHERE dds.planning_date BETWEEN '2023-01-01' AND '2023-10-31'
  AND dds.planning_position <= 9
GROUP BY 1, 2, 3, 4, 5