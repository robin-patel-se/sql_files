USE WAREHOUSE pipe_xlarge
;

SELECT DISTINCT
	ds.salesforce_opportunity_id AS global_sale_id,
	ds.posa_territory,
	fsm.date                     AS date,
	SUM(fsm.margin_constant_currency)
		OVER (PARTITION BY ds.salesforce_opportunity_id, ds.posa_territory
			ORDER BY fsm.date)   AS cumulative_margin,
	SUM(fsm.spvs)
		OVER (PARTITION BY ds.salesforce_opportunity_id, ds.posa_territory
			ORDER BY fsm.date)   AS cumulative_spvs

FROM se.bi.fact_sale_metrics fsm
--FROM {{ ref('base_bi__fact_sale_metrics') }} fsm
	LEFT JOIN se.bi.dim_sale_territory ds
				  --LEFT JOIN {{ ref('base_bi__dim_sale_territory') }} ds
			  ON fsm.se_sale_id = ds.se_sale_id
				  AND fsm.posa_territory = ds.posa_territory
WHERE fsm.posa_territory != 'PL'
  AND fsm.posa_territory != 'Poland'
  AND ds.posa_territory = 'UK' --TODO remove
  AND ds.salesforce_opportunity_id = '006000000000012'
ORDER BY 1, 2, 3

WITH
	pre_2019_value AS (
		SELECT
			f.se_sale_id,
			dst.salesforce_opportunity_id,
			dst.posa_territory,
			dst.first_sale_start_date,
			'1970-01-01'                    AS date_value,
			SUM(f.spvs)                     AS spvs,
			SUM(f.margin_constant_currency) AS margin_constant_currency
		FROM se.bi.fact_sale_metrics f
			LEFT JOIN se.bi.dim_sale_territory dst
					  ON f.se_sale_id = dst.se_sale_id
						  AND f.posa_territory = dst.posa_territory
		WHERE f.date < '2019-01-01'
		GROUP BY 1, 2, 3, 4, 5
	)
		,
	grain AS (
		-- create a base grain for territory sales of every day since go live or
		-- '2019-01-01' whichever is more recent
		SELECT
			dst.se_sale_id,
			dst.salesforce_opportunity_id,
			dst.posa_territory,
			dst.first_sale_start_date,
			s.date_value,
			COALESCE(fsm.spvs, 0)                     AS spvs,
			COALESCE(fsm.margin_constant_currency, 0) AS margin_constant_currency
		FROM se.bi.dim_sale_territory dst
			LEFT JOIN se.data.se_calendar s ON GREATEST(dst.first_sale_start_date, '2019-01-01') <= s.date_value
			AND s.date_value <= CURRENT_DATE
			LEFT JOIN se.bi.fact_sale_metrics fsm ON s.date_value = fsm.date
			AND dst.se_sale_id = fsm.se_sale_id
			AND dst.posa_territory = fsm.posa_territory
		WHERE dst.salesforce_opportunity_id = '0066900001TCA5N' -- TODO remove
-- 		  AND dst.posa_territory = 'UK'                         -- TODO remove
		UNION ALL

		SELECT
			pv.se_sale_id,
			pv.salesforce_opportunity_id,
			pv.posa_territory,
			pv.first_sale_start_date,
			pv.date_value,
			pv.spvs,
			pv.margin_constant_currency
		FROM pre_2019_value pv
		WHERE pv.salesforce_opportunity_id = '0066900001TCA5N' -- TODO remove
-- 		  AND pv.posa_territory = 'UK' -- TODO remove
	)
		,
	cumulative AS (
		SELECT
			g.se_sale_id,
			g.salesforce_opportunity_id,
			g.posa_territory,
			g.first_sale_start_date,
			g.date_value,
			g.spvs,
			SUM(g.spvs) OVER (PARTITION BY g.se_sale_id ORDER BY g.date_value) AS cum_spvs,
			g.margin_constant_currency,
			SUM(g.margin_constant_currency)
				OVER (PARTITION BY g.se_sale_id ORDER BY g.date_value)         AS cum_margin_constant_currency
		FROM grain g
	)
SELECT *
FROM cumulative
WHERE cumulative.se_sale_id = 'A54810'
;

-- SELECT DISTINCT
-- 	ds.salesforce_opportunity_id AS global_sale_id,
-- 	ds.posa_territory,
-- 	fsm.date                     AS date,
-- 	SUM(fsm.margin_constant_currency)
-- 		OVER (PARTITION BY ds.salesforce_opportunity_id, ds.posa_territory
-- 			ORDER BY fsm.date)   AS cumulative_margin,
-- 	SUM(fsm.spvs)
-- 		OVER (PARTITION BY ds.salesforce_opportunity_id, ds.posa_territory
-- 			ORDER BY fsm.date)   AS cumulative_spvs
--
-- FROM se.bi.fact_sale_metrics fsm
-- --FROM {{ ref('base_bi__fact_sale_metrics') }} fsm
-- 	LEFT JOIN se.bi.dim_sale_territory ds
-- 				  --LEFT JOIN {{ ref('base_bi__dim_sale_territory') }} ds
-- 			  ON fsm.se_sale_id = ds.se_sale_id
-- 				  AND fsm.posa_territory = ds.posa_territory
-- WHERE fsm.posa_territory != 'PL'
--   AND fsm.posa_territory != 'Poland'
--   AND ds.posa_territory = 'UK' --TODO remove
--   AND ds.salesforce_opportunity_id = '006000000000012'
-- ORDER BY 1, 2, 3
-- ;

SELECT MIN(dss.view_date) FROM se.data.dim_sale_snapshot dss;

SELECT MIN(view_date) FROM se.data.sale_active sa;

SELECT dst.first_sale_start_date FROM se.bi.dim_sale_territory dst
