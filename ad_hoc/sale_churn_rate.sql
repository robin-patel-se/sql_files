-- weekly sales
USE WAREHOUSE pipe_xlarge
;

CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.sale_churn_base_data AS (
	WITH
		weekly_calendar AS (
			SELECT
				date_value
			FROM se.data.se_calendar sc
			WHERE sc.day_name = 'Sun'
			  AND sc.date_value BETWEEN '2022-02-04'
				AND DATE_TRUNC(WEEK, CURRENT_DATE)
		),
		distinct_sales AS (
			SELECT
				dss.se_sale_id,
				MIN(dss.view_date) AS first_view_date,
				MAX(dss.view_date) AS last_view_date
			FROM se.data.dim_sale_snapshot dss
			WHERE dss.sale_active
			  AND DAYNAME(dss.view_date) = 'Sun'
			  AND dss.se_brand = 'SE Brand'
-- 			  AND dss.se_sale_id = 'A61831'
			GROUP BY 1
		),
		grain AS (
			SELECT
				wc.date_value,
				ds.se_sale_id,
				ds.first_view_date,
				ds.last_view_date
			FROM weekly_calendar wc
			CROSS JOIN distinct_sales ds
		)
	SELECT
		g.date_value,
		g.se_sale_id AS grain_se_sale_id,
		g.first_view_date,
		g.last_view_date,
		d.view_date,
		d.se_sale_id,
		d.sale_name,
		d.sale_product,
		d.sale_type,
		d.product_type,
		d.product_configuration,
		d.product_line,
		d.supplier_id,
		d.supplier_name,
		d.partner_id,
		d.partner_title,
		d.data_model,
		d.sale_start_date,
		d.sale_end_date,
		d.sale_active,
		d.posa_territory,
		d.posa_country,
		d.posu_country,
		d.posu_division,
		d.posu_city,
		d.travel_type,
		d.target_account_list,
		d.posu_sub_region,
		d.posu_region,
		d.posu_cluster,
		d.posu_cluster_region,
		d.posu_cluster_sub_region,
		d.cm_region,
		d.salesforce_opportunity_id,
		d.se_brand,
		d.array_sale_translation,
		d.posa_territory_id,
		d.tech_platform
	FROM grain g
	LEFT JOIN se.data.dim_sale_snapshot d
		ON g.date_value = d.view_date AND g.se_sale_id = d.se_sale_id
)
;


SELECT
	dss.se_sale_id,
	MIN(dss.view_date),
	MAX(dss.view_date)
FROM se.data.dim_sale_snapshot dss
WHERE DAYNAME(dss.view_date) = 'Sun'
  AND dss.se_brand = 'SE Brand'
  AND dss.sale_active
GROUP BY 1
;


SELECT
	scbd.date_value,
	scbd.grain_se_sale_id AS se_sale_id,
	scbd.first_view_date,
	scbd.last_view_date,
	scbd.sale_active
FROM scratch.robinpatel.sale_churn_base_data scbd
WHERE grain_se_sale_id = 'A61831'
;



SELECT
	ds.posu_city,
	SUM(fcb.margin_gross_of_toms_gbp_constant_currency) AS margin,
	COUNT(DISTINCT fcb.booking_id)                      AS bookings
FROM se.data.fact_complete_booking fcb
INNER JOIN se.data.dim_sale ds
	ON fcb.se_sale_id = ds.se_sale_id
WHERE YEAR(fcb.booking_completed_timestamp) = 2025
  AND fcb.se_brand = 'SE Brand'
GROUP BY 1
ORDER BY 3 DESC
;


SELECT
	ds.*
FROM se.data.sale_active sa
INNER JOIN se.data.dim_sale ds
	ON sa.se_sale_id = ds.se_sale_id
;


SELECT
	DATE_TRUNC(MONTH, sa.view_date) AS month,
	COUNT(DISTINCT sa.se_sale_id)
FROM se.data.sale_active sa
INNER JOIN se.data.dim_sale ds
	ON sa.se_sale_id = ds.se_sale_id
WHERE ds.se_brand = 'SE Brand'
GROUP BY 1