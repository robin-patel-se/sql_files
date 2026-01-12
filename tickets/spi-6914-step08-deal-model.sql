USE ROLE PERSONAL_ROLE__ROBINPATEL;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.dim_sale
AS SELECT * FROM data_vault_mvp.dwh.dim_sale;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.bi;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.dim_sale_territory
CLONE data_vault_mvp.bi.dim_sale_territory;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.fact_booking
AS SELECT * FROM data_vault_mvp.dwh.fact_booking;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.generic_targets
AS SELECT * FROM data_vault_mvp.dwh.generic_targets;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.global_sale_attributes
CLONE data_vault_mvp.dwh.global_sale_attributes;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.sale_active
CLONE data_vault_mvp.dwh.sale_active;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.se_calendar
AS SELECT * FROM data_vault_mvp.dwh.se_calendar;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_company_attributes
CLONE data_vault_mvp.dwh.se_company_attributes;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_sale
CLONE data_vault_mvp.dwh.se_sale;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.deal_count
CLONE data_vault_mvp.bi.deal_count;

self_describing_task \
    --include 'biapp.task_catalogue.dv.bi.tableau.deal_count_model.deal_count.py' \
    --method 'run' \
    --start '2025-01-08 00:00:00' \
    --end '2025-01-08 00:00:00'

------------------------------------------------------------------------------------------------------------------------

USE WAREHOUSE pipe_2xlarge
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.deal_count__step01__model_sale_table
AS (
	SELECT
		ds.se_sale_id,
		ds.salesforce_opportunity_id,
		ds.sale_start_date,
		LAST_VALUE(
				ss.company_id
		) OVER (
					PARTITION BY ds.salesforce_opportunity_id
					ORDER BY ds.sale_start_date ASC
					) AS company_id,
		LAST_VALUE(
				COALESCE(ss.company_name, 'Not in Salesforce')
		) OVER (
					PARTITION BY ds.salesforce_opportunity_id
					ORDER BY ds.sale_start_date ASC
					) AS company_name,
		LAST_VALUE(
				COALESCE(ss.current_contractor_name, 'Not in Salesforce')
		) OVER (
					PARTITION BY ds.salesforce_opportunity_id
					ORDER BY ds.sale_start_date ASC
					) AS current_contractor_name,
		ds.posu_cluster,
		-- uses se.bi.dim_sale as source table rather than sale attributes
		ds.posu_cluster_region,
		-- uses se.bi.dim_sale as source table rather than sale attributes
		LAST_VALUE(
				ds.posu_country
		) OVER (
					PARTITION BY ds.salesforce_opportunity_id
					ORDER BY ds.sale_start_date ASC
					) AS posu_country,
		LAST_VALUE(
				ds.posu_cluster_sub_region
		) OVER (
					PARTITION BY ds.salesforce_opportunity_id
					ORDER BY ds.sale_start_date ASC
					) AS posu_cluster_sub_region,
		ds.posa_territory,
		ds.product_type,
		-- uses se.bi.dim_sale as source table rather than sale attributes
		ds.product_configuration -- uses se.bi.dim_sale as source table rather than sale attributes
	FROM data_vault_mvp_dev_robin.dwh.dim_sale ds
		LEFT JOIN data_vault_mvp_dev_robin.dwh.se_sale ss
				  ON ds.se_sale_id = ss.se_sale_id
					  AND
					 ss.class IS DISTINCT FROM 'com.flashsales.sale.ConnectedWebRedirectSale' -- remove WRD catalogue sales
		LEFT JOIN data_vault_mvp_dev_robin.dwh.se_company_attributes sca
				  ON ss.company_id = sca.company_id::VARCHAR
		LEFT JOIN data_vault_mvp_dev_robin.dwh.global_sale_attributes gsa
				  ON ds.salesforce_opportunity_id = gsa.global_sale_id
)
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.deal_count__step02__model_margin
AS (
	SELECT
		dst.salesforce_opportunity_id,
		dst.posa_territory,
		SUM(fb.margin_gross_of_toms_gbp_constant_currency) AS margin_gbp_lifetime,
		SUM(
				IFF(
						DATE_TRUNC('YEAR', booking_completed_date) = '2019-01-01',
						fb.margin_gross_of_toms_gbp_constant_currency,
						0
				)
		)                                                  AS margin_gbp_2019,
		SUM(
				IFF(
						DATE_TRUNC('YEAR', booking_completed_date) = '2020-01-01',
						fb.margin_gross_of_toms_gbp_constant_currency,
						0
				)
		)                                                  AS margin_gbp_2020,
		SUM(
				IFF(
						DATE_TRUNC('YEAR', booking_completed_date) = '2021-01-01',
						fb.margin_gross_of_toms_gbp_constant_currency,
						0
				)
		)                                                  AS margin_gbp_2021,
		SUM(
				IFF(
						DATE_TRUNC('YEAR', booking_completed_date) = '2022-01-01',
						fb.margin_gross_of_toms_gbp_constant_currency,
						0
				)
		)                                                  AS margin_gbp_2022,
		SUM(
				IFF(
						DATE_TRUNC('YEAR', booking_completed_date) = '2023-01-01',
						fb.margin_gross_of_toms_gbp_constant_currency,
						0
				)
		)                                                  AS margin_gbp_2023,
		SUM(
				IFF(
						DATE_TRUNC('YEAR', booking_completed_date) = '2024-01-01',
						fb.margin_gross_of_toms_gbp_constant_currency,
						0
				)
		)                                                  AS margin_gbp_2024
	FROM data_vault_mvp_dev_robin.dwh.fact_booking fb
		LEFT JOIN data_vault_mvp_dev_robin.bi.dim_sale_territory dst
				  ON fb.se_sale_id = dst.se_sale_id
					  AND fb.territory = dst.posa_territory
	WHERE fb.booking_status_type = 'live'
	GROUP BY dst.salesforce_opportunity_id,
			 dst.posa_territory
)
;


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.deal_count__step03__model_margin_global
AS (
	SELECT
		dst.salesforce_opportunity_id,
		SUM(fb.margin_gross_of_toms_gbp_constant_currency) AS margin_gbp_lifetime,
		SUM(
				IFF(
						DATE_TRUNC('YEAR', booking_completed_date) = '2019-01-01',
						fb.margin_gross_of_toms_gbp_constant_currency,
						0
				)
		)                                                  AS margin_gbp_2019,
		SUM(
				IFF(
						DATE_TRUNC('YEAR', booking_completed_date) = '2020-01-01',
						fb.margin_gross_of_toms_gbp_constant_currency,
						0
				)
		)                                                  AS margin_gbp_2020,
		SUM(
				IFF(
						DATE_TRUNC('YEAR', booking_completed_date) = '2021-01-01',
						fb.margin_gross_of_toms_gbp_constant_currency,
						0
				)
		)                                                  AS margin_gbp_2021,
		SUM(
				IFF(
						DATE_TRUNC('YEAR', booking_completed_date) = '2022-01-01',
						fb.margin_gross_of_toms_gbp_constant_currency,
						0
				)
		)                                                  AS margin_gbp_2022,
		SUM(
				IFF(
						DATE_TRUNC('YEAR', booking_completed_date) = '2023-01-01',
						fb.margin_gross_of_toms_gbp_constant_currency,
						0
				)
		)                                                  AS margin_gbp_2023,
		SUM(
				IFF(
						DATE_TRUNC('YEAR', booking_completed_date) = '2024-01-01',
						fb.margin_gross_of_toms_gbp_constant_currency,
						0
				)
		)                                                  AS margin_gbp_2024
	FROM data_vault_mvp_dev_robin.dwh.fact_booking fb
		LEFT JOIN data_vault_mvp_dev_robin.bi.dim_sale_territory dst
				  ON fb.se_sale_id = dst.se_sale_id
					  AND fb.territory = dst.posa_territory
	WHERE fb.booking_status_type = 'live'
	GROUP BY dst.salesforce_opportunity_id
)
;


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.deal_count__step04__model_sale_active
AS (
	SELECT
		ds.salesforce_opportunity_id,
		ds.company_id,
		COALESCE(ds.company_name, 'Not in Salesforce')            AS company_name,
		--CHANGED coalesce include travelbird
		COALESCE(ds.current_contractor_name, 'Not in Salesforce') AS current_contractor_name,
		--CHANGED coalesce include travelbird
		ds.posu_cluster,
		--uses se.bi.dim_sale as source table rather than sale attributes
		ds.posu_cluster_region,
		--uses se.bi.dim_sale as source table rather than sale attributes
		ds.posu_cluster_sub_region,
		--use se.bi.dim_sale as source table rather than sale attributes
		ds.posu_country,
		--uses se.bi.dim_sale as source table rather than sale attributes
		ds.posa_territory,
		sca.segment_2019                                          AS forecast_segment,
		--To be left NULL until logic for TravelBird deals is determined
		gsa.deal_segment                                          AS current_segment,
		--To be left NULL until logic for TravelBird deals is determined
		sa.view_date,
		ds.product_type,
		--use se.bi.dim_sale as source table rather than sale attributes
		ds.product_configuration,
		--use se.bi.dim_sale as source table rather than sale attributes
		COALESCE(ss.pulled_type, 'No Reason Given')               AS pulled_type,
		--COALESCE confirmed by Christie J in CIP
		COALESCE(ss.pulled_reason, 'No Reason Given')             AS pulled_reason,
		--COALESCE confirmed by Christie J in CIP
		COALESCE(margin.margin_gbp_lifetime, 0)                   AS margin_gbp_lifetime,
		COALESCE(margin.margin_gbp_2019, 0)                       AS margin_gbp_2019,
		COALESCE(margin.margin_gbp_2020, 0)                       AS margin_gbp_2020,
		COALESCE(margin.margin_gbp_2021, 0)                       AS margin_gbp_2021,
		COALESCE(margin.margin_gbp_2022, 0)                       AS margin_gbp_2022,
		COALESCE(margin.margin_gbp_2023, 0)                       AS margin_gbp_2023,
		COALESCE(margin.margin_gbp_2024, 0)                       AS margin_gbp_2024,
		MAX(
				CASE
					WHEN LOWER(COALESCE(ss.target_account_list, '')) LIKE '%hunting%' THEN 1
					ELSE 0
				END
		)                                                         AS hunting_list
	FROM data_vault_mvp_dev_robin.bi.deal_count__step01__model_sale_table ds
		LEFT JOIN data_vault_mvp_dev_robin.dwh.sale_active_snapshot sa
				  ON ds.se_sale_id = sa.se_sale_id
		LEFT JOIN data_vault_mvp_dev_robin.dwh.se_sale ss
				  ON ds.se_sale_id = ss.se_sale_id
					  AND
					 ss.class IS DISTINCT FROM 'com.flashsales.sale.ConnectedWebRedirectSale' -- remove WRD catalogue sales
		LEFT JOIN data_vault_mvp_dev_robin.dwh.se_company_attributes sca
				  ON ds.company_id = sca.company_id::VARCHAR
		LEFT JOIN data_vault_mvp_dev_robin.dwh.global_sale_attributes gsa
				  ON ds.salesforce_opportunity_id = gsa.global_sale_id
		LEFT JOIN data_vault_mvp_dev_robin.bi.deal_count__step02__model_margin margin
				  ON margin.salesforce_opportunity_id = ds.salesforce_opportunity_id
					  AND margin.posa_territory = ds.posa_territory
	WHERE sa.active = TRUE
	GROUP BY ALL
)
;


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.deal_count__step05__model_sale_active_global
AS (
	SELECT
		ds.salesforce_opportunity_id,
		ds.company_id,
		COALESCE(ds.company_name, 'Not in Salesforce')            AS company_name,
		--CHANGED coalesce include travelbird
		COALESCE(ds.current_contractor_name, 'Not in Salesforce') AS current_contractor_name,
		--CHANGED coalesce include travelbird
		ds.posu_cluster,
		--uses se.bi.dim_sale as source table rather than sale attributes
		ds.posu_cluster_region,
		--uses se.bi.dim_sale as source table rather than sale attributes
		ds.posu_cluster_sub_region,
		--use se.bi.dim_sale as source table rather than sale attributes
		ds.posu_country,
		--uses se.bi.dim_sale as source table rather than sale attributes
		'Total'                                                   AS posa_territory,
		sca.segment_lly                                           AS forecast_segment,
		--To be left NULL until logic for TravelBird deals is determined
		gsa.deal_segment                                          AS current_segment,
		--To be left NULL until logic for TravelBird deals is determined
		sa.view_date,
		ds.product_type,
		--use se.bi.dim_sale as source table rather than sale attributes
		ds.product_configuration,
		--use se.bi.dim_sale as source table rather than sale attributes
		COALESCE(ssa.pulled_type, 'No Reason Given')              AS pulled_type,
		--COALESCE confirmed by Christie J in CIP
		COALESCE(ssa.pulled_reason, 'No Reason Given')            AS pulled_reason,
		--COALESCE confirmed by Christie J in CIP
		COALESCE(margin_global.margin_gbp_lifetime, 0)            AS margin_gbp_lifetime,
		COALESCE(margin_global.margin_gbp_2019, 0)                AS margin_gbp_2019,
		COALESCE(margin_global.margin_gbp_2020, 0)                AS margin_gbp_2020,
		COALESCE(margin_global.margin_gbp_2021, 0)                AS margin_gbp_2021,
		COALESCE(margin_global.margin_gbp_2022, 0)                AS margin_gbp_2022,
		COALESCE(margin_global.margin_gbp_2023, 0)                AS margin_gbp_2023,
		COALESCE(margin_global.margin_gbp_2024, 0)                AS margin_gbp_2024,
		MAX(
				CASE
					WHEN LOWER(COALESCE(ssa.target_account_list, '')) LIKE '%hunting%' THEN 1
					ELSE 0
				END
		)                                                         AS hunting_list
	FROM data_vault_mvp_dev_robin.bi.deal_count__step01__model_sale_table ds
		LEFT JOIN data_vault_mvp_dev_robin.dwh.sale_active_snapshot sa
				  ON ds.se_sale_id = sa.se_sale_id
		LEFT JOIN data_vault_mvp_dev_robin.dwh.se_sale ssa
				  ON ds.se_sale_id = ssa.se_sale_id
					  AND
					 ssa.class IS DISTINCT FROM 'com.flashsales.sale.ConnectedWebRedirectSale' -- remove WRD catalogue sales
		LEFT JOIN data_vault_mvp_dev_robin.dwh.se_company_attributes sca
				  ON ds.company_id = sca.company_id::VARCHAR
					  --SELECT * FROM se.data.dim_sale; SELECT * FROM se.data.se_company_attributes
		LEFT JOIN data_vault_mvp_dev_robin.dwh.global_sale_attributes gsa
				  ON ds.salesforce_opportunity_id = gsa.global_sale_id
		LEFT JOIN data_vault_mvp_dev_robin.bi.deal_count__step03__model_margin_global margin_global
				  ON margin_global.salesforce_opportunity_id = ds.salesforce_opportunity_id
	WHERE sa.active = TRUE
	GROUP BY ALL
)
;



CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.deal_count__step06__model_calendar
AS (
	SELECT
		date_value AS date,
		week_start
	FROM data_vault_mvp_dev_robin.dwh.se_calendar cal
	WHERE date_value BETWEEN '2020-06-01' AND CURRENT_DATE
)
;

SELECT * FROm se.data.se_sale_attributes ssa WHERE ssa.sale_active

-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.deal_count__step07__model_combine_sale_cal
-- AS (
	SELECT
		cal.week_start,
		cal.date,
		sale_active.salesforce_opportunity_id,
		sale_active.company_id,
		sale_active.company_name,
		sale_active.current_contractor_name,
		sale_active.posu_cluster,
		sale_active.posu_cluster_region,
		sale_active.posu_cluster_sub_region,
		sale_active.posu_country,
		sale_active.posa_territory,
		sale_active.forecast_segment,
		sale_active.current_segment,
		sale_active.product_type,
		sale_active.product_configuration,
		sale_active.pulled_type,
		sale_active.pulled_reason,
		SUM(sale_active.margin_gbp_lifetime) AS margin_gbp_lifetime,
		SUM(sale_active.margin_gbp_2019) AS margin_gbp_2019,
		SUM(sale_active.margin_gbp_2020) AS margin_gbp_2020,
		SUM(sale_active.margin_gbp_2021) AS margin_gbp_2021,
		SUM(sale_active.margin_gbp_2022) AS margin_gbp_2022,
		SUM(sale_active.margin_gbp_2023) AS margin_gbp_2023,
		SUM(sale_active.margin_gbp_2024) AS margin_gbp_2024,
		SUM(sale_active.hunting_list) AS hunting_list,
		MAX(
				CASE
					WHEN cal.date = sale_active.view_date THEN 1
					ELSE 0
				END
		) AS active
	FROM data_vault_mvp_dev_robin.bi.deal_count__step04__model_sale_active sale_active,
		 data_vault_mvp_dev_robin.bi.deal_count__step06__model_calendar cal
	WHERE sale_active.salesforce_opportunity_id = '0061r00001FGQA7'
	GROUP BY ALL
-- )
-- ;

SELECT
	cal.week_start,
	cal.date,
	sale_active_global.salesforce_opportunity_id,
	sale_active_global.company_id,
	sale_active_global.company_name,
	sale_active_global.current_contractor_name,
	sale_active_global.posu_cluster,
	sale_active_global.posu_cluster_region,
	sale_active_global.posu_cluster_sub_region,
	sale_active_global.posu_country,
	sale_active_global.posa_territory,
	sale_active_global.forecast_segment,
	sale_active_global.current_segment,
	sale_active_global.product_type,
	sale_active_global.product_configuration,
	sale_active_global.pulled_type,
	sale_active_global.pulled_reason,
	sale_active_global.margin_gbp_lifetime,
	sale_active_global.margin_gbp_2019,
	sale_active_global.margin_gbp_2020,
	sale_active_global.margin_gbp_2021,
	sale_active_global.margin_gbp_2022,
	sale_active_global.margin_gbp_2023,
	sale_active_global.margin_gbp_2024,
	sale_active_global.hunting_list,
	MAX(
		CASE
			WHEN cal.date = sale_active_global.view_date THEN 1
			ELSE 0
		END
	) AS active
	FROM data_vault_mvp_dev_robin.bi.deal_count__step05__model_sale_active_global sale_active_global, data_vault_mvp_dev_robin.bi.deal_count__step06__model_calendar cal
	GROUP BY ALL