CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.global_sale_attributes CLONE data_vault_mvp.dwh.global_sale_attributes
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.offer_inclusion CLONE data_vault_mvp.dwh.offer_inclusion
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_booking CLONE data_vault_mvp.dwh.se_booking
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.mari_room_rates CLONE data_vault_mvp.dwh.mari_room_rates
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.cms_allocation_link CLONE data_vault_mvp.dwh.cms_allocation_link
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_offer CLONE data_vault_mvp.dwh.se_offer
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_sale CLONE data_vault_mvp.dwh.se_sale
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.fact_booking CLONE data_vault_mvp.dwh.fact_booking
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.dim_sale CLONE data_vault_mvp.dwh.dim_sale
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.harmonised_sale_calendar_view CLONE data_vault_mvp.dwh.harmonised_sale_calendar_view
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.fpa_gsheets.posu_categorisation CLONE latest_vault.fpa_gsheets.posu_categorisation
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_sale_tags CLONE data_vault_mvp.dwh.se_sale_tags
;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.harmonised_offer_calendar_view AS
SELECT *
FROM data_vault_mvp.dwh.harmonised_offer_calendar_view
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.harmonised_sale_calendar_view_snapshot CLONE data_vault_mvp.dwh.harmonised_sale_calendar_view_snapshot
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_events_of_interest CLONE data_vault_mvp.single_customer_view_stg.module_events_of_interest
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_calendar CLONE data_vault_mvp.dwh.se_calendar
;

SELECT GET_DDL('table', 'data_vault_mvp_dev_robin.dwh.harmonised_offer_calendar_view')
;


WITH
	aggregate_inclusions AS (
		-- per offer id
		SELECT
			soi.cms_manual_offer_id,
			soi.board_basis
		FROM data_vault_mvp_dev_robin.dwh.offer_inclusion soi
		WHERE soi.offer_data_model = 'New Data Model'
		GROUP BY 1, 2
	),

	pulling_board_data AS (
		SELECT DISTINCT
			cal.hotel_code,
			cal.se_offer_id,
			soa.offer_active,
			ssa.salesforce_opportunity_id,
			pc.posu_cluster,
			pc.posu_cluster_region,
			pc.posu_cluster_sub_region,
			ai.board_basis
		FROM data_vault_mvp_dev_robin.dwh.cms_allocation_link cal
			LEFT JOIN data_vault_mvp_dev_robin.dwh.se_offer soa
					  ON cal.se_offer_id = soa.se_offer_id ---(AND soa.offer_active = 'TRUE')
			LEFT JOIN data_vault_mvp_dev_robin.dwh.se_sale ssa
					  ON cal.hotel_code = ssa.hotel_code
			LEFT JOIN latest_vault_dev_robin.fpa_gsheets.posu_categorisation pc
					  ON ssa.posu_categorisation_id = pc.posu_categorisation_id
			LEFT JOIN aggregate_inclusions ai
					  ON cal.se_offer_id = 'A' || ai.cms_manual_offer_id::VARCHAR
		WHERE ssa.sale_active = TRUE
	),

	mapped_board_clusters AS (
		SELECT DISTINCT
			ds.se_sale_id,
			ds.posu_cluster,
			ds.posu_cluster_region,
			ds.posu_cluster_sub_region,
			CASE
				WHEN pbd.board_basis IN (
										 'All-Inclusive',
										 'Breakfast & Lunch',
										 'Breakfast & Dinner',
										 'Breakfast',
										 'Breakfast, Lunch & Dinner')
					THEN pbd.board_basis
				ELSE 'Room-Only'
			END AS mapped_board_cluster
		FROM pulling_board_data pbd
			INNER JOIN data_vault_mvp_dev_robin.dwh.dim_sale ds
					   ON pbd.salesforce_opportunity_id = ds.salesforce_opportunity_id
		WHERE ds.sale_active = TRUE
		  AND ds.posa_territory IS DISTINCT FROM 'PL'
	)
-- removing duplications in mapping based on different board basis offered
SELECT
	mbc.se_sale_id,
	mbc.posu_cluster,
	mbc.posu_cluster_region,
	mbc.posu_cluster_sub_region,
	LISTAGG(DISTINCT mbc.mapped_board_cluster, ', ')
			WITHIN GROUP (ORDER BY mbc.mapped_board_cluster) AS mapped_board_cluster
FROM mapped_board_clusters mbc
GROUP BY 1, 2, 3, 4


SELECT *
FROM data_vault_mvp_dev_robin.dwh.sale_clustering__step10__create_lead_rate_cluster
;


SELECT
	hscv.se_sale_id,
	COUNT(DISTINCT
		  IFF(hscv.calendar_date >= CURRENT_DATE(), hscv.calendar_date, NULL)) AS total_dates_available_from_current_date,
	COUNT(DISTINCT IFF(hscv.calendar_date >= CURRENT_DATE() AND hscv.calendar_date <= DATEADD(DAY, 60, CURRENT_DATE()),
					   hscv.calendar_date, NULL))                              AS total_availability_next_60_days,
	COUNT(DISTINCT
		  IFF(hscv.calendar_date >= DATEADD(DAY, -60, CURRENT_DATE()) AND hscv.calendar_date < CURRENT_DATE(),
			  hscv.calendar_date, NULL))                                       AS total_availability_past_60_days
FROM data_vault_mvp_dev_robin.dwh.harmonised_sale_calendar_view hscv
	LEFT JOIN data_vault_mvp_dev_robin.dwh.dim_sale ds ON hscv.se_sale_id = ds.se_sale_id
WHERE hscv.sale_available_in_calendar = TRUE
  AND ds.posa_territory IS DISTINCT FROM 'PL'
GROUP BY 1
;


WITH
	events_of_interest_aggregated AS (
		SELECT
			tba.touch_id,
			tba.touch_start_tstamp::DATE                                             AS session_date,
			cal.last_month_mtd_ly,
			tba.attributed_user_id,
			eoi.se_sale_id,
			--events
			COUNT((IFF(eoi.event_subcategory = 'SPV', eoi.event_hash, NULL)))        AS events_with_spvs,
			COUNT(
					(IFF(eoi.event_category = 'transaction', eoi.event_hash, NULL))) AS sessions_with_bookings,
			SUM(fb.margin_gross_of_toms_gbp_constant_currency)                       AS margin_gbp
		FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes tba
			LEFT JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_events_of_interest eoi
					  ON eoi.touch_id = tba.touch_id
						  AND (eoi.event_subcategory = 'SPV' OR eoi.event_category = 'transaction')
			LEFT JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel tmc
					  ON tmc.touch_id = tba.touch_id
			LEFT JOIN data_vault_mvp_dev_robin.dwh.fact_booking fb ON fb.booking_id = eoi.booking_id
			LEFT JOIN data_vault_mvp_dev_robin.dwh.dim_sale ds ON ds.se_sale_id = eoi.se_sale_id
						  -- We must rejoin into dim sale using fact booking because we're missing a number of se_sale_id against trx events
			LEFT JOIN data_vault_mvp_dev_robin.dwh.dim_sale ds_fb ON ds_fb.se_sale_id = fb.se_sale_id
			AND fb.booking_status_type IN ('live', 'cancelled')
			LEFT JOIN data_vault_mvp_dev_robin.dwh.se_calendar cal ON cal.date_value = tba.touch_start_tstamp::DATE
		WHERE tba.touch_start_tstamp >= '2023-01-01'
		  AND tba.touch_se_brand = 'SE Brand'
		GROUP BY 1, 2, 3, 4, 5
	),

	gpv AS (
		SELECT
			a.se_sale_id,
			--margin
			SUM(IFF(a.session_date >= DATE_TRUNC('year', DATEADD('year', -1, CURRENT_DATE)), COALESCE(a.margin_gbp, 0),
					0))                                                             AS margin_gbp_last_year,
			SUM(IFF(a.session_date >= DATE_TRUNC('year', CURRENT_DATE), COALESCE(a.margin_gbp, 0),
					0))                                                             AS margin_gbp_this_year,
			SUM(IFF(a.session_date = DATEADD('day', -1, CURRENT_DATE), COALESCE(a.margin_gbp, 0),
					0))                                                             AS margin_gbp_yesterday,
			SUM(IFF(a.session_date >= DATEADD('day', -3, CURRENT_DATE), COALESCE(a.margin_gbp, 0),
					0))                                                             AS margin_gbp_last_3_days,
			SUM(IFF(a.session_date >= DATEADD('day', -7, CURRENT_DATE), COALESCE(a.margin_gbp, 0),
					0))                                                             AS margin_gbp_last_7_days,
			SUM(IFF(a.session_date >= DATEADD('day', -14, CURRENT_DATE), COALESCE(a.margin_gbp, 0),
					0))                                                             AS margin_gbp_last_14_days,
			SUM(IFF(a.session_date >= DATEADD('day', -30, CURRENT_DATE), COALESCE(a.margin_gbp, 0),
					0))                                                             AS margin_gbp_last_30_days,
			SUM(IFF(a.session_date >= DATEADD('day', -60, CURRENT_DATE), COALESCE(a.margin_gbp, 0),
					0))                                                             AS margin_gbp_last_60_days,
			SUM(IFF(a.session_date >= DATEADD('day', -90, CURRENT_DATE), COALESCE(a.margin_gbp, 0),
					0))                                                             AS margin_gbp_last_90_days,
			SUM(IFF(a.session_date >= DATEADD('day', -180, CURRENT_DATE), COALESCE(a.margin_gbp, 0),
					0))                                                             AS margin_gbp_last_180_days,
			SUM(IFF(a.session_date >= '2023-01-01', COALESCE(a.margin_gbp, 0),
					0))                                                             AS margin_gbp_2023_onwards,
			SUM(IFF(a.last_month_mtd_ly, COALESCE(a.margin_gbp, 0),
					0))                                                             AS margin_gbp_same_month_last_year,
			--SPVs
			SUM(IFF(a.session_date >= DATE_TRUNC('year', DATEADD('year', -1, CURRENT_DATE)),
					COALESCE(a.events_with_spvs, 0),
					0))                                                             AS spvs_last_year,
			SUM(IFF(a.session_date >= DATE_TRUNC('year', CURRENT_DATE), COALESCE(a.events_with_spvs, 0),
					0))                                                             AS spvs_this_year,
			SUM(IFF(a.session_date = DATEADD('day', -1, CURRENT_DATE), COALESCE(a.events_with_spvs, 0),
					0))                                                             AS spvs_yesterday,
			SUM(IFF(a.session_date >= DATEADD('day', -3, CURRENT_DATE), COALESCE(a.events_with_spvs, 0),
					0))                                                             AS spvs_last_3_days,
			SUM(IFF(a.session_date >= DATEADD('day', -7, CURRENT_DATE), COALESCE(a.events_with_spvs, 0),
					0))                                                             AS spvs_last_7_days,
			SUM(IFF(a.session_date >= DATEADD('day', -14, CURRENT_DATE), COALESCE(a.events_with_spvs, 0),
					0))                                                             AS spvs_last_14_days,
			SUM(IFF(a.session_date >= DATEADD('day', -30, CURRENT_DATE), COALESCE(a.events_with_spvs, 0),
					0))                                                             AS spvs_last_30_days,
			SUM(IFF(a.session_date >= DATEADD('day', -60, CURRENT_DATE), COALESCE(a.events_with_spvs, 0),
					0))                                                             AS spvs_last_60_days,
			SUM(IFF(a.session_date >= DATEADD('day', -90, CURRENT_DATE), COALESCE(a.events_with_spvs, 0),
					0))                                                             AS spvs_last_90_days,
			SUM(IFF(a.session_date >= DATEADD('day', -180, CURRENT_DATE), COALESCE(a.events_with_spvs, 0),
					0))                                                             AS spvs_last_180_days,
			SUM(IFF(a.session_date >= '2023-01-01', COALESCE(a.events_with_spvs, 0),
					0))                                                             AS spvs_2023_onwards,
			SUM(IFF(a.last_month_mtd_ly, COALESCE(a.events_with_spvs, 0),
					0))                                                             AS spvs_same_month_last_year,
			--GPV
			IFF(spvs_last_year = 0, 0, margin_gbp_last_year / spvs_last_year)       AS gpv_last_year,
			IFF(spvs_this_year = 0, 0, margin_gbp_this_year / spvs_this_year)       AS gpv_this_year,
			IFF(spvs_yesterday = 0, 0, margin_gbp_yesterday / spvs_yesterday)       AS gpv_yesterday,
			IFF(spvs_last_3_days = 0, 0, margin_gbp_last_3_days / spvs_last_3_days) AS gpv_last_3_days,
			IFF(spvs_last_7_days = 0, 0, margin_gbp_last_7_days / spvs_last_7_days) AS gpv_last_7_days,
			IFF(spvs_last_14_days = 0, 0, margin_gbp_last_14_days /
										  spvs_last_14_days)                        AS gpv_last_14_days,
			IFF(spvs_last_30_days = 0, 0, margin_gbp_last_30_days /
										  spvs_last_30_days)                        AS gpv_last_30_days,
			IFF(spvs_last_60_days = 0, 0, margin_gbp_last_60_days /
										  spvs_last_60_days)                        AS gpv_last_60_days,
			IFF(spvs_last_90_days = 0, 0, margin_gbp_last_90_days /
										  spvs_last_90_days)                        AS gpv_last_90_days,
			IFF(spvs_last_180_days = 0, 0, margin_gbp_last_180_days /
										   spvs_last_180_days)                      AS gpv_last_180_days,
			IFF(spvs_2023_onwards = 0, 0, margin_gbp_2023_onwards /
										  spvs_2023_onwards)                        AS gpv_2023_onwards,
			IFF(spvs_same_month_last_year = 0, 0, margin_gbp_same_month_last_year /
												  spvs_same_month_last_year)        AS gpv_same_month_last_year
		FROM events_of_interest_aggregated a
		GROUP BY 1
	)

SELECT
	gpv.se_sale_id,
	--margin
	margin_gbp_last_year,
	margin_gbp_this_year,
	margin_gbp_yesterday,
	margin_gbp_last_3_days,
	margin_gbp_last_7_days,
	margin_gbp_last_14_days,
	margin_gbp_last_30_days,
	margin_gbp_last_60_days,
	margin_gbp_last_90_days,
	margin_gbp_last_180_days,
	margin_gbp_2023_onwards,
	margin_gbp_same_month_last_year,
	--spvs
	spvs_last_year,
	spvs_this_year,
	spvs_yesterday,
	spvs_last_3_days,
	spvs_last_7_days,
	spvs_last_14_days,
	spvs_last_30_days,
	spvs_last_60_days,
	spvs_last_90_days,
	spvs_last_180_days,
	spvs_2023_onwards,
	spvs_same_month_last_year,
	--gpv
	gpv_last_year,
	gpv_this_year,
	gpv_yesterday,
	gpv_last_3_days,
	gpv_last_7_days,
	gpv_last_14_days,
	gpv_last_30_days,
	gpv_last_60_days,
	gpv_last_90_days,
	gpv_last_180_days,
	gpv_2023_onwards,
	gpv_same_month_last_year,
	--percentile
	PERCENT_RANK() OVER (ORDER BY gpv_last_year ASC)            AS gpv_last_year_percentile,
	PERCENT_RANK() OVER (ORDER BY gpv_this_year ASC)            AS gpv_this_year_percentile,
	PERCENT_RANK() OVER (ORDER BY gpv_yesterday ASC)            AS gpv_yesterday_percentile,
	PERCENT_RANK() OVER (ORDER BY gpv_last_3_days ASC)          AS gpv_last_3_days_percentile,
	PERCENT_RANK() OVER (ORDER BY gpv_last_7_days ASC)          AS gpv_last_7_days_percentile,
	PERCENT_RANK() OVER (ORDER BY gpv_last_14_days ASC)         AS gpv_last_14_days_percentile,
	PERCENT_RANK() OVER (ORDER BY gpv_last_30_days ASC)         AS gpv_last_30_days_percentile,
	PERCENT_RANK() OVER (ORDER BY gpv_last_60_days ASC)         AS gpv_last_60_days_percentile,
	PERCENT_RANK() OVER (ORDER BY gpv_last_90_days ASC)         AS gpv_last_90_days_percentile,
	PERCENT_RANK() OVER (ORDER BY gpv_last_180_days ASC)        AS gpv_last_180_days_percentile,
	PERCENT_RANK() OVER (ORDER BY gpv_2023_onwards ASC)         AS gpv_2023_onwards_percentile,
	PERCENT_RANK() OVER (ORDER BY gpv_same_month_last_year ASC) AS gpv_same_month_last_year_percentile,
	--buckets
	CASE
		WHEN gpv_last_year_percentile <= 0.2 THEN 'Lowest 20%'
		WHEN gpv_last_year_percentile <= 0.4 THEN 'Lowest 20% to 40%'
		WHEN gpv_last_year_percentile <= 0.6 THEN 'Middle 40% to 60%'
		WHEN gpv_last_year_percentile <= 0.8 THEN 'High 20% to 40%'
		WHEN gpv_last_year_percentile <= 1.0 THEN 'Highest 20%'
		ELSE 'Unknown'
	END                                                         AS gpv_last_year_bucket,
	CASE
		WHEN gpv_this_year_percentile <= 0.2 THEN 'Lowest 20%'
		WHEN gpv_this_year_percentile <= 0.4 THEN 'Lowest 20% to 40%'
		WHEN gpv_this_year_percentile <= 0.6 THEN 'Middle 40% to 60%'
		WHEN gpv_this_year_percentile <= 0.8 THEN 'High 20% to 40%'
		WHEN gpv_this_year_percentile <= 1.0 THEN 'Highest 20%'
		ELSE 'Unknown'
	END                                                         AS gpv_this_year_bucket,
	CASE
		WHEN gpv_yesterday_percentile <= 0.2 THEN 'Lowest 20%'
		WHEN gpv_yesterday_percentile <= 0.4 THEN 'Lowest 20% to 40%'
		WHEN gpv_yesterday_percentile <= 0.6 THEN 'Middle 40% to 60%'
		WHEN gpv_yesterday_percentile <= 0.8 THEN 'High 20% to 40%'
		WHEN gpv_yesterday_percentile <= 1.0 THEN 'Highest 20%'
		ELSE 'Unknown'
	END                                                         AS gpv_yesterday_bucket,
	CASE
		WHEN gpv_last_3_days_percentile <= 0.2 THEN 'Lowest 20%'
		WHEN gpv_last_3_days_percentile <= 0.4 THEN 'Lowest 20% to 40%'
		WHEN gpv_last_3_days_percentile <= 0.6 THEN 'Middle 40% to 60%'
		WHEN gpv_last_3_days_percentile <= 0.8 THEN 'High 20% to 40%'
		WHEN gpv_last_3_days_percentile <= 1.0 THEN 'Highest 20%'
		ELSE 'Unknown'
	END                                                         AS gpv_last_3_days_bucket,
	CASE
		WHEN gpv_last_7_days_percentile <= 0.2 THEN 'Lowest 20%'
		WHEN gpv_last_7_days_percentile <= 0.4 THEN 'Lowest 20% to 40%'
		WHEN gpv_last_7_days_percentile <= 0.6 THEN 'Middle 40% to 60%'
		WHEN gpv_last_7_days_percentile <= 0.8 THEN 'High 20% to 40%'
		WHEN gpv_last_7_days_percentile <= 1.0 THEN 'Highest 20%'
		ELSE 'Unknown'
	END                                                         AS gpv_last_7_days_bucket,
	CASE
		WHEN gpv_last_14_days_percentile <= 0.2 THEN 'Lowest 20%'
		WHEN gpv_last_14_days_percentile <= 0.4 THEN 'Lowest 20% to 40%'
		WHEN gpv_last_14_days_percentile <= 0.6 THEN 'Middle 40% to 60%'
		WHEN gpv_last_14_days_percentile <= 0.8 THEN 'High 20% to 40%'
		WHEN gpv_last_14_days_percentile <= 1.0 THEN 'Highest 20%'
		ELSE 'Unknown'
	END                                                         AS gpv_last_14_days_bucket,
	CASE
		WHEN gpv_last_30_days_percentile <= 0.2 THEN 'Lowest 20%'
		WHEN gpv_last_30_days_percentile <= 0.4 THEN 'Lowest 20% to 40%'
		WHEN gpv_last_30_days_percentile <= 0.6 THEN 'Middle 40% to 60%'
		WHEN gpv_last_30_days_percentile <= 0.8 THEN 'High 20% to 40%'
		WHEN gpv_last_30_days_percentile <= 1.0 THEN 'Highest 20%'
		ELSE 'Unknown'
	END                                                         AS gpv_last_30_days_bucket,
	CASE
		WHEN gpv_last_60_days_percentile <= 0.2 THEN 'Lowest 20%'
		WHEN gpv_last_60_days_percentile <= 0.4 THEN 'Lowest 20% to 40%'
		WHEN gpv_last_60_days_percentile <= 0.6 THEN 'Middle 40% to 60%'
		WHEN gpv_last_60_days_percentile <= 0.8 THEN 'High 20% to 40%'
		WHEN gpv_last_60_days_percentile <= 1.0 THEN 'Highest 20%'
		ELSE 'Unknown'
	END                                                         AS gpv_last_60_days_bucket,
	CASE
		WHEN gpv_last_90_days_percentile <= 0.2 THEN 'Lowest 20%'
		WHEN gpv_last_90_days_percentile <= 0.4 THEN 'Lowest 20% to 40%'
		WHEN gpv_last_90_days_percentile <= 0.6 THEN 'Middle 40% to 60%'
		WHEN gpv_last_90_days_percentile <= 0.8 THEN 'High 20% to 40%'
		WHEN gpv_last_90_days_percentile <= 1.0 THEN 'Highest 20%'
		ELSE 'Unknown'
	END                                                         AS gpv_last_90_days_bucket,
	CASE
		WHEN gpv_last_180_days_percentile <= 0.2 THEN 'Lowest 20%'
		WHEN gpv_last_180_days_percentile <= 0.4 THEN 'Lowest 20% to 40%'
		WHEN gpv_last_180_days_percentile <= 0.6 THEN 'Middle 40% to 60%'
		WHEN gpv_last_180_days_percentile <= 0.8 THEN 'High 20% to 40%'
		WHEN gpv_last_180_days_percentile <= 1.0 THEN 'Highest 20%'
		ELSE 'Unknown'
	END                                                         AS gpv_last_180_days_bucket,
	CASE
		WHEN gpv_2023_onwards_percentile <= 0.2 THEN 'Lowest 20%'
		WHEN gpv_2023_onwards_percentile <= 0.4 THEN 'Lowest 20% to 40%'
		WHEN gpv_2023_onwards_percentile <= 0.6 THEN 'Middle 40% to 60%'
		WHEN gpv_2023_onwards_percentile <= 0.8 THEN 'High 20% to 40%'
		WHEN gpv_2023_onwards_percentile <= 1.0 THEN 'Highest 20%'
		ELSE 'Unknown'
	END                                                         AS gpv_2023_onwards_bucket,
	CASE
		WHEN gpv_same_month_last_year_percentile <= 0.2 THEN 'Lowest 20%'
		WHEN gpv_same_month_last_year_percentile <= 0.4 THEN 'Lowest 20% to 40%'
		WHEN gpv_same_month_last_year_percentile <= 0.6 THEN 'Middle 40% to 60%'
		WHEN gpv_same_month_last_year_percentile <= 0.8 THEN 'High 20% to 40%'
		WHEN gpv_same_month_last_year_percentile <= 1.0 THEN 'Highest 20%'
		ELSE 'Unknown'
	END                                                         AS gpv_same_month_last_year_bucket
FROM gpv
;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.sale_clustering
;


SELECT
	sale_clustering.gpv_last_3_days_bucket,
	COUNT(*)
FROM data_vault_mvp_dev_robin.dwh.sale_clustering
GROUP BY 1
;

SELECT
	oc.gpv_yesterday,
	oc.gpv_yesterday_percentile,
	oc.gpv_yesterday_bucket,
	COUNT(*)
FROM data_vault_mvp.dwh.opportunity_clustering oc
GROUP BY 1, 2, 3
;

SELECT
	oc.total_availability_next_60_days_bucket,
	COUNT(*)
FROM data_vault_mvp.dwh.opportunity_clustering oc
GROUP BY 1
;


SELECT *
FROM se.data.scv_touched_module_events_of_interest stmeoi
WHERE stmeoi.event_category = 'transaction'
;

------------------------------------------------------------------------------------------------------------------------
USE WAREHOUSE pipe_xlarge
;

WITH
	events_of_interest_aggregated AS (
		SELECT
			tba.touch_id,
			tba.touch_start_tstamp::DATE                                         AS session_date,
			cal.last_month_mtd_ly,
			tba.attributed_user_id,
			COALESCE(eoi.se_sale_id, fb.se_sale_id, ds_fb.se_sale_id)            AS se_sale_id,
			--events
			COUNT(IFF(eoi.event_subcategory = 'SPV', eoi.event_hash, NULL))      AS events_with_spvs,
			COUNT(IFF(eoi.event_category = 'transaction', eoi.event_hash, NULL)) AS sessions_with_bookings,
			SUM(fb.margin_gross_of_toms_gbp_constant_currency)                   AS margin_gbp
		FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes tba
			INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_events_of_interest eoi
					   ON eoi.touch_id = tba.touch_id
						   AND (eoi.event_subcategory = 'SPV' OR eoi.event_category = 'transaction') AND
						  eoi.event_tstamp >= '2023-05-01'
			INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel tmc
					   ON tmc.touch_id = tba.touch_id
			LEFT JOIN  data_vault_mvp_dev_robin.dwh.fact_booking fb ON fb.booking_id = eoi.booking_id
			LEFT JOIN  data_vault_mvp_dev_robin.dwh.dim_sale ds ON ds.se_sale_id = eoi.se_sale_id
			LEFT JOIN  data_vault_mvp_dev_robin.dwh.dim_sale ds_fb ON ds_fb.se_sale_id = fb.se_sale_id
			AND fb.booking_status_type IN ('live', 'cancelled')
			LEFT JOIN  data_vault_mvp_dev_robin.dwh.se_calendar cal ON cal.date_value = tba.touch_start_tstamp::DATE
		WHERE tba.touch_start_tstamp >= '2023-05-01'
		  AND tba.touch_se_brand = 'SE Brand'
		GROUP BY 1, 2, 3, 4, 5
	),

	gpv AS (
		SELECT
			a.se_sale_id,
			--margin
			SUM(IFF(a.session_date = DATEADD('day', -1, CURRENT_DATE), COALESCE(a.margin_gbp, 0),
					0))                                                       AS margin_gbp_yesterday,
			--SPVs

			SUM(IFF(a.session_date = DATEADD('day', -1, CURRENT_DATE), COALESCE(a.events_with_spvs, 0),
					0))                                                       AS spvs_yesterday,
			--GPV
			IFF(spvs_yesterday = 0, 0, margin_gbp_yesterday / spvs_yesterday) AS gpv_yesterday
		FROM events_of_interest_aggregated a
		GROUP BY 1
	)

SELECT
	se_sale_id,
	--margin
	margin_gbp_yesterday,

	--spvs
	spvs_yesterday,

	--gpv
	gpv_yesterday,

	--percentile

	PERCENT_RANK() OVER (ORDER BY gpv_yesterday ASC) AS gpv_yesterday_percentile,

	--buckets
	CASE
		WHEN gpv_yesterday_percentile <= 0.2 THEN 'Lowest 20%'
		WHEN gpv_yesterday_percentile <= 0.4 THEN 'Lowest 20% to 40%'
		WHEN gpv_yesterday_percentile <= 0.6 THEN 'Middle 40% to 60%'
		WHEN gpv_yesterday_percentile <= 0.8 THEN 'High 20% to 40%'
		WHEN gpv_yesterday_percentile <= 1.0 THEN 'Highest 20%'
		ELSE 'Unknown'
	END                                              AS gpv_yesterday_bucket,

FROM gpv
;


SELECT
	se_sale_id,
	gpv.spvs_yesterday,
	gpv.margin_gbp_yesterday,
	gpv.gpv_yesterday,
	gpv.gpv_yesterday_percentile,
	SUM(gpv.gpv_yesterday) OVER (ORDER BY gpv.gpv_yesterday) / SUM(gpv.gpv_yesterday) OVER ()
FROM data_vault_mvp_dev_robin.dwh.sale_clustering__step14__margin_spv_gpv_metrics gpv
;


WITH
	aggregated_data AS (
		SELECT
			ua.date,
			ua.shiro_user_id,
			SUM(IFF(ua.app_sessions_30d > 0 OR ua.web_sessions_30d > 0, 1, 0)) AS mau,
			SUM(IFF(ua.app_sessions_30d > 0, 1, 0))                            AS app_mau,
			SUM(IFF(ua.web_sessions_30d > 0, 1, 0))                            AS web_mau,
			SUM(IFF(ua.emails_30d > 0, 1, 0))                                  AS email_mau
		FROM data_vault_mvp.dwh.user_activity ua
			INNER JOIN data_vault_mvp.dwh.user_attributes sua ON ua.shiro_user_id = sua.shiro_user_id
		WHERE ua.date >= CURRENT_DATE - 14
		  AND ua.shiro_user_id = '81852762'
		  AND (ua.app_sessions_30d
			+ ua.web_sessions_30d
			+ ua.emails_30d) > 0
		GROUP BY 1, 2
	)
SELECT
	date,
	shiro_user_id,
	mau,
	app_mau,
	web_mau,
	email_mau,
	SUM(app_mau) OVER (PARTITION BY shiro_user_id ORDER BY date ASC ROWS BETWEEN CURRENT ROW AND 6 FOLLOWING)
FROM aggregated_data
;



SELECT
	se_sale_id,
	gpv.spvs_yesterday,
	gpv.margin_gbp_yesterday,
	gpv.gpv_yesterday,
	gpv.gpv_yesterday_percentile,
	SUM(gpv.gpv_yesterday) OVER (ORDER BY gpv.gpv_yesterday) / SUM(gpv.gpv_yesterday) OVER ()
FROM data_vault_mvp_dev_robin.dwh.sale_clustering__step14__margin_spv_gpv_metrics gpv
;


SELECT * FROM data_vault_mvp_dev_robin.dwh.sale_clustering;


SELECT * FROm data_vault_mvp_dev_robin.dwh.data_science__sale_clustering;

SELECT * FROM se.data.user_activity ua;