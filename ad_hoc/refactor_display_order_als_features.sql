--latest
SELECT
	bs.row_loaded_at,
	bs.schedule_tstamp,
	bs.row_schedule_tstamp,
	bs.se_sale_id,
	bs.display_order,
	ROW_NUMBER() OVER (PARTITION BY bs.territory_id ORDER BY bs.display_order ASC, id ASC) AS display_order_corrected -- matches what the system does
FROM hygiene_vault.cms_mysql.base_sale bs
	INNER JOIN data_vault_mvp.dwh.se_sale ds ON ds.se_sale_id = bs.se_sale_id AND ds.sale_active
WHERE bs.se_sale_id = 'A13747'
-- QUALIFY COUNT(*) OVER (PARTITION BY bs.id) > 1

------------------------------------------------------------------------------------------------------------------------

WITH
	model_display_order AS (
		SELECT
			bs.row_loaded_at::DATE                   AS load_date,
			MIN(load_date) OVER (PARTITION BY bs.id) AS first_load_date, -- used to trim grain blowout to necessary dates
			bs.schedule_tstamp,
			bs.row_schedule_tstamp,
			bs.se_sale_id,
			bs.display_order,
			bs.territory_id
		FROM hygiene_vault.cms_mysql.base_sale bs
-- return the latest record for each id on a date date
		QUALIFY ROW_NUMBER() OVER (PARTITION BY bs.id, bs.row_loaded_at::DATE ORDER BY bs.row_loaded_at DESC) = 1
	),
	sale_by_date_grain AS (
		-- develop a daily grain of a sale from the first time it was observed
		SELECT DISTINCT
			sc.date_value,
			mdo.first_load_date,
			mdo.se_sale_id,
			mdo.territory_id
		FROM model_display_order mdo
			LEFT JOIN se.data.se_calendar sc ON mdo.first_load_date <= sc.date_value AND sc.date_value <= CURRENT_DATE
	),
	daily_sale_display_order AS (
		-- persist a display order throughout days where we've ingested no new data
		SELECT
			date_value,
			sbdg.first_load_date,
			sbdg.se_sale_id,
			mdo.display_order,
			mdo.territory_id,
			LAST_VALUE(mdo.display_order)
					   IGNORE NULLS OVER (PARTITION BY sbdg.se_sale_id ORDER BY sbdg.date_value ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS persisted_display_order
		FROM sale_by_date_grain sbdg
			LEFT JOIN model_display_order mdo ON sbdg.se_sale_id = mdo.se_sale_id AND sbdg.date_value = mdo.load_date
	),
	model_data AS (

		SELECT
			dsdo.date_value,
			dsdo.se_sale_id,
			dsdo.territory_id,
			dsdo.display_order,
			dsdo.persisted_display_order,
			ROW_NUMBER() OVER (PARTITION BY dsdo.territory_id, dsdo.date_value ORDER BY dsdo.persisted_display_order ASC, dsdo.se_sale_id ASC) AS corrected_diplay_order -- matched system logic to calculate a display order
		FROM daily_sale_display_order dsdo
			-- filter to sales that are live on each date
			INNER JOIN se.data.sale_active sa
					   ON dsdo.se_sale_id = sa.se_sale_id AND dsdo.date_value = sa.view_date
		QUALIFY corrected_diplay_order <= 50

	)
SELECT *
FROM model_data md
WHERE md.date_value = CURRENT_DATE
  AND md.territory_id = 1
;

-- looks like the job was turned on 2023-04-04;


SELECT *
FROM se.data.sale_active sa

SELECT *
FROM dbt_dev.dbt_robinpatel_data_science__intermediate.rnr_deal_feature_snapshot rdfs
;
