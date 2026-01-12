SELECT *
FROM data_vault_mvp.dwh.iterable_crm_reporting
WHERE message_id = 'cc66d26f116b43aea85ec4fde770c6e2'
;


campaign 11224669
shiro 78574059

SELECT *
FROM data_vault_mvp.dwh.iterable_crm_reporting__step11__model_scv_spv_data
WHERE shiro_user_id = 78574059
  AND campaign_id = '11224669'
;

SELECT *
FROM data_vault_mvp.dwh.iterable_crm_reporting__step10__model_scv_booking_data
WHERE shiro_user_id = 78574059
  AND campaign_id = '11224669'
;

-- booking has been attributed to this message id
'03cd8ea504a54dbc9ab7e237a92f88f1'


SELECT *
FROM data_vault_mvp.dwh.iterable_crm_reporting__step06__aggregate_sends
WHERE shiro_user_id = '78574059' AND campaign_id = '11224669'
;

------------------------------------------------------------------------------------------------------------------------

dwh.iterable_crm_reporting__step06__aggregate_sends

USE WAREHOUSE pipe_xlarge
;

------------------------------------------------------------------------------------------------------------------------
-- spv check

-- new icr table
WITH
	email_spvs AS (
		SELECT
			stmc.touch_id,
			COALESCE(
					stmc.utm_campaign,
					stba.app_push_open_context:dataFields:campaignId::VARCHAR
			) AS utm_campaign,
			stba.num_spvs
		FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes stba
			INNER JOIN se.data.scv_touch_marketing_channel stmc
					   ON stba.touch_id = stmc.touch_id
						   AND stmc.touch_mkt_channel LIKE ANY ('Email%', 'App Push')
		WHERE stba.touch_start_tstamp >= '2024-01-01' -- looking only at events from 2024
		  AND stba.touch_se_brand = 'SE Brand'
		  AND stba.num_spvs > 0
	),
	scv_campaign_spvs AS (
		SELECT
			es.utm_campaign::VARCHAR AS utm_campaign,
			SUM(es.num_spvs)         AS spvs
		FROM email_spvs es
		GROUP BY 1
	),
	icr_campaign_spvs AS (
		SELECT
			i.campaign_id::VARCHAR AS campaign_id,
			SUM(i.spvs_lc)         AS spvs
		FROM data_vault_mvp.dwh.iterable_crm_reporting i
		WHERE i.send_start_date >= '2024-01-01'
		GROUP BY 1
	),
	model_data AS (
		SELECT
			COALESCE(utm_campaign::VARCHAR, campaign_id::VARCHAR) AS campaign_id,
			scs.spvs                                              AS scv_spvs,
			ics.spvs                                              AS icr_spvs,
			icr_spvs - scv_spvs                                   AS diff,
			icr_spvs / scv_spvs - 1                               AS var
		FROM scv_campaign_spvs scs
			FULL OUTER JOIN icr_campaign_spvs ics ON scs.utm_campaign = ics.campaign_id
	)
SELECT
	SUM(md.scv_spvs)                    AS total_scv_spvs,
	SUM(md.icr_spvs)                    AS total_icr_spvs,
	total_icr_spvs - total_scv_spvs     AS diff,
	total_icr_spvs / total_scv_spvs - 1 AS var
FROM model_data md
;

-- old icr table
WITH
	email_spvs AS (
		SELECT
			stmc.touch_id,
			COALESCE(
					stmc.utm_campaign,
					stba.app_push_open_context:dataFields:campaignId::VARCHAR
			) AS utm_campaign,
			stba.num_spvs
		FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes stba
			INNER JOIN se.data.scv_touch_marketing_channel stmc
					   ON stba.touch_id = stmc.touch_id
						   AND stmc.touch_mkt_channel LIKE ANY ('Email%', 'App Push')
		WHERE stba.touch_start_tstamp >= '2024-01-01' -- looking only at events from 2024
		  AND stba.touch_se_brand = 'SE Brand'
		  AND stba.num_spvs > 0
	),
	scv_campaign_spvs AS (
		SELECT
			es.utm_campaign::VARCHAR AS utm_campaign,
			SUM(es.num_spvs)         AS spvs
		FROM email_spvs es
		GROUP BY 1
	),
	icr_campaign_spvs AS (
		SELECT
			i.campaign_id::VARCHAR AS campaign_id,
			SUM(i.spvs_lc)         AS spvs
		FROM data_vault_mvp.dwh.iterable_crm_reporting_old_version_20241210 i
		WHERE i.send_start_date >= '2024-01-01'
		GROUP BY 1
	),
	model_data AS (
		SELECT
			COALESCE(utm_campaign::VARCHAR, campaign_id::VARCHAR) AS campaign_id,
			scs.spvs                                              AS scv_spvs,
			ics.spvs                                              AS icr_spvs,
			icr_spvs - scv_spvs                                   AS diff,
			icr_spvs / scv_spvs - 1                               AS var
		FROM scv_campaign_spvs scs
			FULL OUTER JOIN icr_campaign_spvs ics ON scs.utm_campaign = ics.campaign_id
	)
SELECT
	SUM(md.scv_spvs)                    AS total_scv_spvs,
	SUM(md.icr_spvs)                    AS total_icr_spvs,
	total_icr_spvs - total_scv_spvs     AS diff,
	total_icr_spvs / total_scv_spvs - 1 AS var
FROM model_data md
;


-- spv diff
/*
SCV_SPVS	ICR_SPVS
87591811	84799318
 */


------------------------------------------------------------------------------------------------------------------------
-- margin check lc

-- new icr table
WITH
	email_bookings AS (
		SELECT
			stmc.touch_id,
			COALESCE(
					stmc.utm_campaign,
					stba.app_push_open_context:dataFields:campaignId::VARCHAR
			)                                                  AS utm_campaign,
			COUNT(DISTINCT stt.booking_id)                     AS bookings,
			SUM(fb.margin_gross_of_toms_gbp_constant_currency) AS margin_gbp
		FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes stba
			INNER JOIN se.data.scv_touch_marketing_channel stmc
					   ON stba.touch_id = stmc.touch_id
						   AND stmc.touch_mkt_channel LIKE ANY ('Email%', 'App Push')
			INNER JOIN se.data.scv_touched_transactions stt
					   ON stba.touch_id = stt.touch_id
			INNER JOIN se.data.fact_booking fb
					   ON stt.booking_id = fb.booking_id
						   AND fb.booking_status_type IN ('live', 'cancelled')
		WHERE stba.touch_start_tstamp >= '2024-01-01' -- looking only at events from 2024
		  AND stba.touch_se_brand = 'SE Brand'
		  AND stba.touch_has_booking
		GROUP BY 1, 2
	),
	scv_campaign_bookings AS (
		SELECT
			eb.utm_campaign::VARCHAR AS utm_campaign,
			SUM(eb.bookings)         AS bookings,
			SUM(eb.margin_gbp)       AS margin_gbp
		FROM email_bookings eb
		GROUP BY 1
	),
	icr_campaign_bookings AS (
		SELECT
			i.campaign_id::VARCHAR AS campaign_id,
			SUM(i.bookings_lc)     AS bookings,
			SUM(i.margin_gbp_lc)   AS margin_gbp
		FROM data_vault_mvp.dwh.iterable_crm_reporting i
		WHERE i.send_start_date >= '2024-01-01'
		GROUP BY 1
	),
	model_data AS (
		SELECT
			COALESCE(utm_campaign::VARCHAR, campaign_id::VARCHAR) AS campaign_id,
			scs.bookings                                          AS scv_bookings,
			ics.bookings                                          AS icr_bookings,
			icr_bookings - scv_bookings                           AS diff,
			icr_bookings / scv_bookings - 1                       AS var,
			scs.margin_gbp                                        AS scv_margin_gbp,
			ics.margin_gbp                                        AS icr_margin_gbp,
			icr_margin_gbp - scv_margin_gbp                       AS diff,
			icr_margin_gbp / scv_margin_gbp - 1                   AS var
		FROM scv_campaign_bookings scs
			FULL OUTER JOIN icr_campaign_bookings ics ON scs.utm_campaign = ics.campaign_id
	)
SELECT
	SUM(md.scv_bookings)                            AS total_scv_bookings,
	SUM(md.icr_bookings)                            AS total_icr_bookings,
	total_icr_bookings - total_scv_bookings         AS bookings_diff,
	total_icr_bookings / total_scv_bookings - 1     AS bookings_var,
	SUM(md.scv_margin_gbp)                          AS total_scv_margin_gbp,
	SUM(md.icr_margin_gbp)                          AS total_icr_margin_gbp,
	total_icr_margin_gbp - total_scv_margin_gbp     AS margin_gbp_diff,
	total_icr_margin_gbp / total_scv_margin_gbp - 1 AS margin_gbp_var
FROM model_data md
;

-- old icr table
WITH
	email_bookings AS (
		SELECT
			stmc.touch_id,
			COALESCE(
					stmc.utm_campaign,
					stba.app_push_open_context:dataFields:campaignId::VARCHAR
			)                                                  AS utm_campaign,
			COUNT(DISTINCT stt.booking_id)                     AS bookings,
			SUM(fb.margin_gross_of_toms_gbp_constant_currency) AS margin_gbp
		FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes stba
			INNER JOIN se.data.scv_touch_marketing_channel stmc
					   ON stba.touch_id = stmc.touch_id
						   AND stmc.touch_mkt_channel LIKE ANY ('Email%', 'App Push')
			INNER JOIN se.data.scv_touched_transactions stt
					   ON stba.touch_id = stt.touch_id
			INNER JOIN se.data.fact_booking fb
					   ON stt.booking_id = fb.booking_id
						   AND fb.booking_status_type IN ('live', 'cancelled')
		WHERE stba.touch_start_tstamp >= '2024-01-01' -- looking only at events from 2024
		  AND stba.touch_se_brand = 'SE Brand'
		  AND stba.touch_has_booking
		GROUP BY 1, 2
	),
	scv_campaign_bookings AS (
		SELECT
			eb.utm_campaign::VARCHAR AS utm_campaign,
			SUM(eb.bookings)         AS bookings,
			SUM(eb.margin_gbp)       AS margin_gbp
		FROM email_bookings eb
		GROUP BY 1
	),
	icr_campaign_bookings AS (
		SELECT
			i.campaign_id::VARCHAR AS campaign_id,
			SUM(i.bookings_lc)     AS bookings,
			SUM(i.margin_gbp_lc)   AS margin_gbp
		FROM data_vault_mvp.dwh.iterable_crm_reporting_old_version_20241210 i
		WHERE i.send_start_date >= '2024-01-01'
		GROUP BY 1
	),
	model_data AS (
		SELECT
			COALESCE(utm_campaign::VARCHAR, campaign_id::VARCHAR) AS campaign_id,
			scs.bookings                                          AS scv_bookings,
			ics.bookings                                          AS icr_bookings,
			icr_bookings - scv_bookings                           AS diff,
			icr_bookings / scv_bookings - 1                       AS var,
			scs.margin_gbp                                        AS scv_margin_gbp,
			ics.margin_gbp                                        AS icr_margin_gbp,
			icr_margin_gbp - scv_margin_gbp                       AS diff,
			icr_margin_gbp / scv_margin_gbp - 1                   AS var
		FROM scv_campaign_bookings scs
			FULL OUTER JOIN icr_campaign_bookings ics ON scs.utm_campaign = ics.campaign_id
	)
SELECT
	SUM(md.scv_bookings)                            AS total_scv_bookings,
	SUM(md.icr_bookings)                            AS total_icr_bookings,
	total_icr_bookings - total_scv_bookings         AS bookings_diff,
	total_icr_bookings / total_scv_bookings - 1     AS bookings_var,
	SUM(md.scv_margin_gbp)                          AS total_scv_margin_gbp,
	SUM(md.icr_margin_gbp)                          AS total_icr_margin_gbp,
	total_icr_margin_gbp - total_scv_margin_gbp     AS margin_gbp_diff,
	total_icr_margin_gbp / total_scv_margin_gbp - 1 AS margin_gbp_var
FROM model_data md
;

------------------------------------------------------------------------------------------------------------------------
-- margin check lnd
USE WAREHOUSE pipe_xlarge;
-- new icr table
WITH
	email_bookings AS (
		SELECT
			stmc.touch_id,
			COALESCE(
					stmc.utm_campaign,
					stba2.app_push_open_context:dataFields:campaignId::VARCHAR
			)                                                  AS utm_campaign,
			COUNT(DISTINCT stt.booking_id)                     AS bookings,
			SUM(fb.margin_gross_of_toms_gbp_constant_currency) AS margin_gbp
		FROM se.data.scv_touch_basic_attributes stba
			INNER JOIN se.data.scv_touch_attribution sta
					   ON stba.touch_id = sta.touch_id AND sta.attribution_model = 'last non direct'
			INNER JOIN se.data.scv_touch_marketing_channel stmc
					   ON sta.attributed_touch_id = stmc.touch_id
						   AND stmc.touch_mkt_channel LIKE ANY ('Email%', 'App Push')
			INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes stba2
					   ON sta.attributed_touch_id = stba2.touch_id
			INNER JOIN se.data.scv_touched_transactions stt
					   ON stba.touch_id = stt.touch_id
			INNER JOIN se.data.fact_booking fb
					   ON stt.booking_id = fb.booking_id
						   AND fb.booking_status_type IN ('live', 'cancelled')
		WHERE stba.touch_start_tstamp >= '2024-01-01' -- looking only at events from 2024
		  AND stba.touch_se_brand = 'SE Brand'
		  AND stba.touch_has_booking
		GROUP BY 1, 2
	),
	scv_campaign_bookings AS (
		SELECT
			eb.utm_campaign::VARCHAR AS utm_campaign,
			SUM(eb.bookings)         AS bookings,
			SUM(eb.margin_gbp)       AS margin_gbp
		FROM email_bookings eb
		GROUP BY 1
	),
	icr_campaign_bookings AS (
		SELECT
			i.campaign_id::VARCHAR AS campaign_id,
			SUM(i.bookings_lnd)    AS bookings,
			SUM(i.margin_gbp_lnd)  AS margin_gbp
		FROM data_vault_mvp.dwh.iterable_crm_reporting i
		WHERE i.send_start_date >= '2024-01-01'
		GROUP BY 1
	),
	model_data AS (
		SELECT
			COALESCE(utm_campaign::VARCHAR, campaign_id::VARCHAR) AS campaign_id,
			scs.bookings                                          AS scv_bookings,
			ics.bookings                                          AS icr_bookings,
			icr_bookings - scv_bookings                           AS diff,
			icr_bookings / scv_bookings - 1                       AS var,
			scs.margin_gbp                                        AS scv_margin_gbp,
			ics.margin_gbp                                        AS icr_margin_gbp,
			icr_margin_gbp - scv_margin_gbp                       AS diff,
			icr_margin_gbp / scv_margin_gbp - 1                   AS var
		FROM scv_campaign_bookings scs
			FULL OUTER JOIN icr_campaign_bookings ics ON scs.utm_campaign = ics.campaign_id
	)

-- SELECT * FROM model_data;
SELECT
	SUM(md.scv_bookings)                            AS total_scv_bookings,
	SUM(md.icr_bookings)                            AS total_icr_bookings,
	total_icr_bookings - total_scv_bookings         AS bookings_diff,
	total_icr_bookings / total_scv_bookings - 1     AS bookings_var,
	SUM(md.scv_margin_gbp)                          AS total_scv_margin_gbp,
	SUM(md.icr_margin_gbp)                          AS total_icr_margin_gbp,
	total_icr_margin_gbp - total_scv_margin_gbp     AS margin_gbp_diff,
	total_icr_margin_gbp / total_scv_margin_gbp - 1 AS margin_gbp_var
FROM model_data md
;

-- old icr table
WITH
	email_bookings AS (
		SELECT
			stmc.touch_id,
			COALESCE(
					stmc.utm_campaign,
					stba2.app_push_open_context:dataFields:campaignId::VARCHAR
			)                                                  AS utm_campaign,
			COUNT(DISTINCT stt.booking_id)                     AS bookings,
			SUM(fb.margin_gross_of_toms_gbp_constant_currency) AS margin_gbp
		FROM se.data.scv_touch_basic_attributes stba
			INNER JOIN se.data.scv_touch_attribution sta
					   ON stba.touch_id = sta.touch_id AND sta.attribution_model = 'last non direct'
			INNER JOIN se.data.scv_touch_marketing_channel stmc
					   ON sta.attributed_touch_id = stmc.touch_id
						   AND stmc.touch_mkt_channel LIKE ANY ('Email%', 'App Push')
			INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes stba2
					   ON sta.attributed_touch_id = stba2.touch_id
			INNER JOIN se.data.scv_touched_transactions stt
					   ON stba.touch_id = stt.touch_id
			INNER JOIN se.data.fact_booking fb
					   ON stt.booking_id = fb.booking_id
						   AND fb.booking_status_type IN ('live', 'cancelled')
		WHERE stba.touch_start_tstamp >= '2024-01-01' -- looking only at events from 2024
		  AND stba.touch_se_brand = 'SE Brand'
		  AND stba.touch_has_booking
		GROUP BY 1, 2
	),
	scv_campaign_bookings AS (
		SELECT
			eb.utm_campaign::VARCHAR AS utm_campaign,
			SUM(eb.bookings)         AS bookings,
			SUM(eb.margin_gbp)       AS margin_gbp
		FROM email_bookings eb
		GROUP BY 1
	),
	icr_campaign_bookings AS (
		SELECT
			i.campaign_id::VARCHAR AS campaign_id,
			SUM(i.bookings_lnd)    AS bookings,
			SUM(i.margin_gbp_lnd)  AS margin_gbp
		FROM data_vault_mvp.dwh.iterable_crm_reporting_old_version_20241210 i
		WHERE i.send_start_date >= '2024-01-01'
		GROUP BY 1
	),
	model_data AS (
		SELECT
			COALESCE(utm_campaign::VARCHAR, campaign_id::VARCHAR) AS campaign_id,
			scs.bookings                                          AS scv_bookings,
			ics.bookings                                          AS icr_bookings,
			icr_bookings - scv_bookings                           AS diff,
			icr_bookings / scv_bookings - 1                       AS var,
			scs.margin_gbp                                        AS scv_margin_gbp,
			ics.margin_gbp                                        AS icr_margin_gbp,
			icr_margin_gbp - scv_margin_gbp                       AS diff,
			icr_margin_gbp / scv_margin_gbp - 1                   AS var
		FROM scv_campaign_bookings scs
			FULL OUTER JOIN icr_campaign_bookings ics ON scs.utm_campaign = ics.campaign_id
	)
SELECT
	SUM(md.scv_bookings)                            AS total_scv_bookings,
	SUM(md.icr_bookings)                            AS total_icr_bookings,
	total_icr_bookings - total_scv_bookings         AS bookings_diff,
	total_icr_bookings / total_scv_bookings - 1     AS bookings_var,
	SUM(md.scv_margin_gbp)                          AS total_scv_margin_gbp,
	SUM(md.icr_margin_gbp)                          AS total_icr_margin_gbp,
	total_icr_margin_gbp - total_scv_margin_gbp     AS margin_gbp_diff,
	total_icr_margin_gbp / total_scv_margin_gbp - 1 AS margin_gbp_var
FROM model_data md
;


SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
;


-- looking at campaign ids that come up in icr that don't apear in scv
