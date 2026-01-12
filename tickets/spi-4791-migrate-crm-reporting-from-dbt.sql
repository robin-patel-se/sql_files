USE ROLE personal_role__robinpatel
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.iterable.email_send CLONE latest_vault.iterable.email_send
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.iterable.campaign CLONE latest_vault.iterable.campaign
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_attributes CLONE data_vault_mvp.dwh.user_attributes
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.iterable.email_open CLONE latest_vault.iterable.email_open
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.iterable.email_click CLONE latest_vault.iterable.email_click
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.iterable.email_unsubscribe CLONE latest_vault.iterable.email_unsubscribe
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution CLONE data_vault_mvp.single_customer_view_stg.module_touch_attribution
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions CLONE data_vault_mvp.single_customer_view_stg.module_touched_transactions
;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.fact_booking AS
SELECT *
FROM data_vault_mvp.dwh.fact_booking fb
;

self_describing_task --include 'biapp/task_catalogue/dv/dwh/email/crm_reporting/iterable/iterable_crm_reporting.py'  --method 'run' --start '2024-02-12 00:00:00' --end '2024-02-12 00:00:00'


CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.campaign AS
SELECT
	c.id                                        AS campaign_id,
	c.campaign_created_at,
	c.campaign_updated_at,
	c.start_at                                  AS campaign_start_date,
	c.ended_at                                  AS campaign_end_date,
	c.name                                      AS campaign_name,
	c.template_id,
	c.message_medium,
	c.created_by_user_id,
	c.updated_by_user_id,
	c.campaign_state,
	c.list_ids,
	c.suppression_list_ids,
	c.send_size,
	c.labels,
	c.type                                      AS campaign_type,
	c.splittable_email_name,
	c.mapped_crm_date,
	c.mapped_territory,
	c.mapped_objective,
	CASE
		WHEN LOWER(c.name) LIKE ('%ame_athena%') THEN 'Athena'
		WHEN LOWER(c.name) LIKE ('%core_athena%') THEN 'Athena'
		WHEN LOWER(c.name) LIKE ('%partner_athena%') THEN 'Athena'
		ELSE c.mapped_platform
	END                                         AS mapped_campaign,
	c.mapped_campaign                           AS mapped_promo,
	SPLIT_PART(c.splittable_email_name, '_', 6) AS mapped_theme,
	SPLIT_PART(c.splittable_email_name, '_', 8) AS mapped_segment,
	c.record,
	CASE
		WHEN LOWER(c.name) LIKE ('%ame_athena%') THEN TRUE
		WHEN LOWER(c.name) LIKE ('%core_athena%') THEN TRUE
		WHEN LOWER(c.name) LIKE ('%partner_athena%') THEN TRUE
		ELSE FALSE
	END                                         AS is_athena,
	CASE
		WHEN LOWER(c.name) = 'ame_abandon_basket' THEN 'AbandonBasket'
		WHEN LOWER(c.name) = 'ame_abandon_basket_bookinglink' THEN 'AbandonBasketbookingLink'
		WHEN LOWER(c.name) = 'ame_abandon_browse_daily' THEN 'AbandonBrowseDaily'
		WHEN LOWER(c.name) LIKE 'ame_abandon_browse_weekly_copy%' THEN 'AbandonBrowseWeekly'
		WHEN LOWER(c.name) = 'ame_welcome_01_sign_up' THEN 'WelcomeSignUp'
		WHEN LOWER(c.name) = 'ame_deal_improvement' THEN 'DealImprovement'
		WHEN LOWER(c.name) = 'ame_keyword_search' THEN 'KeywordSearch'
		WHEN LOWER(c.name) = 'ame_deal_spotlight' THEN 'DealSpotlight'
		WHEN LOWER(c.name) = 'ame_welcome_back' THEN 'WelcomeBack'
		WHEN LOWER(c.name) = 'ame_wishlist_specific_deal' THEN 'WishlistDeal'
		WHEN LOWER(c.name) = 'ame_wishlist_destination' THEN 'WishlistDestination'
		WHEN LOWER(c.name) = 'ame_destination_spotlight' THEN 'DestinationSpotlight'
		WHEN LOWER(c.name) = 'ame_welcome_04_top_ten' THEN 'Welcome4Top10'
		WHEN LOWER(c.name) = 'ame_welcome_02_inspiration' THEN 'Welcome2Inspiration'
		WHEN LOWER(c.name) = 'ame_welcome_03_site_education' THEN 'Welcome3SiteEducation'
		WHEN LOWER(c.name) = 'ame_welcome_05_trust' THEN 'Welcome5Trust'
		WHEN LOWER(c.name) = 'ame_date_spotlight' THEN 'DateSpotlight'
	END                                         AS ame_calculated_campaign_name
FROM latest_vault.iterable.campaign c
;

SELECT
	es.campaign_id,
	es.catalog_collection_count,
	es.catalog_lookup_count,
	es.channel_id,
	es.content_id,
	es.event_created_at::DATE                                                                     AS send_event_date,
	es.event_created_at::TIMESTAMP                                                                AS send_event_time,
	SHA2(es.email)                                                                                AS email_hash,
	ua.shiro_user_id,
	ua.current_affiliate_territory,
	es.message_id,
	SHA2(es.message_id || es.email)                                                               AS messageid_email_hash,
	es.message_type_id,
	es.product_recommendation_count,
	es.template_id,
	LEAD(es.event_created_at::DATE)
		 OVER (PARTITION BY es.campaign_id,SHA2(es.email) ORDER BY es.event_created_at::DATE ASC) AS lead_event_date,
	es.event_created_at::DATE                                                                     AS send_start_date,
	COALESCE(lead_event_date - 1, CURRENT_DATE + 30)                                              AS send_end_date
FROM latest_vault.iterable.email_send es
	LEFT JOIN data_vault_mvp.dwh.user_attributes ua ON ua.email = es.email AND ua.email IS NOT NULL
WHERE es.event_created_at::DATE >= '2021-11-03'

;

SELECT
	SHA2(eo.message_id || eo.email) AS messageid_email_hash,
	eo.campaign_id,
	eo.city,
	eo.country,
	eo.event_created_at::DATE       AS open_event_date,
	eo.event_created_at::TIMESTAMP  AS open_event_time,
	SHA2(eo.email)                  AS email_hash,
	SHA2(eo.ip)                     AS ip_hash,
	eo.message_id,
	eo.region,
	eo.template_id,
	eo.user_agent,
	eo.user_agent_device
FROM latest_vault.iterable.email_open eo
WHERE open_event_date >= '2021-11-03'



SELECT
	SHA2(ec.message_id || ec.email) AS messageid_email_hash,
	ec.message_id,
	ec.campaign_id,
	ec.content_id,
	ec.country,
	ec.event_created_at::DATE       AS click_event_date,
	ec.event_created_at::TIMESTAMP  AS click_event_time,
	SHA2(ec.email)                  AS email_hash,
	ec.href_index,
	SHA2(ec.ip)                     AS ip_hash,
	ec.region,
	ec.template_id,
	ec.url,
	ec.user_agent,
	ec.user_agent_device
FROM latest_vault.iterable.email_click ec
WHERE click_event_date >= '2021-11-03'


SELECT
	eu.event_created_at::DATE       AS unsub_event_date,
	eu.event_created_at::TIMESTAMP  AS unsub_event_time,
	SHA2(eu.message_id || eu.email) AS messageid_email_hash,
	eu.message_id,
	eu.campaign_id,
	eu.unsub_source
FROM latest_vault.iterable.email_unsubscribe eu
WHERE unsub_event_date >= '2021-11-03'
  AND eu.unsub_source IN ('Complaint', 'EmailLink')
;



SELECT
	es.messageid_email_hash,
	es.message_id,
	es.campaign_id,
	es.email_hash,
	es.shiro_user_id,
	es.current_affiliate_territory,
	campaign.campaign_name,
	campaign.splittable_email_name,
	campaign.mapped_crm_date,
	campaign.mapped_territory,
	campaign.mapped_objective,
	campaign.mapped_campaign,
	campaign.mapped_promo,
	campaign.mapped_theme,
	campaign.mapped_segment,
	campaign.is_athena,
	campaign.ame_calculated_campaign_name,
	es.send_event_date,
	es.send_event_time,
	es.send_start_date,
	es.send_end_date,
	COUNT(*) AS email_sends
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step02__model_sends AS es
	LEFT JOIN {{ ref('ci_iterable_email_01_campaign') }} AS campaign
ON campaign.campaign_id = es.campaign_id

	{% IF is_incremental() %}
WHERE send_event_date >= dateadd(day, -15, getdate())
	{% endif %}

GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21



WITH
	aggregate_sends AS (
		SELECT
			es.message_id_email_hash,
			es.message_id,
			es.campaign_id,
			es.email_hash,
			es.shiro_user_id,
			es.current_affiliate_territory,
			es.send_event_date,
			es.send_event_time,
			es.send_start_date,
			es.send_end_date,
			COUNT(*) AS email_sends
		FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step02__model_sends es
		GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
	)
	p-ygo
	ags.*, c.campaign_name, c.splittable_email_name, c.mapped_crm_date, c.mapped_territory, c.mapped_objective, c.mapped_campaign, c.mapped_promo, c.mapped_theme, c.mapped_segment, c.is_athena, c.ame_calculated_campaign_name
FROM aggregate_sends ags
	LEFT JOIN data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step01__campaign_enrichment c
ON ags.campaign_id = c.campaign_id

USE ROLE personal_role__parastouabbasi
;

SELECT *
FROM data_vault_mvp_dev_parastou.dwh.inactive_users
;

dataset_task --include 'cms_mongodb.booking_summary' --operation LatestRecordsOperation --method 'run' --upstream --start '2020-07-15 00:30:00' --end '2020-07-15 00:30:00'



SELECT
	'last non direct'                                  AS attribution_model,
	tmc.utm_campaign                                   AS campaign_id,
	tmc.landing_page_parameters['messageId']::VARCHAR  AS message_id,
	tt.event_tstamp::DATE                              AS event_date,
	tba.attributed_user_id                             AS shiro_user_id,
	COUNT(tt.booking_id)                               AS bookings,
	SUM(fb.margin_gross_of_toms_gbp_constant_currency) AS margin_gbp
FROM data_vault_mvp.single_customer_view_stg.module_touched_transactions tt
	INNER JOIN data_vault_mvp.dwh.fact_booking fb
			   ON fb.booking_id = tt.booking_id
				   AND fb.booking_status_type IN ('live', 'cancelled')
	INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes tba
			   ON tt.touch_id = tba.touch_id
				   AND tba.stitched_identity_type = 'se_user_id'
				   AND tba.touch_start_tstamp::DATE >= '2021-11-03'
	INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_attribution ta
			   ON tt.touch_id = ta.touch_id
				   AND ta.attribution_model = 'last non direct'
	INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel tmc
			   ON ta.attributed_touch_id = tmc.touch_id
WHERE tmc.utm_medium = 'email'
  AND tmc.utm_campaign IS NOT NULL
  AND tt.event_tstamp::DATE >= '2021-11-03'
GROUP BY 1, 2, 3, 4, 5

UNION ALL

SELECT
	'last click'                                       AS attribution_model,
	tmc.utm_campaign                                   AS campaign_id,
	tmc.landing_page_parameters['messageId']::VARCHAR  AS message_id,
	tt.event_tstamp::DATE                              AS event_date,
	tba.attributed_user_id                             AS shiro_user_id,
	COUNT(tt.booking_id)                               AS bookings,
	SUM(fb.margin_gross_of_toms_gbp_constant_currency) AS margin_gbp
FROM data_vault_mvp.single_customer_view_stg.module_touched_transactions tt
	INNER JOIN data_vault_mvp.dwh.fact_booking fb
			   ON fb.booking_id = tt.booking_id
				   AND fb.booking_status_type IN ('live', 'cancelled')
	INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes tba
			   ON tt.touch_id = tba.touch_id
				   AND tba.stitched_identity_type = 'se_user_id'
				   AND tba.touch_start_tstamp::DATE >= '2021-11-03'
	INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel tmc
			   ON tt.touch_id = tmc.touch_id
WHERE tmc.utm_medium = 'email'
  AND tmc.utm_campaign IS NOT NULL
  AND tt.event_tstamp::DATE >= '2021-11-03'
GROUP BY 1, 2, 3, 4, 5
;

SELECT *
FROM latest_vault_dev_robin.iterable.email_unsubscribe
;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step05__model_unsubs
;


SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step08__aggregate_clicks


SELECT
	'last non direct'                                 AS attribution_model,
	tmc.utm_campaign                                  AS campaign_id,
	tmc.landing_page_parameters['messageId']::VARCHAR AS message_id,
	spvs.event_tstamp::DATE                           AS event_date,
	tba.attributed_user_id                            AS shiro_user_id,
	COUNT(*)                                          AS spvs
FROM {{ ref('base_scv__module_touched_spvs') }} AS spvs
INNER JOIN {{ ref('base_scv__module_touch_basic_attributes') }} AS tba
ON spvs.touch_id = tba.touch_id
	AND tba.stitched_identity_type = 'se_user_id'
	INNER JOIN {{ ref('base_scv__module_touch_attribution') }} attr ON spvs.touch_id = attr.touch_id
	AND attr.attribution_model = 'last non direct'
	INNER JOIN {{ ref('base_scv__module_touch_marketing_channel') }} tmc ON attr.attributed_touch_id = tmc.touch_id
WHERE
	tmc.utm_medium = 'email' AND tmc.utm_campaign IS NOT NULL AND spvs.event_tstamp::DATE >='2021-11-03'
GROUP BY 1, 2, 3, 4, 5
UNION ALL

SELECT
	'last click'                                      AS attribution_model,
	tmc.utm_campaign                                  AS campaign_id,
	tmc.landing_page_parameters['messageId']::VARCHAR AS message_id,
	spvs.event_tstamp::DATE                           AS event_date,
	tba.attributed_user_id                            AS shiro_user_id,
	COUNT(*)                                          AS spvs
FROM {{ ref('base_scv__module_touched_spvs') }} AS spvs
INNER JOIN {{ ref('base_scv__module_touch_basic_attributes') }} AS tba
ON spvs.touch_id = tba.touch_id
	AND tba.stitched_identity_type = 'se_user_id'
	INNER JOIN {{ ref('base_scv__module_touch_marketing_channel') }} AS tmc
	ON spvs.touch_id = tmc.touch_id
WHERE
	tmc.utm_medium = 'email' AND tmc.utm_campaign IS NOT NULL AND spvs.event_tstamp::DATE >='2021-11-03'
GROUP BY 1, 2, 3, 4, 5
UNION ALL

SELECT
	'url params'                                                 AS attribution_model,
	PARSE_URL(spvs.page_url)['parameters']:utm_campaign::VARCHAR AS campaign_id,
	PARSE_URL(spvs.page_url)['parameters']:messageId::VARCHAR    AS message_id,
	spvs.event_tstamp::DATE                                      AS event_date,
	tba.attributed_user_id                                       AS shiro_user_id,
	COUNT(*)                                                     AS spvs
FROM {{ ref('base_scv__module_touched_spvs') }} AS spvs
INNER JOIN {{ ref('base_scv__module_touch_basic_attributes') }} AS tba
ON spvs.touch_id = tba.touch_id
	AND tba.stitched_identity_type = 'se_user_id'
WHERE
	PARSE_URL(spvs.page_url)['parameters']:utm_medium::VARCHAR = 'email' AND PARSE_URL(spvs.page_url)['parameters']:utm_campaign::VARCHAR IS NOT NULL AND spvs.event_tstamp::DATE >='2021-11-03'


	{% IF is_incremental() %} AND event_tstamp::DATE >= dateadd(day, -15, getdate())
	{% endif %}


GROUP BY 1, 2, 3, 4, 5



SELECT
	'last non direct'                                 AS attribution_model,
	tmc.utm_campaign                                  AS campaign_id,
	tmc.landing_page_parameters['messageId']::VARCHAR AS message_id,
	spvs.event_tstamp::DATE                           AS event_date,
	tba.attributed_user_id                            AS shiro_user_id,
	COUNT(*)                                          AS spvs
FROM {{ ref('base_scv__module_touched_spvs') }} AS spvs
INNER JOIN {{ ref('base_scv__module_touch_basic_attributes') }} AS tba
ON spvs.touch_id = tba.touch_id
	AND tba.stitched_identity_type = 'se_user_id'
	INNER JOIN {{ ref('base_scv__module_touch_attribution') }} attr
	ON spvs.touch_id = attr.touch_id
	AND attr.attribution_model = 'last non direct'
	INNER JOIN {{ ref('base_scv__module_touch_marketing_channel') }} tmc
	ON attr.attributed_touch_id = tmc.touch_id
WHERE
	tmc.utm_medium = 'email' AND tmc.utm_campaign IS NOT NULL AND spvs.event_tstamp::DATE >='2021-11-03'
GROUP BY 1, 2, 3, 4, 5

UNION ALL

SELECT
	'last click'                                      AS attribution_model,
	tmc.utm_campaign                                  AS campaign_id,
	tmc.landing_page_parameters['messageId']::VARCHAR AS message_id,
	spvs.event_tstamp::DATE                           AS event_date,
	tba.attributed_user_id                            AS shiro_user_id,
	COUNT(*)                                          AS spvs
FROM {{ ref('base_scv__module_touched_spvs') }} AS spvs
INNER JOIN {{ ref('base_scv__module_touch_basic_attributes') }} AS tba
ON spvs.touch_id = tba.touch_id
	AND tba.stitched_identity_type = 'se_user_id'
	INNER JOIN {{ ref('base_scv__module_touch_marketing_channel') }} AS tmc
	ON spvs.touch_id = tmc.touch_id
WHERE
	tmc.utm_medium = 'email' AND tmc.utm_campaign IS NOT NULL AND spvs.event_tstamp::DATE >='2021-11-03'
GROUP BY 1, 2, 3, 4, 5
UNION ALL

SELECT
	'url params'                                                 AS attribution_model,
	PARSE_URL(spvs.page_url)['parameters']:utm_campaign::VARCHAR AS campaign_id,
	PARSE_URL(spvs.page_url)['parameters']:messageId::VARCHAR    AS message_id,
	spvs.event_tstamp::DATE                                      AS event_date,
	tba.attributed_user_id                                       AS shiro_user_id,
	COUNT(*)                                                     AS spvs
FROM {{ ref('base_scv__module_touched_spvs') }} AS spvs
INNER JOIN {{ ref('base_scv__module_touch_basic_attributes') }} AS tba
ON spvs.touch_id = tba.touch_id
	AND tba.stitched_identity_type = 'se_user_id'
WHERE
	PARSE_URL(spvs.page_url)['parameters']:utm_medium::VARCHAR = 'email' AND PARSE_URL(spvs.page_url)['parameters']:utm_campaign::VARCHAR IS NOT NULL AND spvs.event_tstamp::DATE >='2021-11-03'


	{% IF is_incremental() %} AND event_tstamp::DATE >= dateadd(day, -15, getdate())
	{% endif %}


GROUP BY 1, 2, 3, 4, 5


WITH
	stack_spvs AS (
		SELECT
			'last click'                                      AS attribution_model,
			tmc.utm_campaign                                  AS campaign_id,
			tmc.landing_page_parameters['messageId']::VARCHAR AS message_id,
			spvs.event_tstamp::DATE                           AS event_date,
			tba.attributed_user_id                            AS shiro_user_id,
			COUNT(*)                                          AS spvs
		FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs AS spvs
			INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes AS tba
					   ON spvs.touch_id = tba.touch_id
						   AND tba.stitched_identity_type = 'se_user_id'
			INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel AS tmc
					   ON spvs.touch_id = tmc.touch_id
		WHERE tmc.utm_medium = 'email'
		  AND tmc.utm_campaign IS NOT NULL
		  AND spvs.event_tstamp::DATE >= '2021-11-03'
		GROUP BY 1, 2, 3, 4, 5

		UNION ALL

		SELECT
			'url params'                                                 AS attribution_model,
			PARSE_URL(spvs.page_url)['parameters']:utm_campaign::VARCHAR AS campaign_id,
			PARSE_URL(spvs.page_url)['parameters']:messageId::VARCHAR    AS message_id,
			spvs.event_tstamp::DATE                                      AS event_date,
			tba.attributed_user_id                                       AS shiro_user_id,
			COUNT(*)                                                     AS spvs
		FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs AS spvs
			INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes tba.touch_id
	AND tba.stitched_identity_type = 'se_user_id'
WHERE
	PARSE_URL(spvs.page_url)['parameters']:utm_medium::VARCHAR = 'email' AND PARSE_URL(spvs.page_url)['parameters']:utm_campaign::VARCHAR IS NOT NULL AND spvs.event_tstamp::DATE >='2021-11-03'
GROUP BY 1, 2, 3, 4, 5
	)

SELECT
	spv.attribution_model,
	spv.campaign_id,
	spv.message_id,
	spv.shiro_user_id,
	SUM(spv.spvs) AS spvs
FROM stack_spvs spv
GROUP BY 1, 2, 3, 4



SELECT
	em.messageid_email_hash,
	em.message_id,
	em.campaign_id,
	em.splittable_email_name,
	em.mapped_crm_date,
	em.mapped_territory,
	em.current_affiliate_territory,
	em.mapped_objective,
	em.mapped_campaign,
	em.mapped_promo,
	em.mapped_theme,
	em.mapped_segment,
	em.is_athena,
	em.ame_calculated_campaign_name,
	em.email_hash,
	em.shiro_user_id,
	em.campaign_name,
	em.send_event_date,
	em.send_event_time,
	em.send_start_date,
	em.send_end_date,
	COALESCE(em.email_sends, 0)            AS email_sends,
	COALESCE(em.email_opens, 0)            AS email_opens,
	COALESCE(em.unique_email_opens, 0)     AS unique_email_opens,
	COALESCE(em.email_clicks, 0)           AS email_clicks,
	COALESCE(em.unique_email_clicks, 0)    AS unique_email_clicks,
	em.first_open_event_date,
	em.first_open_event_time,
	em.first_click_event_date,
	em.first_click_event_time,
	em.unsub_event_date,
	em.unsub_event_time,
	COALESCE(em.email_unsubs, 0)           AS email_unsubs,
	COALESCE(em.email_unsubs_complaint, 0) AS email_unsubs_complaint,
	COALESCE(em.email_unsubs_emaillink, 0) AS email_unsubs_emaillink,
	COALESCE(bk_lc.bookings, 0)            AS bookings_lc,
	COALESCE(bk_lc.margin_gbp, 0)          AS margin_gbp_lc,
	COALESCE(bk_lnd.bookings, 0)           AS bookings_lnd,
	COALESCE(bk_lnd.margin_gbp, 0)         AS margin_gbp_lnd,
	COALESCE(spv_lc.spvs, 0)               AS spvs_lc,
	COALESCE(spv_lnd.spvs, 0)              AS spvs_lnd,
	COALESCE(url.spvs, 0)                  AS spvs_url
FROM {{ ref('ci_iterable_email_04_combine_email_metrics') }} em
LEFT JOIN scv_bookings bk_lnd
ON COALESCE(bk_lnd.message_id, em.message_id) = em.message_id
	AND bk_lnd.campaign_id::VARCHAR = em.campaign_id::VARCHAR
	AND bk_lnd.shiro_user_id = em.shiro_user_id
	AND bk_lnd.attribution_model = 'last non direct'
	LEFT JOIN scv_bookings bk_lc
	ON COALESCE(bk_lc.message_id, em.message_id) = em.message_id
	AND bk_lc.campaign_id::VARCHAR = em.campaign_id::VARCHAR
	AND bk_lc.shiro_user_id = em.shiro_user_id
	AND bk_lc.attribution_model = 'last click'
	LEFT JOIN scv_spvs spv_lnd
	ON COALESCE(spv_lnd.message_id, em.message_id) = em.message_id
	AND spv_lnd.campaign_id::VARCHAR = em.campaign_id::VARCHAR
	AND spv_lnd.shiro_user_id = em.shiro_user_id
	AND spv_lnd.attribution_model = 'last non direct'
	LEFT JOIN scv_spvs spv_lc
	ON COALESCE(spv_lc.message_id, em.message_id) = em.message_id
	AND spv_lc.campaign_id::VARCHAR = em.campaign_id::VARCHAR
	AND spv_lc.shiro_user_id = em.shiro_user_id
	AND spv_lc.attribution_model = 'last click'
	LEFT JOIN scv_spvs URL
	ON COALESCE(URL.message_id, em.message_id) = em.message_id
	AND URL.campaign_id::VARCHAR = em.campaign_id::VARCHAR
	AND URL.shiro_user_id = em.shiro_user_id
	AND URL.attribution_model = 'url params'
WHERE em.ame_calculated_campaign_name IS NULL
;

ALTER TABLE data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step13__model_non_ame_campaigns
	RENAME COLUMN email_unsubs_emaillink TO email_unsubs_email_link
;



SELECT *
FROM se.data.se_calendar sc
WHERE sc.date_value = '2022-12-31'


SELECT *
FROM latest_vault.iterable.email_unsubscribe eu
;

SELECT GET_DDL('table', 'data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__model_data')
;


CREATE OR REPLACE TRANSIENT TABLE iterable_crm_reporting__model_data
(
	message_id_email_hash        VARCHAR,
	message_id                   VARCHAR,
	campaign_id                  NUMBER,
	combined_email_name          VARCHAR,
	trading                      VARCHAR,
	email_type                   VARCHAR,
	splittable_email_name        VARCHAR,
	mapped_crm_date              VARCHAR,
	mapped_territory             VARCHAR,
	current_affiliate_territory  VARCHAR,
	mapped_objective             VARCHAR,
	mapped_campaign              VARCHAR,
	mapped_promo                 VARCHAR,
	mapped_theme                 VARCHAR,
	mapped_segment               VARCHAR,
	is_athena                    BOOLEAN,
	ame_calculated_campaign_name VARCHAR,
	email_hash                   VARCHAR,
	shiro_user_id                NUMBER,
	campaign_name                VARCHAR,
	send_event_date              DATE,
	send_event_time              TIMESTAMP,
	send_start_date              DATE,
	send_end_date                DATE,
	email_sends                  NUMBER,
	email_opens                  NUMBER,
	unique_email_opens           NUMBER,
	email_clicks                 NUMBER,
	unique_email_clicks          NUMBER,
	first_open_event_date        DATE,
	first_open_event_time        TIMESTAMP,
	first_click_event_date       DATE,
	first_click_event_time       TIMESTAMP,
	unsub_event_date             DATE,
	unsub_event_time             TIMESTAMP,
	email_unsubs                 NUMBER,
	email_unsubs_complaint       NUMBER,
	email_unsubs_email_link      NUMBER,
	bookings_lc                  NUMBER,
	margin_gbp_lc                NUMBER,
	bookings_lnd                 NUMBER,
	margin_gbp_lnd               NUMBER,
	spvs_lc                      NUMBER,
	spvs_lnd                     NUMBER,
	spvs_url                     NUMBER
)
;


SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step10__model_iterable_data
;


USE ROLE pipelinerunner
;

CREATE SCHEMA collab.crm_reporting


;

CREATE OR REPLACE VIEW collab.crm_reporting.iterable_crm_reporting COPY GRANTS AS
SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting
;

GRANT USAGE ON SCHEMA collab.crm_reporting TO ROLE customer_insight_team
;

GRANT USAGE ON SCHEMA collab.crm_reporting TO ROLE data_team_basic
;

GRANT SELECT ON TABLE collab.crm_reporting.iterable_crm_reporting TO ROLE data_team_basic
;

GRANT SELECT ON TABLE collab.crm_reporting.iterable_crm_reporting TO ROLE customer_insight_team
;

USE ROLE personal_role__robinpatel
;

SELECT *
FROM collab.crm_reporting.iterable_crm_reporting
;

SELECT *
FROM dbt.bi_customer_insight.ci_iterable_email_crm_reporting ciecr
;

------------------------------------------------------------------------------------------------------------------------

-- debugging high margin

SELECT
	send_event_date,
	SUM(margin_gbp_lnd)
FROM collab.crm_reporting.iterable_crm_reporting
WHERE send_event_date BETWEEN ('2023-08-07') AND ('2023-08-13')
GROUP BY 1
;

USE ROLE pipelinerunner
;

USE WAREHOUSE pipe_2xlarge
;

CREATE OR REPLACE TRANSIENT TABLE collab.crm_reporting.iterable_crm_reporting_2023_08_08 AS
SELECT *
FROM collab.crm_reporting.iterable_crm_reporting icr
WHERE icr.send_event_date = '2023-08-08'
;

GRANT SELECT ON TABLE collab.crm_reporting.iterable_crm_reporting_2023_08_08 TO ROLE data_team_basic
;

GRANT SELECT ON TABLE collab.crm_reporting.iterable_crm_reporting_2023_08_08 TO ROLE customer_insight_team
;

SELECT
	icr.campaign_id,
	SUM(icr.margin_gbp_lnd)
FROM collab.crm_reporting.iterable_crm_reporting_2023_08_08 icr
GROUP BY 1
ORDER BY 2 DESC
;

SELECT *
FROM collab.crm_reporting.iterable_crm_reporting_2023_08_08 icr
WHERE icr.campaign_id = 3305223
ORDER BY margin_gbp_lnd DESC
;


SELECT
	SUM(icr.margin_gbp)
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step11__model_scv_booking_data icr
WHERE icr.campaign_id = '3305223'
  AND icr.attribution_model = 'last non direct'
;


SELECT
	SUM(icr.margin_gbp)
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step11__model_scv_booking_data icr
WHERE icr.campaign_id = '3305223'
  AND icr.shiro_user_id = '80223052'
  AND icr.attribution_model = 'last non direct'
;


SELECT
	es.campaign_id,
	es.catalog_collection_count,
	es.catalog_lookup_count,
	es.channel_id,
	es.content_id,
	es.event_created_at::DATE                                                                     AS send_event_date,
	es.event_created_at::TIMESTAMP                                                                AS send_event_time,
	ua.shiro_user_id,
	ua.current_affiliate_territory,
	es.message_id,
	es.email,
	SHA2(es.email)                                                                                AS email_hash,
	SHA2(es.message_id || es.email)                                                               AS message_id_email_hash,
	es.message_type_id,
	es.product_recommendation_count,
	es.template_id,
	LEAD(es.event_created_at::DATE)
		 OVER (PARTITION BY es.campaign_id,SHA2(es.email) ORDER BY es.event_created_at::DATE ASC) AS lead_event_date,
	es.event_created_at::DATE                                                                     AS send_start_date,
	COALESCE(lead_event_date - 1, CURRENT_DATE + 30)                                              AS send_end_date
FROM latest_vault_dev_robin.iterable.email_send es
	LEFT JOIN data_vault_mvp_dev_robin.dwh.user_attributes ua ON ua.email = es.email AND ua.email IS NOT NULL
WHERE es.event_created_at::DATE >= '2021-11-03'
  AND ua.shiro_user_id = 80223052
;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step10__model_iterable_data
WHERE iterable_crm_reporting__step10__model_iterable_data.message_id_email_hash =
	  '0b12d3749be3dfebddb7aef350c8e4f3e4114c4a63abb4a00bf4f6d4cb8c522f'
;



WITH
	scv_bookings AS (
		SELECT
			bk.attribution_model,
			bk.campaign_id,
			bk.message_id,
			bk.shiro_user_id,
			SUM(bookings)   AS bookings,
			SUM(margin_gbp) AS margin_gbp
		FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step11__model_scv_booking_data bk
		GROUP BY 1, 2, 3, 4
	),
	scv_spvs AS (
		SELECT
			spv.attribution_model,
			spv.campaign_id,
			spv.message_id,
			spv.shiro_user_id,
			SUM(spv.spvs) AS spvs
		FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step12__model_scv_spv_data spv
		GROUP BY 1, 2, 3, 4
	)
SELECT
	em.message_id_email_hash,
	em.message_id,
	em.campaign_id,
	em.splittable_email_name,
	em.mapped_crm_date,
	em.mapped_territory,
	em.current_affiliate_territory,
	em.mapped_objective,
	em.mapped_platform,
	em.mapped_campaign,
	em.mapped_theme,
	em.mapped_segment,
	em.is_athena,
	em.ame_calculated_campaign_name,
	em.email_hash,
	em.shiro_user_id,
	em.campaign_name,
	em.send_event_date,
	em.send_event_time,
	em.send_start_date,
	em.send_end_date,
	COALESCE(em.email_sends, 0)             AS email_sends,
	COALESCE(em.email_opens, 0)             AS email_opens,
	COALESCE(em.unique_email_opens, 0)      AS unique_email_opens,
	COALESCE(em.email_clicks, 0)            AS email_clicks,
	COALESCE(em.unique_email_clicks, 0)     AS unique_email_clicks,
	em.first_open_event_date,
	em.first_open_event_time,
	em.first_click_event_date,
	em.first_click_event_time,
	em.unsub_event_date,
	em.unsub_event_time,
	COALESCE(em.email_unsubs, 0)            AS email_unsubs,
	COALESCE(em.email_unsubs_complaint, 0)  AS email_unsubs_complaint,
	COALESCE(em.email_unsubs_email_link, 0) AS email_unsubs_email_link,
	COALESCE(bk_lc.bookings, 0)             AS bookings_lc,
	COALESCE(bk_lc.margin_gbp, 0)           AS margin_gbp_lc,
	COALESCE(bk_lnd.bookings, 0)            AS bookings_lnd,
	COALESCE(bk_lnd.margin_gbp, 0)          AS margin_gbp_lnd,
	COALESCE(spv_lc.spvs, 0)                AS spvs_lc,
	COALESCE(spv_lnd.spvs, 0)               AS spvs_lnd,
	COALESCE(url.spvs, 0)                   AS spvs_url
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step10__model_iterable_data em
	LEFT JOIN scv_bookings bk_lnd
			  ON COALESCE(bk_lnd.message_id, em.message_id) = em.message_id
				  AND bk_lnd.campaign_id::VARCHAR = em.campaign_id::VARCHAR
				  AND bk_lnd.shiro_user_id = em.shiro_user_id
				  AND bk_lnd.attribution_model = 'last non direct'
	LEFT JOIN scv_bookings bk_lc
			  ON COALESCE(bk_lc.message_id, em.message_id) = em.message_id
				  AND bk_lc.campaign_id::VARCHAR = em.campaign_id::VARCHAR
				  AND bk_lc.shiro_user_id = em.shiro_user_id
				  AND bk_lc.attribution_model = 'last click'
	LEFT JOIN scv_spvs spv_lnd
			  ON COALESCE(spv_lnd.message_id, em.message_id) = em.message_id
				  AND spv_lnd.campaign_id::VARCHAR = em.campaign_id::VARCHAR
				  AND spv_lnd.shiro_user_id = em.shiro_user_id
				  AND spv_lnd.attribution_model = 'last non direct'
	LEFT JOIN scv_spvs spv_lc
			  ON COALESCE(spv_lc.message_id, em.message_id) = em.message_id
				  AND spv_lc.campaign_id::VARCHAR = em.campaign_id::VARCHAR
				  AND spv_lc.shiro_user_id = em.shiro_user_id
				  AND spv_lc.attribution_model = 'last click'
	LEFT JOIN scv_spvs url
			  ON COALESCE(url.message_id, em.message_id) = em.message_id
				  AND url.campaign_id::VARCHAR = em.campaign_id::VARCHAR
				  AND url.shiro_user_id = em.shiro_user_id
				  AND url.attribution_model = 'url params'
;



SELECT
	em.message_id_email_hash,
	SUM(bk_lc.bookings)    AS bookings_lc,
	SUM(bk_lc.margin_gbp)  AS margin_gbp_lc,
	SUM(bk_lnd.bookings)   AS bookings_lnd,
	SUM(bk_lnd.margin_gbp) AS margin_gbp_lnd
-- 	SUM(spv_lc.spvs)       AS spvs_lc,
-- 	SUM(spv_lnd.spvs)      AS spvs_lnd,
-- 	SUM(url.spvs)          AS spvs_url
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step13__model_non_ame_campaigns em
	LEFT JOIN data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step11__model_scv_booking_data bk_lnd
			  ON COALESCE(bk_lnd.message_id, em.message_id) = em.message_id
				  AND bk_lnd.campaign_id::VARCHAR = em.campaign_id::VARCHAR
				  AND bk_lnd.shiro_user_id = em.shiro_user_id
				  AND bk_lnd.attribution_model = 'last non direct'
				  AND bk_lnd.event_date BETWEEN em.send_start_date AND em.send_end_date
	LEFT JOIN data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step11__model_scv_booking_data bk_lc
			  ON COALESCE(bk_lc.message_id, em.message_id) = em.message_id
				  AND bk_lc.campaign_id::VARCHAR = em.campaign_id::VARCHAR
				  AND bk_lc.shiro_user_id = em.shiro_user_id
				  AND bk_lc.attribution_model = 'last click'
				  AND bk_lc.event_date BETWEEN em.send_start_date AND em.send_end_date
	LEFT JOIN data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step12__model_scv_spv_data spv_lnd
			  ON COALESCE(spv_lnd.message_id, em.message_id) = em.message_id
				  AND spv_lnd.campaign_id::VARCHAR = em.campaign_id::VARCHAR
				  AND spv_lnd.shiro_user_id = em.shiro_user_id
				  AND spv_lnd.attribution_model = 'last non direct'
				  AND spv_lnd.event_date BETWEEN em.send_start_date AND em.send_end_date
-- 	LEFT JOIN data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step12__model_scv_spv_data spv_lc
-- 			  ON COALESCE(spv_lc.message_id, em.message_id) = em.message_id
-- 				  AND spv_lc.campaign_id::VARCHAR = em.campaign_id::VARCHAR
-- 				  AND spv_lc.shiro_user_id = em.shiro_user_id
-- 				  AND spv_lc.attribution_model = 'last click'
-- 				  AND spv_lc.event_date BETWEEN em.send_start_date AND em.send_end_date
-- 	LEFT JOIN data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step12__model_scv_spv_data url
-- 			  ON COALESCE(url.message_id, em.message_id) = em.message_id
-- 				  AND url.campaign_id::VARCHAR = em.campaign_id::VARCHAR
-- 				  AND url.shiro_user_id = em.shiro_user_id
-- 				  AND url.attribution_model = 'url params'
-- 				  AND url.event_date BETWEEN em.send_start_date AND em.send_end_date
WHERE em.ame_calculated_campaign_name IS NOT NULL
  AND em.message_id_email_hash = '0b12d3749be3dfebddb7aef350c8e4f3e4114c4a63abb4a00bf4f6d4cb8c522f'
GROUP BY 1


;


SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step11__model_scv_booking_data bk
	INNER JOIN data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step13__model_non_ame_campaigns em
			   ON COALESCE(bk.message_id, em.message_id) = em.message_id
				   AND bk.campaign_id::VARCHAR = em.campaign_id::VARCHAR
				   AND bk.shiro_user_id = em.shiro_user_id
				   AND bk.event_date BETWEEN em.send_start_date AND em.send_end_date
WHERE em.ame_calculated_campaign_name IS NOT NULL
  AND em.message_id_email_hash = '0b12d3749be3dfebddb7aef350c8e4f3e4114c4a63abb4a00bf4f6d4cb8c522f'



SELECT
	em.message_id_email_hash,
	SUM(IFF(bk.attribution_model = 'last click', bk.bookings, 0))        AS bookings_lc,
	SUM(IFF(bk.attribution_model = 'last click', bk.margin_gbp, 0))      AS margin_gbp_lc,
	SUM(IFF(bk.attribution_model = 'last non direct', bk.bookings, 0))   AS bookings_lc,
	SUM(IFF(bk.attribution_model = 'last non direct', bk.margin_gbp, 0)) AS margin_gbp_lc
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step11__model_scv_booking_data bk
	INNER JOIN data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step13__model_non_ame_campaigns em
			   ON COALESCE(bk.message_id, em.message_id) = em.message_id
				   AND bk.campaign_id::VARCHAR = em.campaign_id::VARCHAR
				   AND bk.shiro_user_id = em.shiro_user_id
				   AND bk.event_date BETWEEN em.send_start_date AND em.send_end_date
WHERE em.ame_calculated_campaign_name IS NOT NULL
  AND em.message_id_email_hash = '0b12d3749be3dfebddb7aef350c8e4f3e4114c4a63abb4a00bf4f6d4cb8c522f'
GROUP BY 1
;

SELECT
	em.message_id_email_hash,
	SUM(IFF(spvs.attribution_model = 'last click', spvs.spvs, 0))      AS spvs_lc,
	SUM(IFF(spvs.attribution_model = 'last non direct', spvs.spvs, 0)) AS spvs_lnd,
	SUM(IFF(spvs.attribution_model = 'url params', spvs.spvs, 0))      AS spvs_url
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step12__model_scv_spv_data spvs
	INNER JOIN data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step13__model_non_ame_campaigns em
			   ON COALESCE(spvs.message_id, em.message_id) = em.message_id
				   AND spvs.campaign_id::VARCHAR = em.campaign_id::VARCHAR
				   AND spvs.shiro_user_id = em.shiro_user_id
				   AND spvs.event_date BETWEEN em.send_start_date AND em.send_end_date
WHERE em.ame_calculated_campaign_name IS NOT NULL
  AND em.message_id_email_hash = '0b12d3749be3dfebddb7aef350c8e4f3e4114c4a63abb4a00bf4f6d4cb8c522f'
GROUP BY 1
;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step13__model_non_ame_campaigns em
	LEFT JOIN data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step11__model_scv_booking_data bk_lnd
			  ON COALESCE(bk_lnd.message_id, em.message_id) = em.message_id
				  AND bk_lnd.campaign_id::VARCHAR = em.campaign_id::VARCHAR
				  AND bk_lnd.shiro_user_id = em.shiro_user_id
				  AND bk_lnd.attribution_model = 'last non direct'
				  AND bk_lnd.event_date BETWEEN em.send_start_date AND em.send_end_date
	LEFT JOIN data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step11__model_scv_booking_data bk
			  ON COALESCE(bk.message_id, em.message_id) = em.message_id
				  AND bk_lc.campaign_id::VARCHAR = em.campaign_id::VARCHAR
				  AND bk_lc.shiro_user_id = em.shiro_user_id
				  AND bk_lc.attribution_model = 'last click'
				  AND bk_lc.event_date BETWEEN em.send_start_date AND em.send_end_date
-- 	LEFT JOIN data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step12__model_scv_spv_data spv_lnd
-- 			  ON COALESCE(spv_lnd.message_id, em.message_id) = em.message_id
-- 				  AND spv_lnd.campaign_id::VARCHAR = em.campaign_id::VARCHAR
-- 				  AND spv_lnd.shiro_user_id = em.shiro_user_id
-- 				  AND spv_lnd.attribution_model = 'last non direct'
-- 				  AND spv_lnd.event_date BETWEEN em.send_start_date AND em.send_end_date
-- 	LEFT JOIN data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step12__model_scv_spv_data spv_lc
-- 			  ON COALESCE(spv_lc.message_id, em.message_id) = em.message_id
-- 				  AND spv_lc.campaign_id::VARCHAR = em.campaign_id::VARCHAR
-- 				  AND spv_lc.shiro_user_id = em.shiro_user_id
-- 				  AND spv_lc.attribution_model = 'last click'
-- 				  AND spv_lc.event_date BETWEEN em.send_start_date AND em.send_end_date
-- 	LEFT JOIN data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step12__model_scv_spv_data url
-- 			  ON COALESCE(url.message_id, em.message_id) = em.message_id
-- 				  AND url.campaign_id::VARCHAR = em.campaign_id::VARCHAR
-- 				  AND url.shiro_user_id = em.shiro_user_id
-- 				  AND url.attribution_model = 'url params'
-- 				  AND url.event_date BETWEEN em.send_start_date AND em.send_end_date
WHERE em.ame_calculated_campaign_name IS NOT NULL
  AND em.message_id_email_hash = '0b12d3749be3dfebddb7aef350c8e4f3e4114c4a63abb4a00bf4f6d4cb8c522f'



SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step11__model_scv_booking_data sbd
WHERE sbd.campaign_id = '7460097'
  AND sbd.attribution_model = 'last non direct'
;



SELECT
	'last non direct'                                 AS attribution_model,
	tmc.utm_campaign                                  AS campaign_id,
	tmc.landing_page_parameters['messageId']::VARCHAR AS message_id,
	tt.event_tstamp::DATE                             AS event_date,
	tba.attributed_user_id                            AS shiro_user_id,
	tt.booking_id                                     AS bookings,
	fb.margin_gross_of_toms_gbp_constant_currency     AS margin_gbp
FROM se.data.scv_touched_transactions tt
	INNER JOIN se.data.fact_booking fb
			   ON fb.booking_id = tt.booking_id
				   AND fb.booking_status_type IN ('live', 'cancelled')
	INNER JOIN se.data_pii.scv_touch_basic_attributes tba
			   ON tt.touch_id = tba.touch_id
				   AND tba.stitched_identity_type = 'se_user_id'
				   AND tba.touch_start_tstamp::DATE >= '2021-11-03'
	INNER JOIN se.data.scv_touch_attribution ta
			   ON tt.touch_id = ta.touch_id
				   AND ta.attribution_model = 'last non direct'
	INNER JOIN se.data.scv_touch_marketing_channel tmc
			   ON ta.attributed_touch_id = tmc.touch_id
WHERE tmc.utm_medium = 'email'
--   AND tmc.utm_campaign IS NOT NULL
  AND tmc.utm_campaign = '7460097'
  AND tt.event_tstamp::DATE >= '2021-11-03'

SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step10__model_iterable_data
WHERE iterable_crm_reporting__step10__model_iterable_data.campaign_id = '7460097'

-- CODE
SELECT
	SUM(margin_gbp_lnd),
	SUM(bookings_lnd)
FROM collab.crm_reporting.iterable_crm_reporting
WHERE campaign_id = '7460097'
;

-- CODE
SELECT
	SUM(margin_gbp_lnd),
	SUM(bookings_lnd)
FROM collab.crm_reporting.iterable_crm_reporting
WHERE campaign_id = '6397263'
;


-- theory being that the join from email to user id in 'step02__model_sends' isn't solid

SELECT
	SUM(mnac.margin_gbp_lnd),
	SUM(mnac.bookings_lnd)
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step13__model_non_ame_campaigns mnac
WHERE mnac.campaign_id = '7460097'


-- raw sql returns 145 bookings, when joined to crm data it only returns 138
-- checking from the list of lnd bookings on the campaign which ones are missing
WITH
	input_bookings AS (
		SELECT
			'last non direct'                                  AS attribution_model,
			tmc.utm_campaign                                   AS campaign_id,
			tmc.landing_page_parameters['messageId']::VARCHAR  AS message_id,
			tt.event_tstamp::DATE                              AS event_date,
			tba.attributed_user_id                             AS shiro_user_id,
			COUNT(tt.booking_id)                               AS bookings,
			SUM(fb.margin_gross_of_toms_gbp_constant_currency) AS margin_gbp
		FROM se.data.scv_touched_transactions tt
			INNER JOIN se.data.fact_booking fb
					   ON fb.booking_id = tt.booking_id
						   AND fb.booking_status_type IN ('live', 'cancelled')
			INNER JOIN se.data_pii.scv_touch_basic_attributes tba
					   ON tt.touch_id = tba.touch_id
						   AND tba.stitched_identity_type = 'se_user_id'
						   AND tba.touch_start_tstamp::DATE >= '2021-11-03'
			INNER JOIN se.data.scv_touch_attribution ta
					   ON tt.touch_id = ta.touch_id
						   AND ta.attribution_model = 'last non direct'
			INNER JOIN se.data.scv_touch_marketing_channel tmc
					   ON ta.attributed_touch_id = tmc.touch_id
		WHERE tmc.utm_medium = 'email'
--   AND tmc.utm_campaign IS NOT NULL
		  AND tmc.utm_campaign = '7460097'
		  AND tt.event_tstamp::DATE >= '2021-11-03'
		GROUP BY 1, 2, 3, 4, 5
	),
	scv_bookings AS (
		SELECT
			bk.attribution_model,
			bk.campaign_id,
			bk.message_id,
			bk.shiro_user_id,
			SUM(bookings)   AS bookings,
			SUM(margin_gbp) AS margin_gbp
		FROM input_bookings bk
		GROUP BY 1, 2, 3, 4
	)

SELECT
	SUM(margin_gbp),
	SUM(bookings)
FROM scv_bookings bk_lnd
	LEFT JOIN data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step10__model_iterable_data em
			  ON COALESCE(bk_lnd.message_id, em.message_id) = em.message_id
				  AND bk_lnd.campaign_id::VARCHAR = em.campaign_id::VARCHAR
				  AND bk_lnd.shiro_user_id = em.shiro_user_id
				  AND bk_lnd.attribution_model = 'last non direct'
				  AND em.ame_calculated_campaign_name IS NULL
WHERE em.campaign_id = '7460097'


USE WAREHOUSE pipe_xlarge
;

WITH
	scv_bookings AS (
		SELECT DISTINCT
			tba.attributed_user_id AS shiro_user_id
		FROM se.data.scv_touched_transactions tt
			INNER JOIN se.data.fact_booking fb
					   ON fb.booking_id = tt.booking_id
						   AND fb.booking_status_type IN ('live', 'cancelled')
			INNER JOIN se.data_pii.scv_touch_basic_attributes tba
					   ON tt.touch_id = tba.touch_id
						   AND tba.stitched_identity_type = 'se_user_id'
						   AND tba.touch_start_tstamp::DATE >= '2021-11-03'
			INNER JOIN se.data.scv_touch_attribution ta
					   ON tt.touch_id = ta.touch_id
						   AND ta.attribution_model = 'last non direct'
			INNER JOIN se.data.scv_touch_marketing_channel tmc
					   ON ta.attributed_touch_id = tmc.touch_id
		WHERE tmc.utm_medium = 'email'
--   AND tmc.utm_campaign IS NOT NULL
		  AND tmc.utm_campaign = '7460097'
		  AND tt.event_tstamp::DATE >= '2021-11-03'
	),
	campaign_users AS (
		SELECT DISTINCT
			em.shiro_user_id
		FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step10__model_iterable_data em
		WHERE em.campaign_id = '7460097'
	)
SELECT
	s.shiro_user_id,
	em.shiro_user_id
FROM scv_bookings s
	LEFT JOIN campaign_users em ON s.shiro_user_id = em.shiro_user_id

-- confirmed there are 6 bookings that are being stripped out due no match on shiro user id of a send

;
/* these users have a booking by campaign etc but aren't in the email send list
SHIRO_USER_ID
79166030
24387061
27438783
12768081
80777322


 */

SELECT
	tba.attributed_user_id AS shiro_user_id,
	fb.booking_id,
	tmc.utm_campaign,
	tmc.utm_medium,
	fb.margin_gross_of_toms_gbp_constant_currency
FROM se.data.scv_touched_transactions tt
	INNER JOIN se.data.fact_booking fb
			   ON fb.booking_id = tt.booking_id
				   AND fb.booking_status_type IN ('live', 'cancelled')
	INNER JOIN se.data_pii.scv_touch_basic_attributes tba
			   ON tt.touch_id = tba.touch_id
				   AND tba.stitched_identity_type = 'se_user_id'
				   AND tba.touch_start_tstamp::DATE >= '2021-11-03'
	INNER JOIN se.data.scv_touch_attribution ta
			   ON tt.touch_id = ta.touch_id
				   AND ta.attribution_model = 'last non direct'
	INNER JOIN se.data.scv_touch_marketing_channel tmc
			   ON ta.attributed_touch_id = tmc.touch_id
WHERE tmc.utm_medium = 'email'
--   AND tmc.utm_campaign IS NOT NULL
  AND tmc.utm_campaign = '7460097'
  AND tt.event_tstamp::DATE >= '2021-11-03'
  AND tba.attributed_user_id IN (
								 79166030,
								 24387061,
								 27438783,
								 12768081,
								 80777322
	)
;

/* these booking ids were from bookings for users that couldn't be matched to a send
BOOKING_ID
A15311632
A15441338
A15217896
A15265257
A15473640
A15231035

 */

SELECT *
FROM se.data.fact_complete_booking fcb
WHERE fcb.booking_id IN (
						 'A15311632',
						 'A15441338',
						 'A15217896',
						 'A15265257',
						 'A15473640',
						 'A15231035'
	)
;


-- find session where booking occurred
SELECT *
FROM se.data.scv_touched_transactions stt
WHERE stt.booking_id = 'A15231035'

-- user id 26019184
SELECT *
FROM se.data_pii.scv_touch_basic_attributes stba
	INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
WHERE stba.touch_id = '0f8fa711e319d6eedcca803c84bae74a1226195119410889f20223583d41b1e3'
  AND stba.touch_start_tstamp::DATE = '2023-08-13'

--checking raw vault on user emails to see if this user id has any other emails

SELECT *
FROM raw_vault.cms_mysql.shiro_user su
WHERE id = 79929056
;

SELECT *
FROM data_vault_mvp.dwh.user_attributes ua
WHERE ua.shiro_user_id IN (79166030,
						   24387061,
						   27438783,
						   12768081,
						   80777322)

SELECT *
FROM latest_vault.iterable.email_send es
WHERE es.campaign_id = 7460097
  AND es.email = 'gjm9g7hjw2@privaterelay.appleid.com'
;


-- CODE
SELECT
	send_event_date,
	SUM(margin_gbp_lnd)
FROM collab.crm_reporting.iterable_crm_reporting
WHERE send_event_date BETWEEN ('2023-08-07') AND ('2023-08-13')
GROUP BY 1
;

USE WAREHOUSE pipe_2xlarge
;

SELECT *
FROM collab.crm_reporting.iterable_crm_reporting
QUALIFY COUNT(*) OVER (PARTITION BY message_id_email_hash) > 1
;


/*
 Duplicates in output data on primary key:

message_id_email_hash: c1c24958e377277bdfd2223888770cfcba4edf3067aeb8dcdd9ed94026e259b1
campaign_id: 4267841
ame_calculated_campaign_name: null
message_id: d59f2a84cdd5406885ed7d88af04aa23
rows: 3


message_id_email_hash: 833fe149b7efd20ccf200f5945d6e526cd6d9f8de264c45ff2b484a49dcaf0d3
campaign_id: 6598135
ame_calculated_campaign_name: null
message_id: 3f9b793619a04a35ab40480c331f9f3a
rows: 3


message_id_email_hash: 158140d3119cae0ea472dd031347d2c6790ac6a334aff646d63d142ad7c4ec02
campaign_id: 7504875
ame_calculated_campaign_name: null
message_id: a214bef251f14d9ba857ec3ad3d5f6a5
rows: 4


message_id_email_hash: 02403037c4b7713579ee91ccfdcc24738be3aad4cadb687484be5e58c2db0b52
campaign_id: 4267841
ame_calculated_campaign_name: null
message_id: d29efcc5f59e45f3b1ab14a23144945c
rows: 8
 */


SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step13__model_non_ame_campaigns icrs13mnac
WHERE icrs13mnac.message_id_email_hash = '02403037c4b7713579ee91ccfdcc24738be3aad4cadb687484be5e58c2db0b52'

SELECT
	icr.campaign_name,
	icr.campaign_id,
	SUM(icr.email_sends)
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting icr
WHERE icr.send_start_date >= CURRENT_DATE - 7
GROUP BY 1, 2
ORDER BY 3 DESC
;


SELECT
	icr.campaign_id,
	icr.campaign_name,
	SUM(icr.email_sends) AS sends
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting icr
WHERE icr.send_start_date >= CURRENT_DATE - 7
  AND icr.campaign_id = 8848776
GROUP BY 1, 2
;


SELECT
	es.campaign_id,
	COUNT(*) AS sends
FROM latest_vault.iterable.email_send es
WHERE es.event_created_at >= CURRENT_DATE - 7
  AND es.campaign_id = 8848776
GROUP BY 1
;

-- step 02
SELECT
	COUNT(*)
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step02__model_sends s
WHERE s.campaign_id = 8848776
;

-- step 06
SELECT
	SUM(s.email_sends)
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step06__aggregate_sends s
WHERE s.campaign_id = 8848776
;

-- step 10
SELECT
	SUM(s.email_sends)
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step10__model_iterable_data s
WHERE s.campaign_id = 8848776
;

SELECT
	SUM(s.email_sends)
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step13__model_non_ame_campaigns s
WHERE s.campaign_id = 8848776
;

SELECT
	SUM(s.email_sends)
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting s
WHERE s.campaign_id = 8848776
;

-- dupes appear to be introduced in step 13 where we add scv data, pulling the code from the
-- module to understand how dupes are happening

-- without joins no dupes
-- introduced lnd bookings no dupes
-- introduced lc bookings no dupes
-- lnd spvs introduces dupes


WITH
	scv_bookings AS (
		SELECT
			bk.attribution_model,
			bk.campaign_id,
			bk.message_id,
			bk.shiro_user_id,
			SUM(bookings)   AS bookings,
			SUM(margin_gbp) AS margin_gbp
		FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step11__model_scv_booking_data bk
		WHERE bk.campaign_id = '8848776'
		GROUP BY 1, 2, 3, 4
	),
	scv_spvs AS (
		SELECT
			spv.attribution_model,
			spv.campaign_id,
			spv.message_id,
			spv.shiro_user_id,
			SUM(spv.spvs) AS spvs
		FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step12__model_scv_spv_data spv
		WHERE spv.campaign_id = '8848776'
		GROUP BY 1, 2, 3, 4
	)
SELECT
	em.campaign_id,
	SUM(em.email_sends) AS email_sends
--                 COALESCE(em.email_opens,0) AS email_opens,
--                 COALESCE(em.unique_email_opens,0) AS unique_email_opens,
--                 COALESCE(em.email_clicks,0) AS email_clicks,
--                 COALESCE(em.unique_email_clicks,0) AS unique_email_clicks,
--                 em.first_open_event_date,
--                 em.first_open_event_time,
--                 em.first_click_event_date,
--                 em.first_click_event_time,
--                 em.unsub_event_date,
--                 em.unsub_event_time,
--                 COALESCE(em.email_unsubs,0) AS email_unsubs,
--                 COALESCE(em.email_unsubs_complaint,0) AS email_unsubs_complaint,
--                 COALESCE(em.email_unsubs_email_link,0) AS email_unsubs_email_link,
--                 COALESCE(bk_lc.bookings,0) bookings_lc,
--                 COALESCE(bk_lc.margin_gbp,0) margin_gbp_lc,
--                 COALESCE(bk_lnd.bookings,0) bookings_lnd,
--                 COALESCE(bk_lnd.margin_gbp,0) margin_gbp_lnd,
--                 COALESCE(spv_lc.spvs,0) AS spvs_lc,
--                 COALESCE(spv_lnd.spvs,0) AS spvs_lnd,
--                 COALESCE(url.spvs,0) AS spvs_url
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step10__model_iterable_data em
	LEFT JOIN scv_bookings bk_lnd
			  ON COALESCE(bk_lnd.message_id, em.message_id) = em.message_id
				  AND bk_lnd.campaign_id::VARCHAR = em.campaign_id::VARCHAR
				  AND bk_lnd.shiro_user_id = em.shiro_user_id
				  AND bk_lnd.attribution_model = 'last non direct'
				  AND em.is_automated_campaign = FALSE
	LEFT JOIN scv_bookings bk_lc
			  ON COALESCE(bk_lc.message_id, em.message_id) = em.message_id
				  AND bk_lc.campaign_id::VARCHAR = em.campaign_id::VARCHAR
				  AND bk_lc.shiro_user_id = em.shiro_user_id
				  AND bk_lc.attribution_model = 'last click'
				  AND em.is_automated_campaign = FALSE
	LEFT JOIN scv_spvs spv_lnd
			  ON COALESCE(spv_lnd.message_id, em.message_id) = em.message_id
				  AND spv_lnd.campaign_id::VARCHAR = em.campaign_id::VARCHAR
				  AND spv_lnd.shiro_user_id = em.shiro_user_id
				  AND spv_lnd.attribution_model = 'last non direct'
				  AND em.is_automated_campaign = FALSE
--             LEFT JOIN scv_spvs spv_lc
--                 ON COALESCE(spv_lc.message_id, em.message_id) = em.message_id
--                 AND spv_lc.campaign_id::VARCHAR = em.campaign_id::VARCHAR
--                 AND spv_lc.shiro_user_id = em.shiro_user_id
--                 AND spv_lc.attribution_model = 'last click'
--                 AND em.is_automated_campaign = FALSE
--             LEFT JOIN scv_spvs url
--                 ON COALESCE(url.message_id, em.message_id) = em.message_id
--                 AND url.campaign_id::VARCHAR = em.campaign_id::VARCHAR
--                 AND url.shiro_user_id = em.shiro_user_id
--                 AND url.attribution_model = 'url params'
--                 AND em.is_automated_campaign = FALSE
WHERE em.campaign_id = 8848776
GROUP BY 1
;



SELECT
	spv.attribution_model,
	spv.campaign_id,
	spv.message_id,
	spv.shiro_user_id,
	SUM(spv.spvs) AS spvs
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step12__model_scv_spv_data spv
WHERE spv.campaign_id = '8848776' AND spv.attribution_model = 'last non direct'
GROUP BY 1, 2, 3, 4
;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step13__model_non_ame_campaigns s
WHERE s.campaign_id = 8848776
QUALIFY COUNT(*) OVER (PARTITION BY s.message_id_email_hash) > 1
;

WITH
	email_metrics AS (
		SELECT *
		FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step10__model_iterable_data em
		WHERE em.campaign_id = 8848776
	),
	scv_spvs AS (
		SELECT
			spv.attribution_model,
			spv.campaign_id,
			spv.message_id,
			spv.shiro_user_id,
			SUM(spv.spvs) AS spvs
		FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step12__model_scv_spv_data spv
		WHERE spv.campaign_id = '8848776'
		GROUP BY 1, 2, 3, 4
	)
		,
	modelling AS (
		SELECT
			em.*,
			spv_lnd.*,
-- 			COALESCE(spv_lc.spvs,0) AS spvs_lc,
			COALESCE(spv_lnd.spvs, 0) AS spvs_lnd
-- 			COALESCE(url.spvs,0) AS spvs_url
		FROM email_metrics em
			LEFT JOIN scv_spvs spv_lnd
					  ON COALESCE(spv_lnd.message_id, em.message_id) = em.message_id
						  AND spv_lnd.campaign_id::VARCHAR = em.campaign_id::VARCHAR
						  AND spv_lnd.shiro_user_id = em.shiro_user_id
						  AND spv_lnd.attribution_model = 'last non direct'
						  AND em.is_automated_campaign = FALSE
-- 			LEFT JOIN scv_spvs spv_lc
-- 					  ON COALESCE(spv_lc.message_id, em.message_id) = em.message_id
-- 						  AND spv_lc.campaign_id::VARCHAR = em.campaign_id::VARCHAR
-- 						  AND spv_lc.shiro_user_id = em.shiro_user_id
-- 						  AND spv_lc.attribution_model = 'last click'
-- 						  AND em.is_automated_campaign = FALSE
-- 			LEFT JOIN scv_spvs url
-- 					  ON COALESCE(url.message_id, em.message_id) = em.message_id
-- 						  AND url.campaign_id::VARCHAR = em.campaign_id::VARCHAR
-- 						  AND url.shiro_user_id = em.shiro_user_id
-- 						  AND url.attribution_model = 'url params'
-- 						  AND em.is_automated_campaign = FALSE
	)
-- SELECT *
-- FROM modelling
-- QUALIFY COUNT(*) OVER (PARTITION BY modelling.message_id_email_hash) > 1

--
-- SELECT
-- 	SUM(m.email_sends),
-- 	SUM(m.spvs_lc),
-- 	SUM(m.spvs_lnd),
-- 	SUM(m.spvs_url)
-- FROM modelling m


--1915746 looks like the real number

-- 1915751 -- with just lnd
-- 1915759 -- with all spv joins

-- found duplication where a user has spvs with a message id and spvs without (but always with campaign id)

SELECT
	spv.attribution_model,
	spv.campaign_id,
	spv.message_id,
	spv.shiro_user_id,
	SUM(spv.spvs) AS spvs
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step12__model_scv_spv_data spv
WHERE spv.campaign_id = '8848776'
  AND spv.shiro_user_id = '46474072'
GROUP BY 1, 2, 3, 4
;

-- ^^ example of this duplication ^^

WITH
	model_data AS (
		SELECT
			spv.attribution_model,
			spv.campaign_id,
			MAX(spv.message_id) OVER (PARTITION BY spv.campaign_id, spv.shiro_user_id) AS message_id,
			spv.shiro_user_id,
			spv.spvs
		FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step12__model_scv_spv_data spv
		WHERE spv.campaign_id = '8848776'
		  AND spv.shiro_user_id = '46474072'
	)
SELECT
	spv.attribution_model,
	spv.campaign_id,
	spv.message_id,
	spv.shiro_user_id,
	SUM(spv.spvs) AS spvs
FROM model_data spv
GROUP BY 1, 2, 3, 4
;

-- persisting the message id when a user has both a message id and not a message id on spvs reduces dupes to 11

-- investigating the 11

SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting
QUALIFY COUNT(*) OVER (PARTITION BY message_id_email_hash) > 1
;


SELECT
	spv.campaign_id,
	spv.message_id,
	spv.shiro_user_id,
	SUM(spv.spvs) AS spvs
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting spv
WHERE spv.campaign_id = '8499715'
  AND spv.shiro_user_id = '12794038'
GROUP BY 1, 2, 3, 4
;

------------------------------------------------------------------------------------------------------------------------
-- investigating high proportion of margin being attributed to browse

SELECT
	icr.mapped_objective,
	SUM(icr.margin_gbp_lnd)
FROM data_vault_mvp.dwh.iterable_crm_reporting icr
GROUP BY 1
;


SELECT
	icr.campaign_id,
	icr.campaign_name,
	SUM(icr.margin_gbp_lnd),
	SUM(icr.margin_gbp_lc)
FROM data_vault_mvp.dwh.iterable_crm_reporting icr
WHERE icr.mapped_objective = 'Browse'
GROUP BY 1, 2
;

-- CAMPAIGN_ID	CAMPAIGN_NAME				SUM(ICR.MARGIN_GBP_LND)	SUM(ICR.MARGIN_GBP_LC)
-- 3306803		AME_Abandon_Browse_Daily	427618594				305518269

SELECT *
FROM data_vault_mvp.dwh.iterable_crm_reporting__step11__model_scv_booking_data bd
WHERE bd.campaign_id = '3306803'
;


SELECT
	bd.attribution_model,
	SUM(bd.margin_gbp)
FROM data_vault_mvp.dwh.iterable_crm_reporting__step11__model_scv_booking_data bd
WHERE bd.campaign_id = '3306803'
GROUP BY 1
;

--	ATTRIBUTION_MODEL	SUM(BD.MARGIN_GBP)
--	last non direct		5,897,987
--	last click			3,906,301


SELECT
	SUM(bd.margin_gbp_lnd)
FROM data_vault_mvp.dwh.iterable_crm_reporting__step13__model_non_ame_campaigns bd
WHERE bd.campaign_id = '3306803'
;


-- 427,590,787


WITH
	scv_bookings AS (
		SELECT
			bk.attribution_model,
			bk.campaign_id,
			bk.message_id,
			bk.shiro_user_id,
			SUM(bookings)   AS bookings,
			SUM(margin_gbp) AS margin_gbp
		FROM data_vault_mvp.dwh.iterable_crm_reporting__step11__model_scv_booking_data bk
		GROUP BY 1, 2, 3, 4
	),
	scv_spvs AS (
		SELECT
			spv.attribution_model,
			spv.campaign_id,
			spv.message_id,
			spv.shiro_user_id,
			SUM(spv.spvs) AS spvs
		FROM data_vault_mvp.dwh.iterable_crm_reporting__step12__model_scv_spv_data spv
		GROUP BY 1, 2, 3, 4
	)
SELECT
	em.message_id_email_hash,
	em.message_id,
	em.campaign_id,
	em.splittable_email_name,
	em.mapped_crm_date,
	em.mapped_territory,
	em.current_affiliate_territory,
	em.mapped_objective,
	em.mapped_platform,
	em.mapped_campaign,
	em.mapped_theme,
	em.mapped_segment,
	em.is_athena,
	em.is_automated_campaign,
	em.ame_calculated_campaign_name,
	em.email_hash,
	em.shiro_user_id,
	em.campaign_name,
	em.send_event_date,
	em.send_event_time,
	em.send_start_date,
	em.send_end_date,
	COALESCE(em.email_sends, 0)             AS email_sends,
	COALESCE(em.email_opens, 0)             AS email_opens,
	COALESCE(em.unique_email_opens, 0)      AS unique_email_opens,
	COALESCE(em.email_clicks, 0)            AS email_clicks,
	COALESCE(em.unique_email_clicks, 0)     AS unique_email_clicks,
	em.first_open_event_date,
	em.first_open_event_time,
	em.first_click_event_date,
	em.first_click_event_time,
	em.unsub_event_date,
	em.unsub_event_time,
	COALESCE(em.email_unsubs, 0)            AS email_unsubs,
	COALESCE(em.email_unsubs_complaint, 0)  AS email_unsubs_complaint,
	COALESCE(em.email_unsubs_email_link, 0) AS email_unsubs_email_link,
	COALESCE(bk_lc.bookings, 0)             AS bookings_lc,
	COALESCE(bk_lc.margin_gbp, 0)           AS margin_gbp_lc,
	COALESCE(bk_lnd.bookings, 0)            AS bookings_lnd,
	COALESCE(bk_lnd.margin_gbp, 0)          AS margin_gbp_lnd,
	COALESCE(spv_lc.spvs, 0)                AS spvs_lc,
	COALESCE(spv_lnd.spvs, 0)               AS spvs_lnd,
	COALESCE(url.spvs, 0)                   AS spvs_url
FROM data_vault_mvp.dwh.iterable_crm_reporting__step10__model_iterable_data em
	LEFT JOIN scv_bookings bk_lnd
			  ON COALESCE(bk_lnd.message_id, em.message_id) = em.message_id
				  AND bk_lnd.campaign_id::VARCHAR = em.campaign_id::VARCHAR
				  AND bk_lnd.shiro_user_id = em.shiro_user_id
				  AND bk_lnd.attribution_model = 'last non direct'
				  AND em.is_automated_campaign = FALSE
	LEFT JOIN scv_bookings bk_lc
			  ON COALESCE(bk_lc.message_id, em.message_id) = em.message_id
				  AND bk_lc.campaign_id::VARCHAR = em.campaign_id::VARCHAR
				  AND bk_lc.shiro_user_id = em.shiro_user_id
				  AND bk_lc.attribution_model = 'last click'
				  AND em.is_automated_campaign = FALSE
	LEFT JOIN scv_spvs spv_lnd
			  ON COALESCE(spv_lnd.message_id, em.message_id) = em.message_id
				  AND spv_lnd.campaign_id::VARCHAR = em.campaign_id::VARCHAR
				  AND spv_lnd.shiro_user_id = em.shiro_user_id
				  AND spv_lnd.attribution_model = 'last non direct'
				  AND em.is_automated_campaign = FALSE
	LEFT JOIN scv_spvs spv_lc
			  ON COALESCE(spv_lc.message_id, em.message_id) = em.message_id
				  AND spv_lc.campaign_id::VARCHAR = em.campaign_id::VARCHAR
				  AND spv_lc.shiro_user_id = em.shiro_user_id
				  AND spv_lc.attribution_model = 'last click'
				  AND em.is_automated_campaign = FALSE
	LEFT JOIN scv_spvs url
			  ON COALESCE(url.message_id, em.message_id) = em.message_id
				  AND url.campaign_id::VARCHAR = em.campaign_id::VARCHAR
				  AND url.shiro_user_id = em.shiro_user_id
				  AND url.attribution_model = 'url params'
				  AND em.is_automated_campaign = FALSE
;


SELECT *
FROM latest_vault_dev_robin.iterable.campaign c
;

-- checking after correcting ame flag

SELECT
	icr.mapped_objective,
	SUM(icr.margin_gbp_lnd)
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting icr
GROUP BY 1
;

SELECT
	icr.campaign_id,
	icr.campaign_name,
	SUM(icr.margin_gbp_lnd),
	SUM(icr.margin_gbp_lc)
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting icr
WHERE icr.mapped_objective = 'Browse'
GROUP BY 1, 2
;

-- CAMPAIGN_ID	CAMPAIGN_NAME				SUM(ICR.MARGIN_GBP_LND)		SUM(ICR.MARGIN_GBP_LC)
-- 3306803		AME_Abandon_Browse_Daily	5668301						3786760


SELECT
	bd.attribution_model,
	SUM(bd.margin_gbp)
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step11__model_scv_booking_data bd
WHERE bd.campaign_id = '3306803'
GROUP BY 1
;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting icr
WHERE icr.campaign_id = 7505053
;


SELECT *
FROM latest_vault.iterable.campaign c
WHERE c.name = '20231012_DE_CORE_NewImprovedPKGs_X_X_X_Targeted'
;

SELECT
	icr.send_event_date,
	COUNT(*)
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step06__send_grain icr
WHERE icr.campaign_id = 7505053
GROUP BY 1
;


SELECT
	type,
	COUNT(*)
FROM latest_vault.iterable.campaign c
GROUP BY 1


SELECT
	DATE_TRUNC(WEEK, ec.event_created_at::DATE) AS date,
	COUNT(*)                                    AS clicks,
	COUNT(DISTINCT ec.message_id)               AS unique_clicks,
	COUNT(DISTINCT ec.email)                    AS unique_clicks
FROM latest_vault.iterable.email_unsubscribe ec
WHERE ec.event_created_at >= '2024-01-29'
  AND ec.unsub_source IN ('Complaint', 'EmailLink')
GROUP BY 1
;


SELECT
	ec.campaign_id IS NULL,
	ec.message_id IS NULL,
	COUNT(*)
FROM latest_vault.iterable.email_unsubscribe ec
WHERE ec.event_created_at >= '2024-01-29'
  AND ec.unsub_source IN ('Complaint', 'EmailLink')
GROUP BY 1, 2
;


SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step05__model_unsubs icrs05mu
;


SELECT
	DATE_TRUNC(WEEK, icrs05mu.unsub_event_date),
	COUNT(*)
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step05__model_unsubs icrs05mu
WHERE icrs05mu.unsub_event_date >= '2024-01-29'
GROUP BY 1
;


SELECT
	DATE_TRUNC(WEEK, icrs05mu.unsub_event_date),
	SUM(icrs05mu.email_unsubs)
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step09__aggregate_unsubs icrs05mu
WHERE icrs05mu.unsub_event_date >= '2024-01-29'
GROUP BY 1
;



SELECT
	DATE_TRUNC(WEEK, icrs05mu.unsub_event_date),
	SUM(icrs05mu.email_unsubs)
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step10__model_iterable_data icrs05mu
WHERE icrs05mu.unsub_event_date >= '2024-01-29'
GROUP BY 1
;


SELECT
	DATE_TRUNC(WEEK, icrs05mu.unsub_event_date),
	SUM(icrs05mu.email_unsubs)
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting icrs05mu
WHERE icrs05mu.unsub_event_date >= '2024-01-29'
GROUP BY 1
;


SELECT
	DATE_TRUNC(WEEK, send_event_date) AS week_starting,
	SUM(email_unsubs)
FROM collab.crm_reporting.iterable_crm_reporting
-- FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting icrs05mu
WHERE send_event_date BETWEEN ('2022-02-07') AND CURRENT_DATE()
GROUP BY 1
;

SELECT
	DATE_TRUNC(WEEK, unsub_event_date) AS week_starting,
	SUM(email_unsubs)
FROM collab.crm_reporting.iterable_crm_reporting
-- FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting icrs05mu
WHERE send_event_date BETWEEN ('2022-02-07') AND CURRENT_DATE()
GROUP BY 1
;


CREATE OR REPLACE VIEW collab.crm_reporting.iterable_crm_reporting COPY GRANTS AS
SELECT *
FROM data_vault_mvp.dwh.iterable_crm_reporting
;

SELECT
	MAX(send_event_date)
FROM collab.crm_reporting.iterable_crm_reporting
;


------------------------------------------------------------------------------------------------------------------------
-- looking for some missing campaigns
-- https://docs.google.com/spreadsheets/d/15iuMQzTkO8dhE9MS27F3LwaA2jfVcWn-SLB2IiQ0pUg/edit#gid=1045931692

-- campaign id: 3677373 is in new dataset but not in email performance
-- it has 296181 sends so these are all missing from email performance

SELECT *
FROM se.data.email_performance ep
WHERE ep.campaign_id = 3677373
;

--CODE
SELECT
	campaign_id,
	SUM(email_sends)
FROM collab.crm_reporting.iterable_crm_reporting
WHERE send_event_date BETWEEN ('2024-02-26') AND ('2024-03-03')
  AND campaign_id = 3677373
GROUP BY 1
;


SELECT *
FROM se.data.crm_jobs_list cjl
WHERE cjl.campaign_id = 3677373
;


SELECT *
FROM se.data.email_performance ep
WHERE ep.campaign_id = 3306803
;

-- doesn't exist

--CODE
SELECT
	campaign_id,
	SUM(email_sends)
FROM collab.crm_reporting.iterable_crm_reporting
WHERE send_event_date BETWEEN ('2024-02-26') AND ('2024-03-03')
  AND campaign_id = 3306803
GROUP BY 1
;


SELECT *
FROM se.data.crm_jobs_list cjl
WHERE cjl.campaign_id = 3306803
;
-- is in the job list --  yes

SELECT *
FROM data_vault_mvp.dwh.email_list el
WHERE el.campaign_id = 3306803
;


SELECT *
FROM latest_vault.iterable.email_send_log esl
WHERE esl.campaign_id = 3306803
;

SELECT *
FROM data_vault_mvp.dwh.email_list el
WHERE el.campaign_id = 3306803
;


SELECT *
FROM latest_vault.iterable.email_send_log esl
WHERE esl.campaign_id = 8914289
;

SELECT *
FROM latest_vault.iterable.email_send_log esl
WHERE esl.campaign_id = 3545719
;

SELECT *
FROM se.data.email_performance ep
WHERE ep.campaign_id = 6596494
;


;



SELECT
	eo.message_id_email_hash,
	eo.campaign_id,
	eo.message_id,
	MIN(eo.open_event_date)                  AS first_open_event_date,
	MIN(eo.open_event_time)                  AS first_open_event_time,
	COUNT(*)                                 AS email_opens,
	COUNT(DISTINCT eo.message_id_email_hash) AS unique_email_opens
FROM {target_table_ref}__step03__model_opens eo
GROUP BY 1, 2, 3



--Brandon Hotel Only
--Sale IDs IN('A64544','A64745')
--SPVs

-- opens
-- as it was 2.4 creds
-- all metrics 8.1 creds
-- without unique emails 3.5 creds

-- clicks
-- as it was 0.3 creds
-- all metrics creds


SELECT GET_DDL('table', 'collab.crm_reporting.iterable_crm_reporting')
;


SELECT *
FROM collab.crm_reporting.iterable_crm_reporting
;

USE ROLE personal_role__robinpatel
;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.fact_booking AS
SELECT *
FROM data_vault_mvp.dwh.fact_booking fb
;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step11__model_scv_booking_data