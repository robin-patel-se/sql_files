USE WAREHOUSE pipe_xlarge
;

SELECT
	em.message_id_email_hash,
	em.message_id,
	em.campaign_id,
	em.crm_channel_type,
	COALESCE(em.splittable_email_name, em.campaign_name)                   AS combined_email_name,
	IFF(is_automated_campaign, 'Not trading', 'Trading')                   AS trading,
	CASE
		WHEN LOWER(SPLIT_PART(em.campaign_name, '_', 1)::VARCHAR) = 'amne' THEN 'Newsletter'
		WHEN LOWER(SPLIT_PART(em.campaign_name, '_', 1)::VARCHAR) = 'amte' THEN 'Trigger'
		WHEN LOWER(SPLIT_PART(em.campaign_name, '_', 1)::VARCHAR) = 'amle' THEN 'Lifecycle'
		ELSE 'Newsletter'
	END                                                                    AS email_type,
	em.splittable_email_name,
	em.mapped_crm_date,
	em.mapped_territory,
	em.current_affiliate_territory,
	COALESCE(em.mapped_objective, SPLIT_PART(combined_email_name, '_', 3)) AS mapped_objective,
	COALESCE(em.mapped_platform, SPLIT_PART(combined_email_name, '_', 4))  AS mapped_platform,
	COALESCE(em.mapped_campaign, SPLIT_PART(combined_email_name, '_', 5))  AS mapped_campaign,
	COALESCE(em.mapped_theme, SPLIT_PART(combined_email_name, '_', 6))     AS mapped_theme,
	COALESCE(em.mapped_segment, SPLIT_PART(combined_email_name, '_', 8))   AS mapped_segment,
	em.is_athena,
	em.is_automated_campaign,
	em.ame_calculated_campaign_name,
	em.email_hash,
	em.shiro_user_id,
	em.campaign_name,
	rfv.rfv_segment,
	rfv.lifecycle                                                          AS rfv_lifecycle,
	CASE
		WHEN rfv.rfv_segment IS NULL AND
			 rfv.signup_date = DATE_TRUNC('week', em.send_event_date)
			THEN 'New Signup in Week'
		ELSE rfv.rfv_segment
	END                                                                    AS rfv_segment_calc,
	CASE
		WHEN rfv.lifecycle IS NULL AND
			 rfv.signup_date = DATE_TRUNC('week', em.send_event_date)
			THEN 'Early Life Active'
		ELSE rfv.lifecycle
	END                                                                    AS rfv_lifecycle_calc,
	em.send_event_date,
	em.send_event_time,
	em.send_start_date,
	em.send_end_date,
	em.email_sends,
	em.email_opens,
	em.email_opens_1d,
	em.email_opens_7d,
	em.email_opens_14d,
	em.unique_email_opens,
	em.unique_email_opens_1d,
	em.unique_email_opens_7d,
	em.unique_email_opens_14d,
	em.email_clicks,
	em.email_clicks_1d,
	em.email_clicks_7d,
	em.email_clicks_14d,
	em.unique_email_clicks,
	em.unique_email_clicks_1d,
	em.unique_email_clicks_7d,
	em.unique_email_clicks_14d,
	em.first_open_event_date,
	em.first_open_event_time,
	em.first_click_event_date,
	em.first_click_event_time,
	em.unsub_event_date,
	em.unsub_event_time,
	em.email_unsubs,
	em.email_unsubs_1d,
	em.email_unsubs_7d,
	em.email_unsubs_14d,
	em.email_unsubs_complaint,
	em.email_unsubs_complaint_1d,
	em.email_unsubs_complaint_7d,
	em.email_unsubs_complaint_14d,
	em.email_unsubs_email_link,
	em.email_unsubs_email_link_1d,
	em.email_unsubs_email_link_7d,
	em.email_unsubs_email_link_14d,
	-- adding here is to accommodate for zero'd out metrics for
	-- automated campaigns in earlier step.
	em.bookings_lc + COALESCE(ame.bookings_lc, 0)                          AS bookings_lc,
	em.bookings_1d_lc + COALESCE(ame.bookings_1d_lc, 0)                    AS bookings_1d_lc,
	em.bookings_7d_lc + COALESCE(ame.bookings_7d_lc, 0)                    AS bookings_7d_lc,
	em.bookings_14d_lc + COALESCE(ame.bookings_14d_lc, 0)                  AS bookings_14d_lc,

	em.margin_gbp_lc + COALESCE(ame.margin_gbp_lc, 0)                      AS margin_gbp_lc,
	em.margin_gbp_1d_lc + COALESCE(ame.margin_gbp_1d_lc, 0)                AS margin_gbp_1d_lc,
	em.margin_gbp_7d_lc + COALESCE(ame.margin_gbp_7d_lc, 0)                AS margin_gbp_7d_lc,
	em.margin_gbp_14d_lc + COALESCE(ame.margin_gbp_14d_lc, 0)              AS margin_gbp_14d_lc,

	em.bookings_lnd + COALESCE(ame.bookings_lnd, 0)                        AS bookings_lnd,
	em.bookings_lnd + COALESCE(ame.bookings_1d_lnd, 0)                     AS bookings_1d_lnd,
	em.bookings_lnd + COALESCE(ame.bookings_7d_lnd, 0)                     AS bookings_7d_lnd,
	em.bookings_lnd + COALESCE(ame.bookings_14d_lnd, 0)                    AS bookings_14d_lnd,

	em.margin_gbp_lnd + COALESCE(ame.margin_gbp_lnd, 0)                    AS margin_gbp_lnd,
	em.margin_gbp_lnd + COALESCE(ame.margin_gbp_1d_lnd, 0)                 AS margin_gbp_1d_lnd,
	em.margin_gbp_lnd + COALESCE(ame.margin_gbp_7d_lnd, 0)                 AS margin_gbp_7d_lnd,
	em.margin_gbp_lnd + COALESCE(ame.margin_gbp_14d_lnd, 0)                AS margin_gbp_14d_lnd,

	em.spvs_lc + COALESCE(ame.spvs_lc, 0)                                  AS spvs_lc,
	em.spvs_lc + COALESCE(ame.spvs_1d_lc, 0)                               AS spvs_1d_lc,
	em.spvs_lc + COALESCE(ame.spvs_7d_lc, 0)                               AS spvs_7d_lc,
	em.spvs_lc + COALESCE(ame.spvs_14d_lc, 0)                              AS spvs_14d_lc,

	em.spvs_lnd + COALESCE(ame.spvs_lnd, 0)                                AS spvs_lnd,
	em.spvs_lnd + COALESCE(ame.spvs_1d_lnd, 0)                             AS spvs_1d_lnd,
	em.spvs_lnd + COALESCE(ame.spvs_7d_lnd, 0)                             AS spvs_7d_lnd,
	em.spvs_lnd + COALESCE(ame.spvs_14d_lnd, 0)                            AS spvs_14d_lnd,

	em.spvs_url + COALESCE(ame.spvs_url, 0)                                AS spvs_url,
	em.spvs_url + COALESCE(ame.spvs_1d_url, 0)                             AS spvs_1d_url,
	em.spvs_url + COALESCE(ame.spvs_7d_url, 0)                             AS spvs_7d_url,
	em.spvs_url + COALESCE(ame.spvs_14d_url, 0)                            AS spvs_14d_url

FROM data_vault_mvp.dwh.iterable_crm_reporting__step13__model_non_ame_campaigns em
	LEFT JOIN data_vault_mvp.dwh.iterable_crm_reporting__step14__model_ame_campaigns ame
			  ON em.message_id_email_hash = ame.message_id_email_hash
	LEFT JOIN data_vault_mvp.dwh.iterable_crm_reporting__step15__model_rfv rfv ON rfv.shiro_user_id = em.shiro_user_id
	AND rfv.event_date = DATE_TRUNC('week', em.send_event_date)
WHERE em.message_id = '58aeb53cd07d4ef5b896af174fad16a9'
;


-- message_id_email_hash 9d18dac4fa30f548d39b42fb3bf68dcdb01f2f1345c0e03f3a31841d937dc8ab

SELECT *
FROM data_vault_mvp.dwh.iterable_crm_reporting__step14__model_ame_campaigns ame
WHERE ame.message_id_email_hash = '9d18dac4fa30f548d39b42fb3bf68dcdb01f2f1345c0e03f3a31841d937dc8ab'


SELECT *
FROM data_vault_mvp.dwh.iterable_crm_reporting__step13__model_non_ame_campaigns em
WHERE em.message_id_email_hash = '9d18dac4fa30f548d39b42fb3bf68dcdb01f2f1345c0e03f3a31841d937dc8ab'

-- campaign id: 11224492

SELECT *
FROM data_vault_mvp.dwh.iterable_crm_reporting__step13__model_non_ame_campaigns em
WHERE em.message_id_email_hash = '9d18dac4fa30f548d39b42fb3bf68dcdb01f2f1345c0e03f3a31841d937dc8ab'


SELECT *
FROM data_vault_mvp.dwh.iterable_crm_reporting__step11__model_scv_booking_data icrs11msbd
WHERE message_id = '58aeb53cd07d4ef5b896af174fad16a9'

-- scv booking data occured on the 10th novemeber 2024


SELECT
	em.message_id_email_hash,
	SUM(IFF(bk.attribution_model = 'last click', bk.bookings, 0))            AS bookings_lc,
	SUM(IFF(bk.attribution_model = 'last click', bk.bookings_1d, 0))         AS bookings_1d_lc,
	SUM(IFF(bk.attribution_model = 'last click', bk.bookings_7d, 0))         AS bookings_7d_lc,
	SUM(IFF(bk.attribution_model = 'last click', bk.bookings_14d, 0))        AS bookings_14d_lc,

	SUM(IFF(bk.attribution_model = 'last click', bk.margin_gbp, 0))          AS margin_gbp_lc,
	SUM(IFF(bk.attribution_model = 'last click', bk.margin_gbp_1d, 0))       AS margin_gbp_1d_lc,
	SUM(IFF(bk.attribution_model = 'last click', bk.margin_gbp_7d, 0))       AS margin_gbp_7d_lc,
	SUM(IFF(bk.attribution_model = 'last click', bk.margin_gbp_14d, 0))      AS margin_gbp_14d_lc,

	SUM(IFF(bk.attribution_model = 'last non direct', bk.bookings, 0))       AS bookings_lnd,
	SUM(IFF(bk.attribution_model = 'last non direct', bk.bookings_1d, 0))    AS bookings_1d_lnd,
	SUM(IFF(bk.attribution_model = 'last non direct', bk.bookings_7d, 0))    AS bookings_7d_lnd,
	SUM(IFF(bk.attribution_model = 'last non direct', bk.bookings_14d, 0))   AS bookings_14d_lnd,

	SUM(IFF(bk.attribution_model = 'last non direct', bk.margin_gbp, 0))     AS margin_gbp_lnd,
	SUM(IFF(bk.attribution_model = 'last non direct', bk.margin_gbp_1d, 0))  AS margin_gbp_1d_lnd,
	SUM(IFF(bk.attribution_model = 'last non direct', bk.margin_gbp_7d, 0))  AS margin_gbp_7d_lnd,
	SUM(IFF(bk.attribution_model = 'last non direct', bk.margin_gbp_14d, 0)) AS margin_gbp_14d_lnd

FROM data_vault_mvp.dwh.iterable_crm_reporting__step11__model_scv_booking_data bk
	LEFT JOIN data_vault_mvp.dwh.iterable_crm_reporting__step13__model_non_ame_campaigns em
			  ON COALESCE(bk.message_id, em.message_id) = em.message_id
				  AND bk.campaign_id::VARCHAR = em.campaign_id::VARCHAR
				  AND bk.shiro_user_id = em.shiro_user_id
				  AND bk.event_date BETWEEN em.send_start_date AND em.send_end_date
WHERE em.is_automated_campaign = TRUE
  AND em.message_id_email_hash = '9d18dac4fa30f548d39b42fb3bf68dcdb01f2f1345c0e03f3a31841d937dc8ab'
GROUP BY 1
;



SELECT
	em.message_id_email_hash,
	SUM(IFF(spvs.attribution_model = 'last click', spvs.spvs, 0))          AS spvs_lc,
	SUM(IFF(spvs.attribution_model = 'last click', spvs.spvs_1d, 0))       AS spvs_1d_lc,
	SUM(IFF(spvs.attribution_model = 'last click', spvs.spvs_7d, 0))       AS spvs_7d_lc,
	SUM(IFF(spvs.attribution_model = 'last click', spvs.spvs_14d, 0))      AS spvs_14d_lc,

	SUM(IFF(spvs.attribution_model = 'last non direct', spvs.spvs, 0))     AS spvs_lnd,
	SUM(IFF(spvs.attribution_model = 'last non direct', spvs.spvs_1d, 0))  AS spvs_1d_lnd,
	SUM(IFF(spvs.attribution_model = 'last non direct', spvs.spvs_7d, 0))  AS spvs_7d_lnd,
	SUM(IFF(spvs.attribution_model = 'last non direct', spvs.spvs_14d, 0)) AS spvs_14d_lnd,

	SUM(IFF(spvs.attribution_model = 'url params', spvs.spvs, 0))          AS spvs_url,
	SUM(IFF(spvs.attribution_model = 'url params', spvs.spvs_1d, 0))       AS spvs_1d_url,
	SUM(IFF(spvs.attribution_model = 'url params', spvs.spvs_7d, 0))       AS spvs_7d_url,
	SUM(IFF(spvs.attribution_model = 'url params', spvs.spvs_14d, 0))      AS spvs_14d_url
FROM data_vault_mvp.dwh.iterable_crm_reporting__step12__model_scv_spv_data spvs
	LEFT JOIN data_vault_mvp.dwh.iterable_crm_reporting__step13__model_non_ame_campaigns em
			  ON COALESCE(spvs.message_id, em.message_id) = em.message_id
				  AND spvs.campaign_id::VARCHAR = em.campaign_id::VARCHAR
				  AND spvs.shiro_user_id = em.shiro_user_id
				  AND spvs.event_date BETWEEN em.send_start_date AND em.send_end_date
WHERE em.is_automated_campaign = TRUE
  AND em.message_id_email_hash = '9d18dac4fa30f548d39b42fb3bf68dcdb01f2f1345c0e03f3a31841d937dc8ab'
GROUP BY 1
;



WITH
	ame_bookings AS (
		SELECT
			em.message_id_email_hash,
			SUM(IFF(bk.attribution_model = 'last click', bk.bookings, 0))            AS bookings_lc,
			SUM(IFF(bk.attribution_model = 'last click', bk.bookings_1d, 0))         AS bookings_1d_lc,
			SUM(IFF(bk.attribution_model = 'last click', bk.bookings_7d, 0))         AS bookings_7d_lc,
			SUM(IFF(bk.attribution_model = 'last click', bk.bookings_14d, 0))        AS bookings_14d_lc,

			SUM(IFF(bk.attribution_model = 'last click', bk.margin_gbp, 0))          AS margin_gbp_lc,
			SUM(IFF(bk.attribution_model = 'last click', bk.margin_gbp_1d, 0))       AS margin_gbp_1d_lc,
			SUM(IFF(bk.attribution_model = 'last click', bk.margin_gbp_7d, 0))       AS margin_gbp_7d_lc,
			SUM(IFF(bk.attribution_model = 'last click', bk.margin_gbp_14d, 0))      AS margin_gbp_14d_lc,

			SUM(IFF(bk.attribution_model = 'last non direct', bk.bookings, 0))       AS bookings_lnd,
			SUM(IFF(bk.attribution_model = 'last non direct', bk.bookings_1d, 0))    AS bookings_1d_lnd,
			SUM(IFF(bk.attribution_model = 'last non direct', bk.bookings_7d, 0))    AS bookings_7d_lnd,
			SUM(IFF(bk.attribution_model = 'last non direct', bk.bookings_14d, 0))   AS bookings_14d_lnd,

			SUM(IFF(bk.attribution_model = 'last non direct', bk.margin_gbp, 0))     AS margin_gbp_lnd,
			SUM(IFF(bk.attribution_model = 'last non direct', bk.margin_gbp_1d, 0))  AS margin_gbp_1d_lnd,
			SUM(IFF(bk.attribution_model = 'last non direct', bk.margin_gbp_7d, 0))  AS margin_gbp_7d_lnd,
			SUM(IFF(bk.attribution_model = 'last non direct', bk.margin_gbp_14d, 0)) AS margin_gbp_14d_lnd

		FROM data_vault_mvp.dwh.iterable_crm_reporting__step11__model_scv_booking_data bk
			LEFT JOIN data_vault_mvp.dwh.iterable_crm_reporting__step13__model_non_ame_campaigns em
					  ON COALESCE(bk.message_id, em.message_id) = em.message_id
						  AND bk.campaign_id::VARCHAR = em.campaign_id::VARCHAR
						  AND bk.shiro_user_id = em.shiro_user_id
						  AND bk.event_date BETWEEN em.send_start_date AND em.send_end_date
		WHERE em.is_automated_campaign = TRUE
		  AND em.message_id_email_hash = '9d18dac4fa30f548d39b42fb3bf68dcdb01f2f1345c0e03f3a31841d937dc8ab'
		GROUP BY 1
	),
	ame_spvs AS (
		SELECT
			em.message_id_email_hash,
			SUM(IFF(spvs.attribution_model = 'last click', spvs.spvs, 0))          AS spvs_lc,
			SUM(IFF(spvs.attribution_model = 'last click', spvs.spvs_1d, 0))       AS spvs_1d_lc,
			SUM(IFF(spvs.attribution_model = 'last click', spvs.spvs_7d, 0))       AS spvs_7d_lc,
			SUM(IFF(spvs.attribution_model = 'last click', spvs.spvs_14d, 0))      AS spvs_14d_lc,

			SUM(IFF(spvs.attribution_model = 'last non direct', spvs.spvs, 0))     AS spvs_lnd,
			SUM(IFF(spvs.attribution_model = 'last non direct', spvs.spvs_1d, 0))  AS spvs_1d_lnd,
			SUM(IFF(spvs.attribution_model = 'last non direct', spvs.spvs_7d, 0))  AS spvs_7d_lnd,
			SUM(IFF(spvs.attribution_model = 'last non direct', spvs.spvs_14d, 0)) AS spvs_14d_lnd,

			SUM(IFF(spvs.attribution_model = 'url params', spvs.spvs, 0))          AS spvs_url,
			SUM(IFF(spvs.attribution_model = 'url params', spvs.spvs_1d, 0))       AS spvs_1d_url,
			SUM(IFF(spvs.attribution_model = 'url params', spvs.spvs_7d, 0))       AS spvs_7d_url,
			SUM(IFF(spvs.attribution_model = 'url params', spvs.spvs_14d, 0))      AS spvs_14d_url
		FROM data_vault_mvp.dwh.iterable_crm_reporting__step12__model_scv_spv_data spvs
			LEFT JOIN data_vault_mvp.dwh.iterable_crm_reporting__step13__model_non_ame_campaigns em
					  ON COALESCE(spvs.message_id, em.message_id) = em.message_id
						  AND spvs.campaign_id::VARCHAR = em.campaign_id::VARCHAR
						  AND spvs.shiro_user_id = em.shiro_user_id
						  AND spvs.event_date BETWEEN em.send_start_date AND em.send_end_date
		WHERE em.is_automated_campaign = TRUE
		  AND em.message_id_email_hash = '9d18dac4fa30f548d39b42fb3bf68dcdb01f2f1345c0e03f3a31841d937dc8ab'
		GROUP BY 1
	)
SELECT
	COALESCE(s.message_id_email_hash, b.message_id_email_hash) AS message_id_email_hash,

	b.bookings_lc,
	b.bookings_1d_lc,
	b.bookings_7d_lc,
	b.bookings_14d_lc,

	COALESCE(b.margin_gbp_lc, 0)                               AS margin_gbp_lc,
	COALESCE(b.margin_gbp_1d_lc, 0)                            AS margin_gbp_1d_lc,
	COALESCE(b.margin_gbp_7d_lc, 0)                            AS margin_gbp_7d_lc,
	COALESCE(b.margin_gbp_14d_lc, 0)                           AS margin_gbp_14d_lc,

	COALESCE(b.bookings_lnd, 0)                                AS bookings_lnd,
	COALESCE(b.bookings_1d_lnd, 0)                             AS bookings_1d_lnd,
	COALESCE(b.bookings_7d_lnd, 0)                             AS bookings_7d_lnd,
	COALESCE(b.bookings_14d_lnd, 0)                            AS bookings_14d_lnd,

	COALESCE(b.margin_gbp_lnd, 0)                              AS margin_gbp_lnd,
	COALESCE(b.margin_gbp_1d_lnd, 0)                           AS margin_gbp_1d_lnd,
	COALESCE(b.margin_gbp_7d_lnd, 0)                           AS margin_gbp_7d_lnd,
	COALESCE(b.margin_gbp_14d_lnd, 0)                          AS margin_gbp_14d_lnd,

	COALESCE(s.spvs_lc, 0)                                     AS spvs_lc,
	COALESCE(s.spvs_1d_lc, 0)                                  AS spvs_1d_lc,
	COALESCE(s.spvs_7d_lc, 0)                                  AS spvs_7d_lc,
	COALESCE(s.spvs_14d_lc, 0)                                 AS spvs_14d_lc,

	COALESCE(s.spvs_lnd, 0)                                    AS spvs_lnd,
	COALESCE(s.spvs_1d_lnd, 0)                                 AS spvs_1d_lnd,
	COALESCE(s.spvs_7d_lnd, 0)                                 AS spvs_7d_lnd,
	COALESCE(s.spvs_14d_lnd, 0)                                AS spvs_14d_lnd,

	COALESCE(s.spvs_url, 0)                                    AS spvs_url,
	COALESCE(s.spvs_1d_url, 0)                                 AS spvs_1d_url,
	COALESCE(s.spvs_7d_url, 0)                                 AS spvs_7d_url,
	COALESCE(s.spvs_14d_url, 0)                                AS spvs_14d_url

FROM ame_spvs s
	FULL OUTER JOIN ame_bookings b ON s.message_id_email_hash = b.message_id_email_hash


------------------------------------------------------------------------------------------------------------------------


SELECT *
FROM latest_vault.iterable.email_send
WHERE event_created_at::DATE > CURRENT_DATE - 90
  AND message_id = 'c77926b6cd0b4ae38b4a3847e21ffe5d'
;

------------------------------------------------------------------------------------------------------------------------
USE WAREHOUSE pipe_xlarge
;

SELECT *
FROM data_vault_mvp.dwh.iterable_crm_reporting icr
WHERE icr.message_id_email_hash = '9d18dac4fa30f548d39b42fb3bf68dcdb01f2f1345c0e03f3a31841d937dc8ab'
  AND icr.send_start_date >= '2024-11-09'
;

--message id 58aeb53cd07d4ef5b896af174fad16a9


SELECT
	em.message_id_email_hash,
	SUM(IFF(spvs.attribution_model = 'last click', spvs.spvs, 0))          AS spvs_lc,
	SUM(IFF(spvs.attribution_model = 'last click', spvs.spvs_1d, 0))       AS spvs_1d_lc,
	SUM(IFF(spvs.attribution_model = 'last click', spvs.spvs_7d, 0))       AS spvs_7d_lc,
	SUM(IFF(spvs.attribution_model = 'last click', spvs.spvs_14d, 0))      AS spvs_14d_lc,

	SUM(IFF(spvs.attribution_model = 'last non direct', spvs.spvs, 0))     AS spvs_lnd,
	SUM(IFF(spvs.attribution_model = 'last non direct', spvs.spvs_1d, 0))  AS spvs_1d_lnd,
	SUM(IFF(spvs.attribution_model = 'last non direct', spvs.spvs_7d, 0))  AS spvs_7d_lnd,
	SUM(IFF(spvs.attribution_model = 'last non direct', spvs.spvs_14d, 0)) AS spvs_14d_lnd,

	SUM(IFF(spvs.attribution_model = 'url params', spvs.spvs, 0))          AS spvs_url,
	SUM(IFF(spvs.attribution_model = 'url params', spvs.spvs_1d, 0))       AS spvs_1d_url,
	SUM(IFF(spvs.attribution_model = 'url params', spvs.spvs_7d, 0))       AS spvs_7d_url,
	SUM(IFF(spvs.attribution_model = 'url params', spvs.spvs_14d, 0))      AS spvs_14d_url
FROM data_vault_mvp.dwh.iterable_crm_reporting__step12__model_scv_spv_data spvs
	LEFT JOIN data_vault_mvp.dwh.iterable_crm_reporting__step13__model_non_ame_campaigns em
			  ON COALESCE(spvs.message_id, em.message_id) = em.message_id
				  AND spvs.campaign_id::VARCHAR = em.campaign_id::VARCHAR
				  AND spvs.shiro_user_id = em.shiro_user_id
				  AND spvs.event_date BETWEEN em.send_start_date AND em.send_end_date
WHERE em.is_automated_campaign = TRUE
  AND em.message_id_email_hash = '9d18dac4fa30f548d39b42fb3bf68dcdb01f2f1345c0e03f3a31841d937dc8ab'
GROUP BY 1


SELECT *
FROM data_vault_mvp.dwh.iterable_crm_reporting__step12__model_scv_spv_data spvs
WHERE spvs.message_id = '58aeb53cd07d4ef5b896af174fad16a9'
;

-- no spvs found in step 12


SELECT
	page_url,
	contexts_com_secretescapes_content_context_1,
	event_tstamp,
	device_platform,
	page_referrer
FROM hygiene_vault_mvp.snowplow.event_stream
WHERE user_id = '76345808'
  AND event_tstamp::date >= '2024-11-09'
ORDER BY event_tstamp ASC
;

USE WAREHOUSE pipe_xlarge
;

SELECT
	sts.page_url,
	sts.event_tstamp,
	stba.touch_landing_page,
	stba.touch_experience,
	PARSE_URL(stba.touch_landing_page)['parameters']['utm_campaign']::VARCHAR AS landing_page_utm_campaign
FROM se.data.scv_touched_spvs sts
	INNER JOIN se.data_pii.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
WHERE stba.attributed_user_id = '76345808'
  AND stba.stitched_identity_type = 'se_user_id'
  AND sts.event_tstamp::date >= '2024-11-09'
ORDER BY event_tstamp ASC
;



SELECT
	PARSE_URL('https://www.secretescapes.de/beach-resort-unter-palmen-in-khao-lak-kostenfrei-stornierbar-khaolak-laguna-resort-khao-lak-thailand/sale-hotel?userId=76345808&timestamp=1731135882814&noPasswordSignIn=true&authHash=49017f9ddf2b494f6b8cc5bb05a90de4b2b5e94f&utm_medium=email&utm_source=newsletter&utm_campaign=11224492&utm_platform=ITERABLE&utm_content=SEGMENT_CORE_DE_ACT_01M_ATHENA_PoC_A&copyVersion=athenaSaturday_9&messageId=58aeb53cd07d4ef5b896af174fad16a9&sale_id=A26898&landing-page=sale-page')
;

USE WAREHOUSE pipe_xlarge
;

WITH
	last_click AS (
		SELECT
			'last click'                                                                                                       AS attribution_model,
			COALESCE(tmc.utm_campaign,
					 tba.app_push_open_context:dataFields:campaignId::VARCHAR)                                                 AS campaign_id,
			MAX(COALESCE(tmc.landing_page_parameters['messageId']::VARCHAR,
						 tba.app_push_open_context:dataFields:messageId::VARCHAR))
				OVER (PARTITION BY tba.attributed_user_id, COALESCE(tmc.utm_campaign,
																	tba.app_push_open_context:dataFields:campaignId::VARCHAR)) AS message_id,
			spvs.event_tstamp::DATE                                                                                            AS event_date,
			tba.attributed_user_id                                                                                             AS shiro_user_id
		FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs AS spvs
			INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes AS tba
					   ON spvs.touch_id = tba.touch_id
						   AND tba.stitched_identity_type = 'se_user_id'
			INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel AS tmc
					   ON spvs.touch_id = tmc.touch_id
		WHERE (
				  (
					  tmc.utm_medium = 'email'
						  AND tmc.utm_campaign IS NOT NULL
						  AND spvs.event_tstamp::DATE = '2024-11-13'
					  )
					  OR tba.app_push_open_context:dataFields:campaignId::VARCHAR IS NOT NULL
				  )
	)
SELECT
	lc.message_id,
	COUNT(*) AS spvs
FROM last_click lc
GROUP BY 1
;


USE WAREHOUSE pipe_xlarge
;

SELECT
	'last click'                                                       AS attribution_model,
	COALESCE(tmc.utm_campaign,
			 tba.app_push_open_context:dataFields:campaignId::VARCHAR) AS campaign_id,
	COALESCE(tmc.landing_page_parameters['messageId']::VARCHAR,
			 tba.app_push_open_context:dataFields:messageId::VARCHAR)  AS message_id_coalesce,
	MAX(message_id_coalesce)
		OVER (PARTITION BY tba.attributed_user_id, campaign_id)        AS message_id,
	spvs.event_tstamp,
	spvs.event_tstamp::DATE                                            AS event_date,
	tba.attributed_user_id                                             AS shiro_user_id
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs AS spvs
	INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes AS tba
			   ON spvs.touch_id = tba.touch_id
				   AND tba.stitched_identity_type = 'se_user_id'
	INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel AS tmc
			   ON spvs.touch_id = tmc.touch_id
WHERE (
	(
		tmc.utm_medium = 'email'
			AND tmc.utm_campaign IS NOT NULL
			AND spvs.event_tstamp::DATE >= '2021-11-03'
		)
		OR tba.app_push_open_context:dataFields:campaignId::VARCHAR IS NOT NULL
	)
--   AND campaign_id = '8848776'
--   AND tba.attributed_user_id = '46474072'
  AND campaign_id = '11224492'
  AND tba.attributed_user_id = '76345808'

------------------------------------------------------------------------------------------------------------------------

USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.iterable
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.iterable.campaign
	CLONE latest_vault.iterable.campaign
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.iterable.email_send
	CLONE latest_vault.iterable.email_send
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.iterable.email_open
	CLONE latest_vault.iterable.email_open
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.iterable.email_click
	CLONE latest_vault.iterable.email_click
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.iterable.email_unsubscribe
	CLONE latest_vault.iterable.email_unsubscribe
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.iterable.app_push_send
	CLONE latest_vault.iterable.app_push_send
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.iterable.app_push_open
	CLONE latest_vault.iterable.app_push_open
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh
;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.fact_booking
AS
SELECT *
FROM data_vault_mvp.dwh.fact_booking
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_attributes
	CLONE data_vault_mvp.dwh.user_attributes
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs
	CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions
	CLONE data_vault_mvp.single_customer_view_stg.module_touched_transactions
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_attribution
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel
;

CREATE SCHEMA IF NOT EXISTS dbt_dev_robin.bi_customer_insight
;

CREATE OR REPLACE TRANSIENT TABLE dbt_dev_robin.bi_customer_insight.ci_rfv_segments_historical_weekly
	CLONE dbt.bi_customer_insight.ci_rfv_segments_historical_weekly
;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.iterable_crm_reporting
	CLONE data_vault_mvp.dwh.iterable_crm_reporting
;

self_describing_task
\
    --include 'biapp.task_catalogue.dv.dwh.email.crm_reporting.iterable.iterable_crm_reporting.py' \
    --method 'run' \
    --start '2024-12-09 00:00:00' \
    --end '2024-12-09 00:00:00'
;



------------------------------------------------------------------------------------------------------------------------

CREATE SCHEMA collab.iterable_crm_reporting_debug
;

CREATE VIEW collab.iterable_crm_reporting_debug.iterable_crm_reporting AS
SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting
;

-- job returns duplications

SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting icr
QUALIFY COUNT(*) OVER (PARTITION BY icr.message_id_email_hash) > 1


/*
Example hashes with dupes
MESSAGE_ID_EMAIL_HASH	MESSAGE_ID	SEND_START_DATE	CAMPAIGN_ID
8f1252abd7956d454f321dda23a36b50adafbca3eae36ceebd47583cddb4a1f9	625a693e7163439a81af8ba08f3cf10b	2023-05-05	6756848
8f1252abd7956d454f321dda23a36b50adafbca3eae36ceebd47583cddb4a1f9	625a693e7163439a81af8ba08f3cf10b	2023-05-05	6756848
a69788208dcfe9d433f6a5e7e238f94460c5de54825e698c276a24b08434ad2d	9434221aa6ec43c0b99940b9c2978181	2023-09-01	7618718
a69788208dcfe9d433f6a5e7e238f94460c5de54825e698c276a24b08434ad2d	9434221aa6ec43c0b99940b9c2978181	2023-09-01	7618718
ddd033f3f111506050147c2cbaf50f195dcd20843c06f6c3f7b36537a8b31a05	a248e37045e5435c90d663f39706f9a0	2024-08-23	10835227
ddd033f3f111506050147c2cbaf50f195dcd20843c06f6c3f7b36537a8b31a05	a248e37045e5435c90d663f39706f9a0	2024-08-23	10835227
f8433867ba0d6f6800d430e6db4eec03ae1bc7926cca4e704f68b912b72e1713	48a4c19213374aa8837655f5788c23ce	2023-06-23	7112249
f8433867ba0d6f6800d430e6db4eec03ae1bc7926cca4e704f68b912b72e1713	48a4c19213374aa8837655f5788c23ce	2023-06-23	7112249
92a45bdb6acef04af518c85e987d1ade38292d30620291a2d04ed9cf7153961e	5e4d7b56f388481fa9081b760245bd68	2024-01-26	8848556
92a45bdb6acef04af518c85e987d1ade38292d30620291a2d04ed9cf7153961e	5e4d7b56f388481fa9081b760245bd68	2024-01-26	8848556
fa1e09ed89a9d82f042abe08d45f7a0bc69c0410bcb8fdd178238ed6e9e99866	268ba8b925904c31ac86819a9a743541	2023-06-16	6952380
fa1e09ed89a9d82f042abe08d45f7a0bc69c0410bcb8fdd178238ed6e9e99866	268ba8b925904c31ac86819a9a743541	2023-06-16	6952380
67bf217d203a731e31427b7f615a32c11e8e9032ecf1cb82b991810a3a0c6f9b	7c79c5d011dd4a66a79f3baada86e04e	2024-09-27	11200262
67bf217d203a731e31427b7f615a32c11e8e9032ecf1cb82b991810a3a0c6f9b	7c79c5d011dd4a66a79f3baada86e04e	2024-09-27	11200262
5068d05cfcc5d3cf1beae1fb3a33714d66f33f1ebb6c1f51bf355d65ab9acbec	1cf03986c4a44dcebde111659a3d8a93	2024-01-15	8769812
5068d05cfcc5d3cf1beae1fb3a33714d66f33f1ebb6c1f51bf355d65ab9acbec	1cf03986c4a44dcebde111659a3d8a93	2024-01-15	8769812
6ae3990972e4592637c59466e9ce71f97ecc9554e3951a449f595324cb672257	c47760c96a50416b824a41216bf2acad	2023-05-25	6909902
6ae3990972e4592637c59466e9ce71f97ecc9554e3951a449f595324cb672257	c47760c96a50416b824a41216bf2acad	2023-05-25	6909902
f110eb859edc542e47df0681f19ef9df76c811c91ea0f6d8a14900f9fa1cbf1e	b6bed965a07e4d8988eba8da14bb7945	2023-11-14	8276288
f110eb859edc542e47df0681f19ef9df76c811c91ea0f6d8a14900f9fa1cbf1e	b6bed965a07e4d8988eba8da14bb7945	2023-11-14	8276288
38cf83abc0db720ff3c294a6ed2f487849f60eb03c18ebe8f42daa938a9848cd	1f7fc6e5347b4c02835464bfdff0e412	2024-07-31	10622011
38cf83abc0db720ff3c294a6ed2f487849f60eb03c18ebe8f42daa938a9848cd	1f7fc6e5347b4c02835464bfdff0e412	2024-07-31	10622011
449a783e6780920922a843c9fe405b2cd27db2540fd635ca2bc9c42947b86b29	90283dba12244bed888826b261f4df49	2023-07-25	7323453
449a783e6780920922a843c9fe405b2cd27db2540fd635ca2bc9c42947b86b29	90283dba12244bed888826b261f4df49	2023-07-25	7323453
acc52c6cf1e2b305f1fb20d95cafb7b7d8c2ddccad1e4b6447101006f5183181	c3bbcb960bfe414a94fa928874ef1ad3	2024-08-19	10799834
acc52c6cf1e2b305f1fb20d95cafb7b7d8c2ddccad1e4b6447101006f5183181	c3bbcb960bfe414a94fa928874ef1ad3	2024-08-19	10799834
5826c4bd03be911f86d62f6b75bfe7eb486a3f789a03ce0b52d45895c730f5c1	43ff6d6f33314497b2e14a9be482543e	2024-08-23	10835227
5826c4bd03be911f86d62f6b75bfe7eb486a3f789a03ce0b52d45895c730f5c1	43ff6d6f33314497b2e14a9be482543e	2024-08-23	10835227
32c6b5dd94f0b7bac465cd56f33a07144fa942c7cecd011b6589c83689df333b	6bdf9f67dded439996f5ddbe5c3c9408	2024-04-05	9484272
32c6b5dd94f0b7bac465cd56f33a07144fa942c7cecd011b6589c83689df333b	6bdf9f67dded439996f5ddbe5c3c9408	2024-04-05	9484272
9038ceabaaa9774fedcb5d9a463338fba5fa69ab793e91e648025cfb04798285	78e980b5d4df4715ac6b7be6ee66cbd0	2024-06-05	10064385
9038ceabaaa9774fedcb5d9a463338fba5fa69ab793e91e648025cfb04798285	78e980b5d4df4715ac6b7be6ee66cbd0	2024-06-05	10064385
4f3e7818e194ce09fbd4754fffc11b826ebba558182c5c40d778399add363312	aeea666295754efe935c7d8d211ffa75	2024-09-09	11009144
4f3e7818e194ce09fbd4754fffc11b826ebba558182c5c40d778399add363312	aeea666295754efe935c7d8d211ffa75	2024-09-09	11009144
a9d39e8ec233c7bd1bef03527eabea1371ee2aa68a11dbad4641499a882b7f47	bbdfbfec8fa64a7cb431b34ea88efbb9	2024-07-12	10429069
a9d39e8ec233c7bd1bef03527eabea1371ee2aa68a11dbad4641499a882b7f47	bbdfbfec8fa64a7cb431b34ea88efbb9	2024-07-12	10429069
91594766f0d064b91f2377c07a0e787735dca7543ca526397749fd99276e1e61	2ea143be4fef4db79c7b2e32afff52d8	2023-10-20	8067540
91594766f0d064b91f2377c07a0e787735dca7543ca526397749fd99276e1e61	2ea143be4fef4db79c7b2e32afff52d8	2023-10-20	8067540
ae85780de6ef8d0834f82bac22a9e74ef436303a935b87599ade40628827e1b6	73c4410f32774340942a5a4be5ef330b	2024-07-17	10486605
ae85780de6ef8d0834f82bac22a9e74ef436303a935b87599ade40628827e1b6	73c4410f32774340942a5a4be5ef330b	2024-07-17	10486605
12cf585ff4a460371a9559df4ca6956aa68ce6e710e697cb159b7da8c5348843	2bc9fc33d5444190b95bf3676e54e603	2023-06-23	7112249
12cf585ff4a460371a9559df4ca6956aa68ce6e710e697cb159b7da8c5348843	2bc9fc33d5444190b95bf3676e54e603	2023-06-23	7112249
a1cf43d7162643f3a2a6bf765b51aee1affc7de77648814f468f4113ab4b0c0a	ed657a7bf1e4423083dbca425531f7f7	2023-11-10	8248566
a1cf43d7162643f3a2a6bf765b51aee1affc7de77648814f468f4113ab4b0c0a	ed657a7bf1e4423083dbca425531f7f7	2023-11-10	8248566
d4c9f9d5c9fab632838f58ced14ae369f47084d731afc221e720d1239a5ba02c	939d34b3f7e34ec095c05865feab6853	2023-06-16	7054508
d4c9f9d5c9fab632838f58ced14ae369f47084d731afc221e720d1239a5ba02c	939d34b3f7e34ec095c05865feab6853	2023-06-16	7054508
1c9bab1452f90041bd5ba21dfc8c1231d1429148d5de45687eb68c55ea3920be	d7d0b76e69ed4d9ebea5a38797988a69	2024-07-02	10331764
1c9bab1452f90041bd5ba21dfc8c1231d1429148d5de45687eb68c55ea3920be	d7d0b76e69ed4d9ebea5a38797988a69	2024-07-02	10331764
53a0027d2bd87e7832f2c2a5730f7cdca89a6bd8a7775899398e1b114eb40609	1c7dab91b4de4ce0a4b2f00a6644b67f	2023-07-25	7323756
53a0027d2bd87e7832f2c2a5730f7cdca89a6bd8a7775899398e1b114eb40609	1c7dab91b4de4ce0a4b2f00a6644b67f	2023-07-25	7323756

*/

SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting icr
WHERE icr.message_id_email_hash = '8f1252abd7956d454f321dda23a36b50adafbca3eae36ceebd47583cddb4a1f9'
  AND icr.send_start_date = '2023-05-05'

SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step12__model_scv_spv_data icr
WHERE icr.message_id = '625a693e7163439a81af8ba08f3cf10b'
;

-- no dupes at attribution spv level

SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step13__model_non_ame_campaigns icr
WHERE icr.message_id = '625a693e7163439a81af8ba08f3cf10b'
;

-- dupes appear in step 13


WITH
	scv_bookings AS (
		SELECT
			bk.attribution_model,
			bk.campaign_id,
			bk.message_id,
			bk.shiro_user_id,
			SUM(bookings)       AS bookings,
			SUM(bookings_1d)    AS bookings_1d,
			SUM(bookings_7d)    AS bookings_7d,
			SUM(bookings_14d)   AS bookings_14d,
			SUM(margin_gbp)     AS margin_gbp,
			SUM(margin_gbp_1d)  AS margin_gbp_1d,
			SUM(margin_gbp_7d)  AS margin_gbp_7d,
			SUM(margin_gbp_14d) AS margin_gbp_14d
		FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step11__model_scv_booking_data bk
		WHERE bk.campaign_id = '6756848'
		GROUP BY 1, 2, 3, 4
	),
	scv_spvs AS (
		SELECT
			spv.attribution_model,
			spv.campaign_id,
			spv.message_id,
			spv.shiro_user_id,
			SUM(spv.spvs)     AS spvs,
			SUM(spv.spvs_1d)  AS spvs_1d,
			SUM(spv.spvs_7d)  AS spvs_7d,
			SUM(spv.spvs_14d) AS spvs_14d
		FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step12__model_scv_spv_data spv
		WHERE spv.campaign_id = '6756848' AND spv.shiro_user_id = '3900682'
		GROUP BY 1, 2, 3, 4
	)
SELECT
	em.message_id_email_hash,
	em.message_id,
	em.campaign_id,
	em.crm_channel_type,
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
	COALESCE(em.email_sends, 0)                 AS email_sends,

	COALESCE(em.email_opens, 0)                 AS email_opens,
	COALESCE(em.email_opens_1d, 0)              AS email_opens_1d,
	COALESCE(em.email_opens_7d, 0)              AS email_opens_7d,
	COALESCE(em.email_opens_14d, 0)             AS email_opens_14d,

	COALESCE(em.unique_email_opens, 0)          AS unique_email_opens,
	COALESCE(em.unique_email_opens_1d, 0)       AS unique_email_opens_1d,
	COALESCE(em.unique_email_opens_7d, 0)       AS unique_email_opens_7d,
	COALESCE(em.unique_email_opens_14d, 0)      AS unique_email_opens_14d,

	COALESCE(em.email_clicks, 0)                AS email_clicks,
	COALESCE(em.email_clicks_1d, 0)             AS email_clicks_1d,
	COALESCE(em.email_clicks_7d, 0)             AS email_clicks_7d,
	COALESCE(em.email_clicks_14d, 0)            AS email_clicks_14d,

	COALESCE(em.unique_email_clicks, 0)         AS unique_email_clicks,
	COALESCE(em.unique_email_clicks_1d, 0)      AS unique_email_clicks_1d,
	COALESCE(em.unique_email_clicks_7d, 0)      AS unique_email_clicks_7d,
	COALESCE(em.unique_email_clicks_14d, 0)     AS unique_email_clicks_14d,

	em.first_open_event_date,
	em.first_open_event_time,
	em.first_click_event_date,
	em.first_click_event_time,
	em.unsub_event_date,
	em.unsub_event_time,
	COALESCE(em.email_unsubs, 0)                AS email_unsubs,
	COALESCE(em.email_unsubs_1d, 0)             AS email_unsubs_1d,
	COALESCE(em.email_unsubs_7d, 0)             AS email_unsubs_7d,
	COALESCE(em.email_unsubs_14d, 0)            AS email_unsubs_14d,

	COALESCE(em.email_unsubs_complaint, 0)      AS email_unsubs_complaint,
	COALESCE(em.email_unsubs_complaint_1d, 0)   AS email_unsubs_complaint_1d,
	COALESCE(em.email_unsubs_complaint_7d, 0)   AS email_unsubs_complaint_7d,
	COALESCE(em.email_unsubs_complaint_14d, 0)  AS email_unsubs_complaint_14d,

	COALESCE(em.email_unsubs_email_link, 0)     AS email_unsubs_email_link,
	COALESCE(em.email_unsubs_email_link_1d, 0)  AS email_unsubs_email_link_1d,
	COALESCE(em.email_unsubs_email_link_7d, 0)  AS email_unsubs_email_link_7d,
	COALESCE(em.email_unsubs_email_link_14d, 0) AS email_unsubs_email_link_14d,

	COALESCE(bk_lc.bookings, 0)                 AS bookings_lc,
	COALESCE(bk_lc.bookings_1d, 0)              AS bookings_1d_lc,
	COALESCE(bk_lc.bookings_7d, 0)              AS bookings_7d_lc,
	COALESCE(bk_lc.bookings_14d, 0)             AS bookings_14d_lc,

	COALESCE(bk_lc.margin_gbp, 0)               AS margin_gbp_lc,
	COALESCE(bk_lc.margin_gbp, 0)               AS margin_gbp_1d_lc,
	COALESCE(bk_lc.margin_gbp, 0)               AS margin_gbp_7d_lc,
	COALESCE(bk_lc.margin_gbp, 0)               AS margin_gbp_14d_lc,

	COALESCE(bk_lnd.bookings, 0)                AS bookings_lnd,
	COALESCE(bk_lnd.bookings_1d, 0)             AS bookings_1d_lnd,
	COALESCE(bk_lnd.bookings_7d, 0)             AS bookings_7d_lnd,
	COALESCE(bk_lnd.bookings_14d, 0)            AS bookings_14d_lnd,

	COALESCE(bk_lnd.margin_gbp, 0)              AS margin_gbp_lnd,
	COALESCE(bk_lnd.margin_gbp, 0)              AS margin_gbp_1d_lnd,
	COALESCE(bk_lnd.margin_gbp, 0)              AS margin_gbp_7d_lnd,
	COALESCE(bk_lnd.margin_gbp, 0)              AS margin_gbp_14d_lnd,

	COALESCE(spv_lc.spvs, 0)                    AS spvs_lc,
	COALESCE(spv_lc.spvs_1d, 0)                 AS spvs_1d_lc,
	COALESCE(spv_lc.spvs_7d, 0)                 AS spvs_7d_lc,
	COALESCE(spv_lc.spvs_14d, 0)                AS spvs_14d_lc,

	COALESCE(spv_lnd.spvs, 0)                   AS spvs_lnd,
	COALESCE(spv_lnd.spvs_1d, 0)                AS spvs_1d_lnd,
	COALESCE(spv_lnd.spvs_7d, 0)                AS spvs_7d_lnd,
	COALESCE(spv_lnd.spvs_14d, 0)               AS spvs_14d_lnd,

	COALESCE(url.spvs, 0)                       AS spvs_url,
	COALESCE(url.spvs_1d, 0)                    AS spvs_1d_url,
	COALESCE(url.spvs_7d, 0)                    AS spvs_7d_url,
	COALESCE(url.spvs_14d, 0)                   AS spvs_14d_url,
	spv_lc.*
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
WHERE em.message_id_email_hash = '8f1252abd7956d454f321dda23a36b50adafbca3eae36ceebd47583cddb4a1f9'


-- copied step 13 code, added filters for message id
-- no dupes found when filtered by message id
-- filtered for message_id_email_hash
-- no dupes when filters for message id in scv dataset exists
-- removed message id filters added campaign filter


SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step12__model_scv_spv_data spv
WHERE spv.campaign_id = '6756848' AND spv.shiro_user_id = '3900682'

-- found duplications with null message id after a click event has happened WITH a message id
/*
 ATTRIBUTION_MODEL	CAMPAIGN_ID	MESSAGE_ID	EVENT_DATE	SHIRO_USER_ID	SPVS	SPVS_1D	SPVS_7D	SPVS_14D
last click	6756848	625a693e7163439a81af8ba08f3cf10b	2023-05-05	3900682	5	5	5	5
last non direct	6756848	625a693e7163439a81af8ba08f3cf10b	2023-05-05	3900682	12	12	12	12
last non direct	6756848		2023-05-08	3900682	1	0	1	1
last non direct	6756848		2023-05-06	3900682	5	5	5	5

 */

-- taking the step 12 code and filtering for this campaign and user
WITH
	step_12 AS (
		SELECT
			'last non direct'       AS attribution_model,
			COALESCE(
					tmc.utm_campaign,
					tba2.app_push_open_context:dataFields:campaignId::VARCHAR
			)                       AS campaign_id,
			/*------------------------------------------------------------------------------------------------------------
			COALESCE(
					tmc.landing_page_parameters['messageId']::VARCHAR,
					tba.app_push_open_context:dataFields:messageId::VARCHAR
			)                       AS message_id_coalesce,

			-- max message id window function

			-- if there is a message id available, prioritise that however we have  found instances
			-- where the message id is not populated on a given day but have seen events for the
			-- same campaign with a message id using max partition on the campaign id and date to
			-- remove nulls and use the message id from another spv for the same campaign on that
			-- event date
			COALESCE(
					message_id_coalesce, -- note this is an aliased field
					MAX(message_id_coalesce) OVER (
						PARTITION BY
							tba.attributed_user_id,
							campaign_id, -- note this is an aliased field
							spvs.event_tstamp::DATE
						)
			)                       AS message_id,
			------------------------------------------------------------------------------------------------------------*/
			/*----------------------------------------------------------------------------------------------------------
			  -- non wind
			 */
			COALESCE(
					tmc.landing_page_parameters['messageId']::VARCHAR,
					tba.app_push_open_context:dataFields:messageId::VARCHAR
			)                       AS message_id,

			-----------------------------------------------------------------------------------------------------------*/
			spvs.event_tstamp::DATE AS event_date,
			tba.attributed_user_id  AS shiro_user_id
		FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs AS spvs
			INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes AS tba
					   ON spvs.touch_id = tba.touch_id
						   AND tba.stitched_identity_type = 'se_user_id'
			INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution attr
					   ON spvs.touch_id = attr.touch_id
						   AND attr.attribution_model = 'last non direct'
			INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel tmc
					   ON attr.attributed_touch_id = tmc.touch_id
			INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes tba2
					   ON attr.attributed_touch_id = tba2.touch_id
						   AND tba2.stitched_identity_type = 'se_user_id'
		WHERE ((tmc.utm_medium = 'email'
			AND tmc.utm_campaign IS NOT NULL
			AND spvs.event_tstamp::DATE >= '2021-11-03'
				   )
			OR tba2.app_push_open_context:dataFields:campaignId::VARCHAR IS NOT NULL)
-- 		  AND shiro_user_id = '3900682'
-- 		  AND campaign_id = '6756848'
-- 		  AND shiro_user_id = '76345808'
-- 		  AND campaign_id = '11224492'
		  AND tba.attributed_user_id = '46474072'
		  AND campaign_id = '8848776'
	),
	step_12_agg AS (
		SELECT
			asd.attribution_model,
			asd.campaign_id,
			asd.message_id,
			asd.event_date,
			asd.shiro_user_id,
			COUNT(*) AS spvs
		FROM step_12 asd
		GROUP BY 1, 2, 3, 4, 5
	),

	scv_spvs AS (
		SELECT
			spv.attribution_model,
			spv.campaign_id,
			spv.message_id,
			spv.shiro_user_id,
			SUM(spv.spvs) AS spvs
		FROM step_12_agg spv
		GROUP BY 1, 2, 3, 4
	),
	step_13 AS (
		SELECT
			em.message_id_email_hash,
			em.message_id,
			em.campaign_id,
			em.crm_channel_type,
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

			em.first_open_event_date,
			em.first_open_event_time,
			em.first_click_event_date,
			em.first_click_event_time,
			em.unsub_event_date,
			em.unsub_event_time,


			COALESCE(spv_lnd.spvs, 0) AS spvs_lnd

		FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step10__model_iterable_data em

			LEFT JOIN scv_spvs spv_lnd
					  ON COALESCE(spv_lnd.message_id, em.message_id) = em.message_id
						  AND spv_lnd.campaign_id::VARCHAR = em.campaign_id::VARCHAR
						  AND spv_lnd.shiro_user_id = em.shiro_user_id
						  AND spv_lnd.attribution_model = 'last non direct'
						  AND em.is_automated_campaign = FALSE
	),
	agg_step_13 AS (
		SELECT
			message_id_email_hash,
			message_id,
			campaign_id,
			crm_channel_type,
			splittable_email_name,
			mapped_crm_date,
			mapped_territory,
			current_affiliate_territory,
			mapped_objective,
			mapped_platform,
			mapped_campaign,
			mapped_theme,
			mapped_segment,
			is_athena,
			is_automated_campaign,
			ame_calculated_campaign_name,
			email_hash,
			shiro_user_id,
			campaign_name,
			send_event_date,
			send_event_time,
			send_start_date,
			send_end_date,

			SUM(spvs_lnd)
		FROM step_13
		GROUP BY ALL
	)

SELECT *
FROM agg_step_13
;



-- if automated campaign, campaign id be the same message id different
-- if non automated campaign, campaign id be different so joining on campaign id is safe


-- problem appears to be for automated campaigns that have a mix of spvs that have the message id and some that don't
-- if we don't have a message id for non automated campaign doesn't matter as the campaign id is safe
-- for automated campaigns without a message id we need to associate them to a relevant send  (not campaign)
-- it would appear that the duplication issue is contained to non automated campaigns

-- if we fix the non automated campaign nulls with a max then it will break automated campaigns as all spvs will
-- have a message id
-- if we don't fix with a max then we have duplications on non automated campaigns because we have a mix of message id
-- and non message id


--- tldr we've introduced a fix for non automated campaigns to handle duplication when a user will click an email with a
-- certain campaign id and some links have a message id and others don't. To fix this we MAX'd the message id, however
-- this will create a message id for all spvs without a message id but have a campaign id, as a result this will
-- misattribute any automated campaign's spv without a message id to one message id

-- to fix we should simply lean on the join message id/no message id logic and aggregate up after


------------------------------------------------------------------------------------------------------------------------

-- refactor for readability
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.iterable_crm_reporting_20241210 CLONE data_vault_mvp_dev_robin.dwh.iterable_crm_reporting
;

-- 8401462451 rows before refactor
-- 8401462451 rows after refactor

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.iterable_crm_reporting_20241210_refactor CLONE data_vault_mvp_dev_robin.dwh.iterable_crm_reporting


-- bug found with non automated campaign wtihout a message id, scv spvs and bookings won't join back unless it occurs
-- within 30 days of send



SELECT
                em.message_id_email_hash,
                em.message_id,
                em.campaign_id,
                em.crm_channel_type,
                COALESCE(em.splittable_email_name,em.campaign_name) AS combined_email_name,
                IFF(is_automated_campaign, 'Not trading', 'Trading') AS trading,
                CASE
                    WHEN LOWER(SPLIT_PART(em.campaign_name,'_', 1)::VARCHAR) = 'amne' THEN 'Newsletter'
                    WHEN LOWER(SPLIT_PART(em.campaign_name,'_', 1)::VARCHAR) = 'amte' THEN 'Trigger'
                    WHEN LOWER(SPLIT_PART(em.campaign_name,'_', 1)::VARCHAR) = 'amle' THEN 'Lifecycle'
                    ELSE 'Newsletter'
                END AS email_type,
                em.splittable_email_name,
                em.mapped_crm_date,
                em.mapped_territory,
                em.current_affiliate_territory,
                COALESCE(em.mapped_objective, SPLIT_PART(combined_email_name, '_', 3)) AS mapped_objective,
                COALESCE(em.mapped_platform, SPLIT_PART(combined_email_name, '_', 4)) AS mapped_platform,
                COALESCE(em.mapped_campaign, SPLIT_PART(combined_email_name, '_', 5)) AS mapped_campaign,
                COALESCE(em.mapped_theme, SPLIT_PART(combined_email_name, '_', 6)) AS mapped_theme,
                COALESCE(em.mapped_segment, SPLIT_PART(combined_email_name, '_', 8)) AS mapped_segment,
                em.is_athena,
                em.is_automated_campaign,
                em.ame_calculated_campaign_name,
                em.email_hash,
                em.shiro_user_id,
                em.campaign_name,
                rfv.rfv_segment,
                rfv.lifecycle AS rfv_lifecycle,
                CASE
                    WHEN rfv.rfv_segment IS NULL AND
                            rfv.signup_date = DATE_TRUNC('week',em.send_event_date)
                        THEN 'New Signup in Week'
                    ELSE rfv.rfv_segment
                    END AS rfv_segment_calc,
                CASE
                    WHEN rfv.lifecycle IS NULL AND
                            rfv.signup_date = DATE_TRUNC('week',em.send_event_date)
                        THEN 'Early Life Active'
                    ELSE rfv.lifecycle
                    END AS rfv_lifecycle_calc,
                em.send_event_date,
                em.send_event_time,
                em.send_start_date,
                em.send_end_date,
                em.email_sends,

                o.email_opens,
                o.email_opens_1d,
                o.email_opens_7d,
                o.email_opens_14d,
                o.unique_email_opens,
                o.unique_email_opens_1d,
                o.unique_email_opens_7d,
                o.unique_email_opens_14d,

                c.email_clicks,
                c.email_clicks_1d,
                c.email_clicks_7d,
                c.email_clicks_14d,
                c.unique_email_clicks,
                c.unique_email_clicks_1d,
                c.unique_email_clicks_7d,
                c.unique_email_clicks_14d,

                o.first_open_event_date,
                o.first_open_event_time,

                c.first_click_event_date,
                c.first_click_event_time,

                u.unsub_event_date,
                u.unsub_event_time,
                u.email_unsubs,
                u.email_unsubs_1d,
                u.email_unsubs_7d,
                u.email_unsubs_14d,
                u.email_unsubs_complaint,
                u.email_unsubs_complaint_1d,
                u.email_unsubs_complaint_7d,
                u.email_unsubs_complaint_14d,
                u.email_unsubs_email_link,
                u.email_unsubs_email_link_1d,
                u.email_unsubs_email_link_7d,
                u.email_unsubs_email_link_14d,

                COALESCE(scv.bookings_lc, 0)            AS bookings_lc,
                COALESCE(scv.bookings_1d_lc, 0)         AS bookings_1d_lc,
                COALESCE(scv.bookings_7d_lc, 0)         AS bookings_7d_lc,
                COALESCE(scv.bookings_14d_lc, 0)        AS bookings_14d_lc,

                COALESCE(scv.margin_gbp_lc, 0)          AS margin_gbp_lc,
                COALESCE(scv.margin_gbp_1d_lc, 0)       AS margin_gbp_1d_lc,
                COALESCE(scv.margin_gbp_7d_lc, 0)       AS margin_gbp_7d_lc,
                COALESCE(scv.margin_gbp_14d_lc, 0)      AS margin_gbp_14d_lc,

                COALESCE(scv.bookings_lnd, 0)           AS bookings_lnd,
                COALESCE(scv.bookings_lnd, 0)           AS bookings_1d_lnd,
                COALESCE(scv.bookings_lnd, 0)           AS bookings_7d_lnd,
                COALESCE(scv.bookings_lnd, 0)           AS bookings_14d_lnd,

                COALESCE(scv.margin_gbp_lnd, 0)         AS margin_gbp_lnd,
                COALESCE(scv.margin_gbp_lnd, 0)         AS margin_gbp_1d_lnd,
                COALESCE(scv.margin_gbp_lnd, 0)         AS margin_gbp_7d_lnd,
                COALESCE(scv.margin_gbp_lnd, 0)         AS margin_gbp_14d_lnd,

                COALESCE(scv.spvs_lc, 0)                AS spvs_lc,
                COALESCE(scv.spvs_lc, 0)                AS spvs_1d_lc,
                COALESCE(scv.spvs_lc, 0)                AS spvs_7d_lc,
                COALESCE(scv.spvs_lc, 0)                AS spvs_14d_lc,

                COALESCE(scv.spvs_lnd, 0)               AS spvs_lnd,
                COALESCE(scv.spvs_lnd, 0)               AS spvs_1d_lnd,
                COALESCE(scv.spvs_lnd, 0)               AS spvs_7d_lnd,
                COALESCE(scv.spvs_lnd, 0)               AS spvs_14d_lnd,

                COALESCE(scv.spvs_url, 0)               AS spvs_url,
                COALESCE(scv.spvs_url, 0)               AS spvs_1d_url,
                COALESCE(scv.spvs_url, 0)               AS spvs_7d_url,
                COALESCE(scv.spvs_url, 0)               AS spvs_14d_url

            FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step06__aggregate_sends em
                -- iterable event data
                LEFT JOIN data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step07__aggregate_opens AS o
                    ON o.message_id_email_hash = em.message_id_email_hash
                    AND o.campaign_id = em.campaign_id
                LEFT JOIN data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step08__aggregate_clicks AS c
                    ON c.message_id_email_hash = em.message_id_email_hash
                    AND c.campaign_id = em.campaign_id
                LEFT JOIN data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step09__aggregate_unsubs AS u
                    ON u.message_id_email_hash = em.message_id_email_hash
                    AND u.campaign_id = em.campaign_id

                -- scv event data
                LEFT JOIN data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step12__model_scv_data scv
                    ON em.message_id_email_hash = scv.message_id_email_hash

                -- rfv
                LEFT JOIN data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step13__model_rfv rfv
                    ON rfv.shiro_user_id = em.shiro_user_id
                    AND rfv.event_date = DATE_TRUNC('week',em.send_event_date)
