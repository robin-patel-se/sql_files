/*
This campaign ID is a core email send so should be good to check ID - 14094689 (edited)
11:30
& this is a push campaign ID which is also to most users 7080502
*/


SELECT
	icrm.send_event_date,
	icrm.campaign_id,
	SUM(icrm.email_clicks_1d),
	SUM(icrm.email_opens_1d),
	SUM(icrm.spvs_1d_lc),
	SUM(icrm.spvs_1d_lnd),
	SUM(icrm.bookings_1d_lc),
	SUM(icrm.bookings_1d_lnd)
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__migration icrm
WHERE icrm.campaign_id = 14094689
GROUP BY icrm.send_event_date, icrm.campaign_id


SELECT
	icr.send_event_date,
	icr.campaign_id,
	SUM(icr.email_clicks_1d),
	SUM(icr.email_opens_1d),
	SUM(icr.spvs_1d_lc),
	SUM(icr.spvs_1d_lnd),
	SUM(icr.bookings_1d_lc),
	SUM(icr.bookings_1d_lnd)
FROM data_vault_mvp.dwh.iterable_crm_reporting icr
WHERE icr.campaign_id = 14094689
GROUP BY icr.send_event_date, icr.campaign_id
;

USE WAREHOUSE pipe_xlarge
;
-- migration
SELECT
	icrm.send_event_date,
	icrm.campaign_id,
	SUM(icrm.email_clicks_1d),
	SUM(icrm.email_opens_1d),
	SUM(icrm.spvs_1d_lc),
	SUM(icrm.spvs_1d_lnd),
	SUM(icrm.bookings_1d_lc),
	SUM(icrm.bookings_1d_lnd)
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__migration icrm
WHERE icrm.campaign_id = 7080502
GROUP BY icrm.send_event_date, icrm.campaign_id

-- prod
SELECT
	icr.send_event_date,
	icr.campaign_id,
	SUM(icr.email_clicks_1d),
	SUM(icr.email_opens_1d),
	SUM(icr.spvs_1d_lc),
	SUM(icr.spvs_1d_lnd),
	SUM(icr.spvs_1d_url),
	SUM(icr.bookings_1d_lc),
	SUM(icr.bookings_1d_lnd),
FROM data_vault_mvp.dwh.iterable_crm_reporting icr
WHERE icr.campaign_id = 7080502
GROUP BY icr.send_event_date, icr.campaign_id
;

SELECT
	icrs.send_event_date,
	icrs.campaign_id,
	SUM(icrs.spvs_1d_lc),
	SUM(icrs.spvs_1d_lnd),
	SUM(icrs.spvs_1d_url),
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__spvs icrs
WHERE icrs.campaign_id = 7080502
GROUP BY icrs.send_event_date, icrs.campaign_id
;


-- prod
SELECT
	icr.send_event_date,
	icr.campaign_id,
	SUM(icr.spvs_1d_lc),
	SUM(icr.spvs_1d_lnd),
	SUM(icr.spvs_1d_url),
	SUM(icr.spvs_7d_lc),
	SUM(icr.spvs_7d_lnd),
	SUM(icr.spvs_7d_url),
FROM data_vault_mvp.dwh.iterable_crm_reporting icr
WHERE icr.campaign_id = 7080502
GROUP BY icr.send_event_date, icr.campaign_id
;

--dev
SELECT
	icrs.send_event_date,
	icrs.campaign_id,
	SUM(icrs.spvs_1d_lc),
	SUM(icrs.spvs_1d_lnd),
	SUM(icrs.spvs_1d_url),
	SUM(icrs.spvs_7d_lc),
	SUM(icrs.spvs_7d_lnd),
	SUM(icrs.spvs_7d_url),
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__spvs icrs
WHERE icrs.campaign_id = 7080502
GROUP BY icrs.send_event_date, icrs.campaign_id
;

SELECT
	icrs.send_event_date,
	icrs.campaign_id,
	COUNT(*) AS sends
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__sends icrs
WHERE icrs.campaign_id = 7080502
GROUP BY icrs.send_event_date, icrs.campaign_id
;

SELECT
	MIN(icrs.send_event_date)
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__sends icrs
;

USE WAREHOUSE pipe_xlarge
;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__spvs__step03__stack_spv_attribution_models s
WHERE s.campaign_id = '7080502'
  AND s.attribution_model = 'last non direct'
;

SELECT
	s.send_event_date,
	SUM(s.spvs_1d),
	SUM(s.spvs_7d)
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__spvs__step04__attach_send_data s
WHERE s.campaign_id = '7080502'
  AND s.attribution_model = 'last non direct'
GROUP BY s.send_event_date
;

SELECT
	app_push_open_context:dataFields:campaignId::VARCHAR,
	*
FROM dwh.iterable_crm_reporting__spvs__step01__batch_spvs icrss01bs
WHERE app_push_open_context IS NOT NULL
;

WITH
	dev AS (
		SELECT
			'last non direct' AS attribution_model,
			COALESCE(
					touch_marketing_channel.utm_campaign,
					touch_basic_attributes.app_push_open_context:dataFields:campaignId::VARCHAR
			)                 AS campaign_id,
			COALESCE(
					touch_marketing_channel.landing_page_parameters['messageId']::VARCHAR,
					touch_basic_attributes.app_push_open_context:dataFields:messageId::VARCHAR
			)                 AS message_id,
			spvs.*

		FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__spvs__step01__batch_spvs spvs
			INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution touch_attribution
					   ON spvs.touch_id = touch_attribution.touch_id
						   AND touch_attribution.attribution_model = 'last non direct'
						   AND touch_attribution.touch_start_tstamp >= DATEADD('day', -28, '2021-11-02 04:30:00'::DATE)
			INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel touch_marketing_channel
					   ON touch_attribution.attributed_touch_id = touch_marketing_channel.touch_id
						   -- additional days in lookback to allow for attribution of 30d
						   AND
						  touch_marketing_channel.touch_start_tstamp >= DATEADD('day', -60, '2021-11-02 04:30:00'::DATE)
						   -- attributes from the session joined via attribution
			INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes touch_basic_attributes
					   ON touch_attribution.attributed_touch_id = touch_basic_attributes.touch_id
						   AND touch_basic_attributes.stitched_identity_type = 'se_user_id'
						   -- additional days in lookback to allow for attribution of 30d
						   AND
						  touch_basic_attributes.touch_start_tstamp >= DATEADD('day', -60, '2021-11-02 04:30:00'::DATE)
		WHERE (touch_marketing_channel.utm_medium = 'email' AND touch_marketing_channel.utm_campaign IS NOT NULL)
		   OR touch_basic_attributes.app_push_open_context:dataFields:campaignId::VARCHAR IS NOT NULL
	)
SELECT
	dev.message_id IS NULL,
	COUNT(*)
FROM dev
WHERE dev.campaign_id = '7080502'
GROUP BY 1
;

WITH
	prod AS (
		SELECT
			'last non direct'       AS attribution_model,
			COALESCE(
					tmc.utm_campaign,
					tba2.app_push_open_context:dataFields:campaignId::VARCHAR
			)                       AS campaign_id,
			COALESCE(
					tmc.landing_page_parameters['messageId']::VARCHAR,
					tba.app_push_open_context:dataFields:messageId::VARCHAR
			)                       AS message_id,
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
		WHERE (
				  tmc.utm_medium = 'email'
					  AND tmc.utm_campaign IS NOT NULL
					  AND spvs.event_tstamp::DATE >= '2021-11-03'
				  ) OR
			  tba2.app_push_open_context:dataFields:campaignId::VARCHAR IS NOT NULL
	)
SELECT
	prod.message_id IS NULL,
	COUNT(*)
FROM prod
WHERE prod.campaign_id = '7080502'
GROUP BY 1
;



SELECT
	id,
	*
FROM latest_vault.iterable.campaign c sample row (20 rows)
WHERE c.campaign_created_at >= CURRENT_DATE() - 28
;
;

/*
ID
'14340457',
'14358055',
'14325451',
'14335715',
'14326683',
'14326060',
'14108879',
'14156216',
'14143319',
'14174695',
'14268776',
'14239246',
'14159506',
'14050834',
'14186853',
'14069774',
'14290123',
'14268916',
'14156593',
'14147357',




*/
USE WAREHOUSE pipe_xlarge
;

-- prod
SELECT
	icr.campaign_id,
	SUM(icr.email_opens_1d),
	SUM(icr.email_opens_7d),
	SUM(icr.email_clicks_1d),
	SUM(icr.email_clicks_7d),
	SUM(icr.email_unsubs_1d),
	SUM(icr.email_unsubs_7d),
	SUM(icr.spvs_1d_lc),
	SUM(icr.spvs_1d_lnd),
	SUM(icr.spvs_1d_url),
	SUM(icr.spvs_7d_lc),
	SUM(icr.spvs_7d_lnd),
	SUM(icr.spvs_7d_url),
	SUM(icr.bookings_1d_lc),
	SUM(icr.bookings_1d_lnd),
	SUM(icr.bookings_7d_lc),
	SUM(icr.bookings_7d_lnd),
	SUM(icr.margin_gbp_1d_lc),
	SUM(icr.margin_gbp_1d_lnd),
	SUM(icr.margin_gbp_7d_lc),
	SUM(icr.margin_gbp_7d_lnd),
FROM data_vault_mvp.dwh.iterable_crm_reporting icr
WHERE icr.campaign_id IN (
						  '14340457',
						  '14358055',
						  '14325451',
						  '14335715',
						  '14326683',
						  '14326060',
						  '14108879',
						  '14156216',
						  '14143319',
						  '14174695',
						  '14268776',
						  '14239246',
						  '14159506',
						  '14050834',
						  '14186853',
						  '14069774',
						  '14290123',
						  '14268916',
						  '14156593',
						  '14147357'
	)
GROUP BY 1
;

-- migration
SELECT
	icrm.campaign_id,
	SUM(icrm.email_opens_1d),
	SUM(icrm.email_opens_7d),
	SUM(icrm.email_clicks_1d),
	SUM(icrm.email_clicks_7d),
	SUM(icrm.email_unsubs_1d),
	SUM(icrm.email_unsubs_7d),
	SUM(icrm.spvs_1d_lc),
	SUM(icrm.spvs_1d_lnd),
	SUM(icrm.spvs_1d_url),
	SUM(icrm.spvs_7d_lc),
	SUM(icrm.spvs_7d_lnd),
	SUM(icrm.spvs_7d_url),
	SUM(icrm.bookings_1d_lc),
	SUM(icrm.bookings_1d_lnd),
	SUM(icrm.bookings_7d_lc),
	SUM(icrm.bookings_7d_lnd),
	SUM(icrm.margin_gbp_1d_lc),
	SUM(icrm.margin_gbp_1d_lnd),
	SUM(icrm.margin_gbp_7d_lc),
	SUM(icrm.margin_gbp_7d_lnd),
FROM data_vault_mvp.dwh.iterable_crm_reporting__migration icrm
WHERE icrm.campaign_id IN (
						   '14340457',
						   '14358055',
						   '14325451',
						   '14335715',
						   '14326683',
						   '14326060',
						   '14108879',
						   '14156216',
						   '14143319',
						   '14174695',
						   '14268776',
						   '14239246',
						   '14159506',
						   '14050834',
						   '14186853',
						   '14069774',
						   '14290123',
						   '14268916',
						   '14156593',
						   '14147357'
	)
GROUP BY 1
;

-- legacy
USE WAREHOUSE pipe_xlarge
;

SELECT
	icr.send_event_date,
	SUM(icr.email_sends)       AS email_sends,
	SUM(icr.email_opens_1d)    AS email_opens_1d,
	SUM(icr.email_opens_7d)    AS email_opens_7d,
	SUM(icr.email_clicks_1d)   AS email_clicks_1d,
	SUM(icr.email_clicks_7d)   AS email_clicks_7d,
	SUM(icr.email_unsubs_1d)   AS email_unsubs_1d,
	SUM(icr.email_unsubs_7d)   AS email_unsubs_7d,
	SUM(icr.spvs_1d_lc)        AS spvs_1d_lc,
	SUM(icr.spvs_1d_lnd)       AS spvs_1d_lnd,
	SUM(icr.spvs_1d_url)       AS spvs_1d_url,
	SUM(icr.spvs_7d_lc)        AS spvs_7d_lc,
	SUM(icr.spvs_7d_lnd)       AS spvs_7d_lnd,
	SUM(icr.spvs_7d_url)       AS spvs_7d_url,
	SUM(icr.bookings_1d_lc)    AS bookings_1d_lc,
	SUM(icr.bookings_1d_lnd)   AS bookings_1d_lnd,
	SUM(icr.bookings_7d_lc)    AS bookings_7d_lc,
	SUM(icr.bookings_7d_lnd)   AS bookings_7d_lnd,
	SUM(icr.margin_gbp_1d_lc)  AS margin_gbp_1d_lc,
	SUM(icr.margin_gbp_1d_lnd) AS margin_gbp_1d_lnd,
	SUM(icr.margin_gbp_7d_lc)  AS margin_gbp_7d_lc,
	SUM(icr.margin_gbp_7d_lnd) AS margin_gbp_7d_lnd,
FROM data_vault_mvp.dwh.iterable_crm_reporting icr
WHERE icr.send_event_date >= CURRENT_DATE() - 56
GROUP BY 1
;

-- new
SELECT
	icrm.send_event_date,
	SUM(icrm.email_sends)       AS email_sends,
	SUM(icrm.email_opens_1d)    AS email_opens_1d,
	SUM(icrm.email_opens_7d)    AS email_opens_7d,
	SUM(icrm.email_clicks_1d)   AS email_clicks_1d,
	SUM(icrm.email_clicks_7d)   AS email_clicks_7d,
	SUM(icrm.email_unsubs_1d)   AS email_unsubs_1d,
	SUM(icrm.email_unsubs_7d)   AS email_unsubs_7d,
	SUM(icrm.spvs_1d_lc)        AS spvs_1d_lc,
	SUM(icrm.spvs_1d_lnd)       AS spvs_1d_lnd,
	SUM(icrm.spvs_1d_url)       AS spvs_1d_url,
	SUM(icrm.spvs_7d_lc)        AS spvs_7d_lc,
	SUM(icrm.spvs_7d_lnd)       AS spvs_7d_lnd,
	SUM(icrm.spvs_7d_url)       AS spvs_7d_url,
	SUM(icrm.bookings_1d_lc)    AS bookings_1d_lc,
	SUM(icrm.bookings_1d_lnd)   AS bookings_1d_lnd,
	SUM(icrm.bookings_7d_lc)    AS bookings_7d_lc,
	SUM(icrm.bookings_7d_lnd)   AS bookings_7d_lnd,
	SUM(icrm.margin_gbp_1d_lc)  AS margin_gbp_1d_lc,
	SUM(icrm.margin_gbp_1d_lnd) AS margin_gbp_1d_lnd,
	SUM(icrm.margin_gbp_7d_lc)  AS margin_gbp_7d_lc,
	SUM(icrm.margin_gbp_7d_lnd) AS margin_gbp_7d_lnd,
FROM data_vault_mvp.dwh.iterable_crm_reporting__migration icrm
WHERE icrm.send_event_date >= CURRENT_DATE() - 56
GROUP BY 1
;


SELECT
	id
FROM data_vault_mvp.dwh.iterable_crm_reporting__sends icrs sample row (50 rows)
WHERE icrs.send_event_date >= CURRENT_DATE - 28
  AND crm_channel_type = 'email' --email, in-app, app


WITH
	campaign_ids AS (
		SELECT DISTINCT
			icrs.campaign_id,
			crm_channel_type
		FROM data_vault_mvp.dwh.iterable_crm_reporting__sends icrs
		WHERE icrs.send_event_date >= CURRENT_DATE - 28
		  AND crm_channel_type = 'app' --email, in-app, app
	)
SELECT
	campaign_id
FROM campaign_ids SAMPLE ROW (50 ROWS)


-- 50 random email campaigns within last 28 days

/*
CAMPAIGN_ID
'14085813',
'14110376',
'13454903',
'10896174',
'12191055',
'14258357',
'12191054',
'14376079',
'14258317',
'13194511',
'13455033',
'13636335',
'10941752',
'10840228',
'13310044',
'14187293',
'9710874',
'11031039',
'11031034',
'14187318',
'14099767',
'13636354',
'10840431',
'10610498',
'14078916',
'14308011',
'12190783',
'14172522',
'14114473',
'10941775',
'14308055',
'11742964',
'10896261',
'10149040',
'11031961',
'14160759',
'14186853',
'13309850',
'12190179',
'14188926',
'12190800',
'14311864',
'14210608',
'14174981',
'14026246',
'10610500',
'14186852',
'12190784',
'11977333',
'3587008',
*/

-- prod
SELECT
	icr.campaign_id,
	SUM(icr.email_opens_1d),
	SUM(icr.email_opens_7d),
	SUM(icr.email_clicks_1d),
	SUM(icr.email_clicks_7d),
	SUM(icr.email_unsubs_1d),
	SUM(icr.email_unsubs_7d),
	SUM(icr.spvs_1d_lc),
	SUM(icr.spvs_1d_lnd),
	SUM(icr.spvs_1d_url),
	SUM(icr.spvs_7d_lc),
	SUM(icr.spvs_7d_lnd),
	SUM(icr.spvs_7d_url),
	SUM(icr.bookings_1d_lc),
	SUM(icr.bookings_1d_lnd),
	SUM(icr.bookings_7d_lc),
	SUM(icr.bookings_7d_lnd),
	SUM(icr.margin_gbp_1d_lc),
	SUM(icr.margin_gbp_1d_lnd),
	SUM(icr.margin_gbp_7d_lc),
	SUM(icr.margin_gbp_7d_lnd),
FROM data_vault_mvp.dwh.iterable_crm_reporting icr
WHERE icr.campaign_id IN (
						  '14085813',
						  '14110376',
						  '13454903',
						  '10896174',
						  '12191055',
						  '14258357',
						  '12191054',
						  '14376079',
						  '14258317',
						  '13194511',
						  '13455033',
						  '13636335',
						  '10941752',
						  '10840228',
						  '13310044',
						  '14187293',
						  '9710874',
						  '11031039',
						  '11031034',
						  '14187318',
						  '14099767',
						  '13636354',
						  '10840431',
						  '10610498',
						  '14078916',
						  '14308011',
						  '12190783',
						  '14172522',
						  '14114473',
						  '10941775',
						  '14308055',
						  '11742964',
						  '10896261',
						  '10149040',
						  '11031961',
						  '14160759',
						  '14186853',
						  '13309850',
						  '12190179',
						  '14188926',
						  '12190800',
						  '14311864',
						  '14210608',
						  '14174981',
						  '14026246',
						  '10610500',
						  '14186852',
						  '12190784',
						  '11977333',
						  '3587008'
	)
GROUP BY 1
ORDER BY 1
;

-- migration
SELECT
	icrm.campaign_id,
	SUM(icrm.email_opens_1d),
	SUM(icrm.email_opens_7d),
	SUM(icrm.email_clicks_1d),
	SUM(icrm.email_clicks_7d),
	SUM(icrm.email_unsubs_1d),
	SUM(icrm.email_unsubs_7d),
	SUM(icrm.spvs_1d_lc),
	SUM(icrm.spvs_1d_lnd),
	SUM(icrm.spvs_1d_url),
	SUM(icrm.spvs_7d_lc),
	SUM(icrm.spvs_7d_lnd),
	SUM(icrm.spvs_7d_url),
	SUM(icrm.bookings_1d_lc),
	SUM(icrm.bookings_1d_lnd),
	SUM(icrm.bookings_7d_lc),
	SUM(icrm.bookings_7d_lnd),
	SUM(icrm.margin_gbp_1d_lc),
	SUM(icrm.margin_gbp_1d_lnd),
	SUM(icrm.margin_gbp_7d_lc),
	SUM(icrm.margin_gbp_7d_lnd),
FROM data_vault_mvp.dwh.iterable_crm_reporting__migration icrm
WHERE icrm.campaign_id IN (
						   '14085813',
						   '14110376',
						   '13454903',
						   '10896174',
						   '12191055',
						   '14258357',
						   '12191054',
						   '14376079',
						   '14258317',
						   '13194511',
						   '13455033',
						   '13636335',
						   '10941752',
						   '10840228',
						   '13310044',
						   '14187293',
						   '9710874',
						   '11031039',
						   '11031034',
						   '14187318',
						   '14099767',
						   '13636354',
						   '10840431',
						   '10610498',
						   '14078916',
						   '14308011',
						   '12190783',
						   '14172522',
						   '14114473',
						   '10941775',
						   '14308055',
						   '11742964',
						   '10896261',
						   '10149040',
						   '11031961',
						   '14160759',
						   '14186853',
						   '13309850',
						   '12190179',
						   '14188926',
						   '12190800',
						   '14311864',
						   '14210608',
						   '14174981',
						   '14026246',
						   '10610500',
						   '14186852',
						   '12190784',
						   '11977333',
						   '3587008'
	)
GROUP BY 1
ORDER BY 1
;

-- 50 random inapp campaigns within last 28 days

/*
'9645950',
'14326048',
'13809746',
'11521299',
'14091114',
'11603584',
'14239490',
'11521279',
'11521281',
'14142316',
'14135763',
'13607893',
'9576299',
'11009185',
'14341120',
'10459340',
'11521273',
'14100780',
'9387785',
'14028183',
'10622022',
'11009294',
'11521956',
'10459339',
'9645952',
'11009298',
'10459342',
'9645951',
'9645726',
'11521957',
'9372910',
'9576298',
'10459343',
'14187129',
'10459344',
'10459337',
'14350002',
'9576276',
'10622043',
'9387833',
'13809749',
'11521962',
'9539070',
'13607881',
'14343736',
'9539069',
'11521304',
'9645728',
'14135755'
*/


-- prod
SELECT
	icr.campaign_id,
	SUM(icr.email_opens_1d),
	SUM(icr.email_opens_7d),
	SUM(icr.email_clicks_1d),
	SUM(icr.email_clicks_7d),
	SUM(icr.email_unsubs_1d),
	SUM(icr.email_unsubs_7d),
	SUM(icr.spvs_1d_lc),
	SUM(icr.spvs_1d_lnd),
	SUM(icr.spvs_1d_url),
	SUM(icr.spvs_7d_lc),
	SUM(icr.spvs_7d_lnd),
	SUM(icr.spvs_7d_url),
	SUM(icr.bookings_1d_lc),
	SUM(icr.bookings_1d_lnd),
	SUM(icr.bookings_7d_lc),
	SUM(icr.bookings_7d_lnd),
	SUM(icr.margin_gbp_1d_lc),
	SUM(icr.margin_gbp_1d_lnd),
	SUM(icr.margin_gbp_7d_lc),
	SUM(icr.margin_gbp_7d_lnd),
FROM data_vault_mvp.dwh.iterable_crm_reporting icr
WHERE icr.campaign_id IN (
						  '9645950',
						  '14326048',
						  '13809746',
						  '11521299',
						  '14091114',
						  '11603584',
						  '14239490',
						  '11521279',
						  '11521281',
						  '14142316',
						  '14135763',
						  '13607893',
						  '9576299',
						  '11009185',
						  '14341120',
						  '10459340',
						  '11521273',
						  '14100780',
						  '9387785',
						  '14028183',
						  '10622022',
						  '11009294',
						  '11521956',
						  '10459339',
						  '9645952',
						  '11009298',
						  '10459342',
						  '9645951',
						  '9645726',
						  '11521957',
						  '9372910',
						  '9576298',
						  '10459343',
						  '14187129',
						  '10459344',
						  '10459337',
						  '14350002',
						  '9576276',
						  '10622043',
						  '9387833',
						  '13809749',
						  '11521962',
						  '9539070',
						  '13607881',
						  '14343736',
						  '9539069',
						  '11521304',
						  '9645728',
						  '14135755'
	)
GROUP BY 1
ORDER BY 1
;

-- migration
SELECT
	icrm.campaign_id,
	SUM(icrm.email_opens_1d),
	SUM(icrm.email_opens_7d),
	SUM(icrm.email_clicks_1d),
	SUM(icrm.email_clicks_7d),
	SUM(icrm.email_unsubs_1d),
	SUM(icrm.email_unsubs_7d),
	SUM(icrm.spvs_1d_lc),
	SUM(icrm.spvs_1d_lnd),
	SUM(icrm.spvs_1d_url),
	SUM(icrm.spvs_7d_lc),
	SUM(icrm.spvs_7d_lnd),
	SUM(icrm.spvs_7d_url),
	SUM(icrm.bookings_1d_lc),
	SUM(icrm.bookings_1d_lnd),
	SUM(icrm.bookings_7d_lc),
	SUM(icrm.bookings_7d_lnd),
	SUM(icrm.margin_gbp_1d_lc),
	SUM(icrm.margin_gbp_1d_lnd),
	SUM(icrm.margin_gbp_7d_lc),
	SUM(icrm.margin_gbp_7d_lnd),
FROM data_vault_mvp.dwh.iterable_crm_reporting__migration icrm
WHERE icrm.campaign_id IN (
						   '9645950',
						   '14326048',
						   '13809746',
						   '11521299',
						   '14091114',
						   '11603584',
						   '14239490',
						   '11521279',
						   '11521281',
						   '14142316',
						   '14135763',
						   '13607893',
						   '9576299',
						   '11009185',
						   '14341120',
						   '10459340',
						   '11521273',
						   '14100780',
						   '9387785',
						   '14028183',
						   '10622022',
						   '11009294',
						   '11521956',
						   '10459339',
						   '9645952',
						   '11009298',
						   '10459342',
						   '9645951',
						   '9645726',
						   '11521957',
						   '9372910',
						   '9576298',
						   '10459343',
						   '14187129',
						   '10459344',
						   '10459337',
						   '14350002',
						   '9576276',
						   '10622043',
						   '9387833',
						   '13809749',
						   '11521962',
						   '9539070',
						   '13607881',
						   '14343736',
						   '9539069',
						   '11521304',
						   '9645728',
						   '14135755'
	)
GROUP BY 1
ORDER BY 1
;


-- 50 random app push campaigns in the last 28 days

/*
'14142214',
'14252859',
'8812690',
'14187126',
'7080511',
'14326058',
'14252923',
'11162835',
'8812205',
'14239463',
'8812701',
'11162837',
'14326683',
'8812206',
'9067307',
'13611160',
'8812705',
'13611162',
'13611054',
'8812166',
'14290543',
'14100782',
'14147515',
'14326690',
'13611133',
'6052158',
'14325451',
'7080543',
'8812165',
'14349976',
'14091108',
'8812156',
'14028180',
'11162840',
'6052078',
'8811956',
'9641289',
'14341108',
'8809616',
'14135752',
'8812202',
'14227743',
'14350710',
'14159418',
'8812094',
'13611185',
'9641262',
'9641254',
'13611145',
'7080502'
*/

-- prod
SELECT
	icr.campaign_id,
	SUM(icr.email_opens_1d),
	SUM(icr.email_opens_7d),
	SUM(icr.email_clicks_1d),
	SUM(icr.email_clicks_7d),
	SUM(icr.email_unsubs_1d),
	SUM(icr.email_unsubs_7d),
	SUM(icr.spvs_1d_lc),
	SUM(icr.spvs_1d_lnd),
	SUM(icr.spvs_1d_url),
	SUM(icr.spvs_7d_lc),
	SUM(icr.spvs_7d_lnd),
	SUM(icr.spvs_7d_url),
	SUM(icr.bookings_1d_lc),
	SUM(icr.bookings_1d_lnd),
	SUM(icr.bookings_7d_lc),
	SUM(icr.bookings_7d_lnd),
	SUM(icr.margin_gbp_1d_lc),
	SUM(icr.margin_gbp_1d_lnd),
	SUM(icr.margin_gbp_7d_lc),
	SUM(icr.margin_gbp_7d_lnd),
FROM data_vault_mvp.dwh.iterable_crm_reporting icr
WHERE icr.campaign_id IN (
						  '14142214',
						  '14252859',
						  '8812690',
						  '14187126',
						  '7080511',
						  '14326058',
						  '14252923',
						  '11162835',
						  '8812205',
						  '14239463',
						  '8812701',
						  '11162837',
						  '14326683',
						  '8812206',
						  '9067307',
						  '13611160',
						  '8812705',
						  '13611162',
						  '13611054',
						  '8812166',
						  '14290543',
						  '14100782',
						  '14147515',
						  '14326690',
						  '13611133',
						  '6052158',
						  '14325451',
						  '7080543',
						  '8812165',
						  '14349976',
						  '14091108',
						  '8812156',
						  '14028180',
						  '11162840',
						  '6052078',
						  '8811956',
						  '9641289',
						  '14341108',
						  '8809616',
						  '14135752',
						  '8812202',
						  '14227743',
						  '14350710',
						  '14159418',
						  '8812094',
						  '13611185',
						  '9641262',
						  '9641254',
						  '13611145',
						  '7080502'
	)
GROUP BY 1
ORDER BY 1
;

-- migration
SELECT
	icrm.campaign_id,
	SUM(icrm.email_opens_1d),
	SUM(icrm.email_opens_7d),
	SUM(icrm.email_clicks_1d),
	SUM(icrm.email_clicks_7d),
	SUM(icrm.email_unsubs_1d),
	SUM(icrm.email_unsubs_7d),
	SUM(icrm.spvs_1d_lc),
	SUM(icrm.spvs_1d_lnd),
	SUM(icrm.spvs_1d_url),
	SUM(icrm.spvs_7d_lc),
	SUM(icrm.spvs_7d_lnd),
	SUM(icrm.spvs_7d_url),
	SUM(icrm.bookings_1d_lc),
	SUM(icrm.bookings_1d_lnd),
	SUM(icrm.bookings_7d_lc),
	SUM(icrm.bookings_7d_lnd),
	SUM(icrm.margin_gbp_1d_lc),
	SUM(icrm.margin_gbp_1d_lnd),
	SUM(icrm.margin_gbp_7d_lc),
	SUM(icrm.margin_gbp_7d_lnd),
FROM data_vault_mvp.dwh.iterable_crm_reporting__migration icrm
WHERE icrm.campaign_id IN (
						   '14142214',
						   '14252859',
						   '8812690',
						   '14187126',
						   '7080511',
						   '14326058',
						   '14252923',
						   '11162835',
						   '8812205',
						   '14239463',
						   '8812701',
						   '11162837',
						   '14326683',
						   '8812206',
						   '9067307',
						   '13611160',
						   '8812705',
						   '13611162',
						   '13611054',
						   '8812166',
						   '14290543',
						   '14100782',
						   '14147515',
						   '14326690',
						   '13611133',
						   '6052158',
						   '14325451',
						   '7080543',
						   '8812165',
						   '14349976',
						   '14091108',
						   '8812156',
						   '14028180',
						   '11162840',
						   '6052078',
						   '8811956',
						   '9641289',
						   '14341108',
						   '8809616',
						   '14135752',
						   '8812202',
						   '14227743',
						   '14350710',
						   '14159418',
						   '8812094',
						   '13611185',
						   '9641262',
						   '9641254',
						   '13611145',
						   '7080502'
	)
GROUP BY 1
ORDER BY 1
;
