-- look at sessions that start with oauth

USE WAREHOUSE pipe_xlarge
;

SELECT
	stba.touch_start_tstamp,
	stba.stitched_identity_type,
	stba.attributed_user_id,
	stba.touch_landing_page,
	stmc.referrer_hostname,
	stmc.referrer_medium,
	stmc.touch_mkt_channel,
	stba.touch_referrer_url
FROM se.data_pii.scv_touch_basic_attributes stba
INNER JOIN se.data.scv_touch_marketing_channel stmc
	ON stba.touch_id = stmc.touch_id
WHERE stba.touch_se_brand = 'Travelist'
  AND stba.touch_start_tstamp >= '2025-11-01'
  AND stba.touch_landing_page LIKE '%oauth%'

-- 0


SELECT
	stba.touch_start_tstamp,
	stba.stitched_identity_type,
	stba.attributed_user_id,
	stba.touch_landing_page,
	stmc.referrer_hostname,
	stmc.referrer_medium,
	stba.touch_referrer_url,
	stmc.touch_mkt_channel,
	stba.touch_referrer_url,
	stba.touch_has_booking
FROM se.data_pii.scv_touch_basic_attributes stba
INNER JOIN se.data.scv_touch_marketing_channel stmc
	ON stba.touch_id = stmc.touch_id
WHERE stba.touch_se_brand = 'Travelist'
  AND stba.touch_start_tstamp >= '2025-11-01'
  AND stba.touch_landing_page LIKE '%oauth%'
  AND stba.touch_has_booking

-- 55 of the 6.7K have a booking


WITH
	oauth_landing_sessions AS (
		SELECT
			stba.touch_start_tstamp,
			stba.stitched_identity_type,
			stba.attributed_user_id,
			stba.touch_landing_page,
			stmc.referrer_hostname,
			stmc.referrer_medium,
			stba.touch_referrer_url,
			stmc.touch_mkt_channel,
			stba.touch_referrer_url,
			stba.touch_has_booking
		FROM se.data_pii.scv_touch_basic_attributes stba
		INNER JOIN se.data.scv_touch_marketing_channel stmc
			ON stba.touch_id = stmc.touch_id
		WHERE stba.touch_se_brand = 'Travelist'
		  AND stba.touch_start_tstamp >= '2025-01-01'
		  AND stba.touch_landing_page LIKE '%oauth%'
		  AND stba.touch_has_booking
	)
SELECT
	oauth_landing_sessions.referrer_hostname,
	COUNT(*)
FROM oauth_landing_sessions
GROUP BY 1
;


------------------------------------------------------------------------------------------------------------------------

USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_unique_urls
	CLONE data_vault_mvp.single_customer_view_stg.module_unique_urls
;

-- optional statement to create the module target table --

-- clone prod scv tables
DROP SCHEMA data_vault_mvp_dev_robin.single_customer_view_stg
;

CREATE OR REPLACE SCHEMA data_vault_mvp_dev_robin.single_customer_view_stg CLONE data_vault_mvp.single_customer_view_stg
;

-- alter the prod table name -- will rerun module on empty table
ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_url_hostname
	RENAME TO data_vault_mvp_dev_robin.single_customer_view_stg.module_url_hostname__prod
;

/*
rerun 02_01_module_url_hostname on empty table since begining of scv
self_describing_task --include 'biapp/task_catalogue/dv/dwh/scv/01_url_manipulation/02_01_module_url_hostname.py'  --method 'run' --start '2022-12-01 00:00:00' --end '2022-12-01 00:00:00'
*/

-- sense check data
SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_url_hostname
;

-- check that accounts.google medium has changed
SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_url_hostname
WHERE module_url_hostname.url_medium != 'oauth'
  AND module_url_hostname.url_hostname LIKE 'accounts.google.%'
;

-- row counts
SELECT
	COUNT(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_url_hostname
;

SELECT
	COUNT(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_url_hostname__prod
;


/*
rerun 02_01_utm_or_referrer_hostname_marker since beginning of scv
self_describing_task --include 'biapp/task_catalogue/dv/dwh/scv/03_touchification/02_01_utm_or_referrer_hostname_marker.py'  --method 'run' --start '2022-12-01 00:00:00' --end '2022-12-01 00:00:00'
table is regen
*/

/*
rerun 03_touchification since beginning of scv
self_describing_task --include 'biapp/task_catalogue/dv/dwh/scv/03_touchification/03_touchification.py'  --method 'run' --start '2022-12-01 00:00:00' --end '2022-12-01 00:00:00'
*/

SELECT
	COUNT(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
; -- 2,999,904,452
SELECT
	COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touchification
; -- 2,999,904,452

SELECT
	COUNT(DISTINCT touch_id)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
; -- 530,128,155
SELECT
	COUNT(DISTINCT touch_id)
FROM data_vault_mvp.single_customer_view_stg.module_touchification
;

-- 530,586,652

-- trying tro replicate the utm referrer logic to test facebook exclusion
SELECT
	e.event_hash,
	e.event_tstamp,
	e.derived_tstamp,
	i.attributed_user_id,
	i.stitched_identity_type,
	e.page_url,
	e.page_referrer,
	SHA2(NULLIF(COALESCE(p.utm_campaign, '') ||
				COALESCE(p.utm_medium, '') ||
				COALESCE(p.utm_source, '') ||
				COALESCE(p.utm_term, '') ||
				COALESCE(p.utm_content, '') ||
				COALESCE(p.click_id, '') ||
				COALESCE(r.url_hostname, ''), '')) AS partition_marker

FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchifiable_events e
INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_identity_stitching i
	ON COALESCE(e.unique_browser_id, e.cookie_id, e.idfv, e.session_userid) =
	   COALESCE(i.unique_browser_id, i.cookie_id, i.idfv, i.session_userid)
LEFT JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_extracted_params p
	ON e.page_url = p.url
	-- join referrer hostnames but exclude internal, oauth and payment gateway referrers so they aren't considered as a utm marker.
LEFT JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_url_hostname r
	ON e.page_referrer = r.url
	AND r.url_medium NOT IN ('internal', 'payment_gateway', 'oauth')
WHERE e.event_tstamp >= CURRENT_DATE - 1
  AND i.stitched_identity_type = 'tvl_user_id'
--   AND e.page_referrer LIKE '%facebook%'
--   AND e.page_url LIKE '%oauth%'
  AND i.attributed_user_id = '3385226'
;


WITH
	utm_referrer_marker AS
		(
			SELECT
				e.event_hash,
				e.event_tstamp,
				e.derived_tstamp,
				i.attributed_user_id,
				i.stitched_identity_type,
				e.page_url,
				e.page_referrer,
				PARSE_URL(e.page_url, 1)['host'],
				IFF(
						e.page_referrer = 'https://www.facebook.com/'
							AND PARSE_URL(e.page_url, 1)['host']::VARCHAR LIKE 'travelist.%',
						NULL,
						r.url_hostname
				)                                        AS url_host,
				SHA2(NULLIF(COALESCE(p.utm_campaign, '') ||
							COALESCE(p.utm_medium, '') ||
							COALESCE(p.utm_source, '') ||
							COALESCE(p.utm_term, '') ||
							COALESCE(p.utm_content, '') ||
							COALESCE(p.click_id, '') ||
							COALESCE(url_host, ''), '')) AS partition_marker

			FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchifiable_events e
			INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_identity_stitching i
				ON COALESCE(e.unique_browser_id, e.cookie_id, e.idfv, e.session_userid) =
				   COALESCE(i.unique_browser_id, i.cookie_id, i.idfv, i.session_userid)
			LEFT JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_extracted_params p
				ON e.page_url = p.url
				-- join referrer hostnames but exclude internal, oauth and payment gateway referrers so they aren't considered as a utm marker.
			LEFT JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_url_hostname r
				ON e.page_referrer = r.url
				AND r.url_medium NOT IN ('internal', 'payment_gateway', 'oauth')
			WHERE e.event_tstamp >= '2025-11-10'
			  AND i.stitched_identity_type = 'tvl_user_id'
--   AND e.page_referrer LIKE '%facebook%'
--   AND e.page_url LIKE '%oauth%'
			  AND i.attributed_user_id = '2439876'
		),

	partition_grouping AS (
		--persist the partition marker until the next partition marker
			SELECT
				u.event_hash,
				u.event_tstamp,
				u.attributed_user_id,
				u.stitched_identity_type,
				CASE
					WHEN LAST_VALUE(u.partition_marker)
									IGNORE NULLS OVER (PARTITION BY u.attributed_user_id, u.stitched_identity_type
										ORDER BY u.event_tstamp, u.derived_tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) IS NULL
						THEN 'first_group' -- when there are no unique utm params or referrer hostname for the first set of events
					ELSE LAST_VALUE(u.partition_marker)
									IGNORE NULLS OVER (PARTITION BY u.attributed_user_id, u.stitched_identity_type
										ORDER BY u.event_tstamp, u.derived_tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
				END AS partition_group
			FROM utm_referrer_marker u
		)
SELECT *
FROM partition_grouping
;

-- getting example for 2439876 in event stream so can share with piotr
SELECT
	ses.event_tstamp,
	ses.unique_browser_id,
	ses.user_id,
	ses.page_url,
	ses.page_referrer,
	ses.event_name
FROM se.data_pii.scv_event_stream ses
-- WHERE ses.user_id = '2439876'
WHERE ses.unique_browser_id = 'bb9c0674-ea94-41cc-bd01-ccb15fd84883'
  AND ses.event_tstamp::DATE = '2025-11-10'
  AND ses.se_brand = 'Travelist'
ORDER BY ses.event_tstamp
;

USE WAREHOUSE pipe_xlarge
;

SELECT *
FROM se.data_pii.scv_touch_basic_attributes stba
INNER JOIN se.data.scv_touch_marketing_channel stmc
	ON stba.touch_id = stmc.touch_id
	AND stmc.touch_start_tstamp >= '2025-11-01'
WHERE stba.touch_se_brand = 'Travelist'
  AND stba.touch_start_tstamp >= '2025-11-01'
  AND stmc.referrer_hostname = 'www.facebook.com'
;


SELECT
	PARSE_URL(column1),
	PARSE_URL(column1)['parameters'],
	PARSE_URL(column1)['parameters']['utm_campaign'] AS utm_campaign,
	PARSE_URL(column1)['parameters']['utm_medium']   AS utm_medium,
	PARSE_URL(column1)['parameters']['utm_source']   AS utm_source,
	PARSE_URL(column1)['parameters']['utm_term']     AS utm_term,
	PARSE_URL(column1)['parameters']['utm_content']  AS utm_content,
	PARSE_URL(column1)['parameters']['fbclid']       AS fbclid,
FROM
VALUES ('https://www.facebook.com?hello=test'),
	   ('https://travelist.pl/130007/polska-szczyrk-mercure-szczyrk-resort/?utm_campaign=prospecting&utm_source=facebook&utm_medium=paid-social&utm_term=prospecting_broadmixaplus_mix__fb-traffic-aplus-mix&utm_content=1200x1200_130007-mercureszczyrk-bw-112025_page-post-link-ad_statyczna_TRA_PL_PSO_FBO_0000_PRO_2108_STA_PPL_0_MIX_4786_285_20250915&MWID=TRA_PL_PSO_FBO_0000_PRO_2108_STA_PPL_0_MIX_4786_285_20250915&fbclid=IwY2xjawN-mVtleHRuA2FlbQEwAGFkaWQBqyoBn46CDHNydGMGYXBwX2lkDzQwOTk2MjYyMzA4NTYwOQABHhazGeFA3zXDJTxyaLKU9Gpk-fGtIdS7vlW1fo8TFZkSmJWX2a0V45wVGzq7_aem_FXwbvAfi15uLXxSXDkkP4A&utm_id=120236000729610748'),
	   ('https://travelist.pl/hotele/czechy?utm_campaign=brak&utm_source=facebook_facebook&utm_medium=organic-social&utm_term=brak_mix_zagranica___&utm_content=kanal_czechy-kanal_inspiracje_foto_TRA_PL_OSO_FBO_FBOO_NON_MIXX_PHO_INS_0_S51_T487_CHN_20251020&MWID=TRA_PL_OSO_FBO_FBOO_NON_MIXX_PHO_INS_0_S51_T487_CHN_20251020&fbclid=IwZXh0bgNhZW0BMABhZGlkAaspQ4gPb5xzcnRjBmFwcF9pZAwzNTA2ODU1MzE3MjgAAR6NXG0AQe_2hSz9a_gY4QzPdqP4OhlDJhWukCErQ1fsRObIw-dIyNT2CjwNRA_aem_nRjU8CbtW2DLWjLCxvcF_g'),
	   ('https://travelist.pl/130007/polska-szczyrk-mercure-szczyrk-resort/?utm_campaign=prospecting&utm_source=facebook&utm_medium=paid-social&utm_term=prospecting_broad-cold_mix__fb-traffic&utm_content=1200x1200_130007-mercureszczyrkresort-ongoing-102025_page-post-link-ad_statyczna_TRA_PL_PSO_FBO_0000_PRO_5079_STA_PPL_0_MIX_4738_285_20220322&MWID=TRA_PL_PSO_FBO_0000_PRO_5079_STA_PPL_0_MIX_4738_285_20220322&fbclid=IwZXh0bgNhZW0BMABhZGlkAaspRIxK2ewBHjxKs_yq-dxCGY6fNnopGQbyU19xKJJ0M9MDzC5IxLEPywCAs125cx8BKiSl_aem_GxQd0MimXYd65qOmG9JxlQ&utm_id=23852676473270747'),
	   ('https://travelist.pl/magazyn/top-10-najlepszych-polskich-uzdrowisk-na-jesienna-regeneracje/?utm_campaign=brak&utm_source=facebook_facebook&utm_medium=organic-social&utm_term=brak_inspiracja_ranking-uzdrowisk-2025___&utm_content=magazyn_ranking-uzdrowisk-2025-mag-wyniki_akcja-specjalna_foto_TRA_PL_OSO_FBO_FBOO_NON_INSP_PHO_ASP_0_U98_T695_MAG_20251013&MWID=TRA_PL_OSO_FBO_FBOO_NON_INSP_PHO_ASP_0_U98_T695_MAG_20251013&fbclid=IwZXh0bgNhZW0BMABhZGlkAaso4W4is7xzcnRjBmFwcF9pZAwzNTA2ODU1MzE3MjgAAR4-0oJtLqk7ECnzYjAX824uHxtTjpX2kDqs2VL170M4InxXxeNcfO-Z1F9wQQ_aem_WUMpG2wO9G8PAPQ2lCX0bw'),
	   ('https://travelist.pl/130007/polska-szczyrk-mercure-szczyrk-resort/?utm_campaign=prospecting&utm_source=facebook&utm_medium=paid-social&utm_term=prospecting_broadmixaplus_mix__fb-traffic-aplus-mix&utm_content=1200x1200_130007-mercureszczyrk-bw-112025_page-post-link-ad_statyczna_TRA_PL_PSO_FBO_0000_PRO_2108_STA_PPL_0_MIX_4786_285_20250915&MWID=TRA_PL_PSO_FBO_0000_PRO_2108_STA_PPL_0_MIX_4786_285_20250915&fbclid=IwZXh0bgNhZW0BMABhZGlkAasqAZ-OggxzcnRjBmFwcF9pZAo2NjI4NTY4Mzc5AAEe4nSiazQhIfJWE5XgTDATkvQBLE_HA7hTZsLQlYXmAIidlRatf41e1H8nI04_aem_izhjG4mcxB5VE-7YrHUlvg&utm_id=120236000729610748'),
	   ('https://travelist.pl/117497/wybrzeze-chalupy-hotel-meridian/?utm_campaign=prospecting-lead&utm_source=facebook&utm_medium=paid-social&utm_term=prospecting-lead_laluzytkownicybudzetowilead_mix__fb-prospecting-lead&utm_content=1200x1200_117497-hotelmeridianandspa-ongoing-102025_page-post-link-ad_statyczna_TRA_PL_PSO_FBO_0000_PRL_6023_STA_PPL_0_MIX_4680_285_20241014&MWID=TRA_PL_PSO_FBO_0000_PRL_6023_STA_PPL_0_MIX_4680_285_20241014&fbclid=IwZXh0bgNhZW0BMABhZGlkAasonAjIjoxzcnRjBmFwcF9pZAwzNTA2ODU1MzE3MjgAAR7wh4J5AmUcJ8TshNKcl5nQToZh82aW0z4BqZ_SdUB3afojVxfzBgH2uurK-g_aem_aCIUpR6oBWOUXC6vdhh-2w&utm_id=120213617424740748'),
	   ('https://travelist.pl/117930/polska-beskid-slaski-wisla-crystal-mountain/?utm_campaign=brak&utm_source=facebook_instagram&utm_medium=organic-social&utm_term=brak_sprzedaz_gorskie-resorty___&utm_content=oferta_crystal-mountain-_hotel_video_TRA_PL_OSO_FBO_INST_NON_SPRZ_VID_HOT_0_U102_S564_OFR_20251105&MWID=TRA_PL_OSO_FBO_INST_NON_SPRZ_VID_HOT_0_U102_S564_OFR_20251105&fbclid=IwZXh0bgNhZW0BMABhZGlkAasqCjXILKxzcnRjBmFwcF9pZAwzNTA2ODU1MzE3MjgAAR6pkMglpNMSsBnvgUQbDf3YX480ZoSacHrrMiDvKFS64q8S0rV7hXsV1MUY6Q_aem_IE8Y-UHKnQcMpRLmsCG_Hg&utm_id=120207590387490748'),
	   ('https://travelist.pl/hotele?oauth=LEAD,facebook,lead_form#_=_'),
	   ('https://travelist.pl/odkryj/black-month?utm_campaign=brak&utm_source=facebook_instagram&utm_medium=organic-social&utm_term=brak_sprzedaz_black-month-2025___&utm_content=kanal_black-month-2025-kanal-glowny_akcja-specjalna_video_TRA_PL_OSO_FBO_INST_NON_SPRZ_VID_ASP_0_U104_T701_CHN_20251105&MWID=TRA_PL_OSO_FBO_INST_NON_SPRZ_VID_ASP_0_U104_T701_CHN_20251105&fbclid=IwZXh0bgNhZW0BMABhZGlkAasqAIoCahxzcnRjBmFwcF9pZAwzNTA2ODU1MzE3MjgAAR6Hc9-hlOucluWnGUunW4r8JfK_4Ow5oLR8DWcq81uViJnuEJka_cK30hh9AA_aem_5xUL9QZarJgDLaLr0fedyg&utm_id=120207590387490748'),
	   ('https://travelist.pl/130007/polska-szczyrk-mercure-szczyrk-resort/?MWID=TRA_PL_OSO_FBO_FBOO_NON_SPRZ_PHO_HIN_0_S89_T489_OFR_20251104&fbclid=IwZXh0bgNhZW0BMABhZGlkAasqAMsSJUxzcnRjBmFwcF9pZAo2NjI4NTY4Mzc5AAEeNwncw8bM4YHsi1RLpUkPkYdiMXDBq83t3CLLkgtLqerVWq7Ur0ci_WhVyiI_aem_9leWBzd9upClIdxGYqZJTA&utm_campaign=brak&utm_content=oferta_mercure-szczyrk-resort_hotel-ind_foto_TRA_PL_OSO_FBO_FBOO_NON_SPRZ_PHO_HIN_0_S89_T489_OFR_20251104&utm_medium=organic-social&utm_source=facebook_facebook&utm_term=brak_sprzedaz_ind___'),
	   ('https://travelist.pl/odkryj/black-month?utm_campaign=brak&utm_source=facebook_instagram&utm_medium=organic-social&utm_term=brak_sprzedaz_black-month-2025___&utm_content=kanal_black-month-2025-kanal-glowny_akcja-specjalna_video_TRA_PL_OSO_FBO_INST_NON_SPRZ_VID_ASP_0_U104_T701_CHN_20251105&MWID=TRA_PL_OSO_FBO_INST_NON_SPRZ_VID_ASP_0_U104_T701_CHN_20251105&fbclid=IwZXh0bgNhZW0BMABhZGlkAasqAIoCahxzcnRjBmFwcF9pZAwzNTA2ODU1MzE3MjgAAR7OU6DFIdt87bV-nfhr7QgfCijUjv-x4NIV6KWOXKBWeuUadiGCYSe2NKukhw_aem_jjox2WJs3KCRm7LQgmFVHA&utm_id=120207590387490748'),
	   ('https://travelist.pl/magazyn/top-10-najlepszych-polskich-uzdrowisk-na-jesienna-regeneracje/?utm_campaign=brak&utm_source=facebook_facebook&utm_medium=organic-social&utm_term=brak_inspiracja_ranking-uzdrowisk-2025___&utm_content=magazyn_ranking-uzdrowisk-2025-mag-wyniki_akcja-specjalna_foto_TRA_PL_OSO_FBO_FBOO_NON_INSP_PHO_ASP_0_U98_T695_MAG_20251013&MWID=TRA_PL_OSO_FBO_FBOO_NON_INSP_PHO_ASP_0_U98_T695_MAG_20251013&fbclid=IwY2xjawN8gmdleHRuA2FlbQIxMABicmlkETE5MDFtbDMxVWNLTGEzaVM1c3J0YwZhcHBfaWQQMjIyMDM5MTc4ODIwMDg5MgABHqMCL8uVCMbEot7QnYMyLQXyt5xyoghvJddj7NI1Wzmbis3jQmOvtHe90aZj_aem_b6AJUmRlp_ejMOn7yIvRiw'),
	   ('https://travelist.pl/130007/polska-szczyrk-mercure-szczyrk-resort/?utm_campaign=prospecting&utm_source=facebook&utm_medium=paid-social&utm_term=prospecting_broadmixaplus_mix__fb-traffic-aplus-mix&utm_content=1200x1200_130007-mercureszczyrk-bw-112025_page-post-link-ad_statyczna_TRA_PL_PSO_FBO_0000_PRO_2108_STA_PPL_0_MIX_4786_285_20250915&MWID=TRA_PL_PSO_FBO_0000_PRO_2108_STA_PPL_0_MIX_4786_285_20250915&fbclid=IwY2xjawN8HIdleHRuA2FlbQEwAGFkaWQBqyoBn46CDHNydGMGYXBwX2lkDzQwOTk2MjYyMzA4NTYwOQABHgbQmskFGbPczoR56B9Ts204Ui-JKUBI9LIeUjkAEJKl2glP__xrE02W3VIu_aem_SBpAdy-mJObDqZyVPb6mmQ&utm_id=120236000729610748'),
	   ('https://travelist.pl/130007/polska-szczyrk-mercure-szczyrk-resort/?MWID=TRA_PL_OSO_FBO_FBOO_NON_SPRZ_PHO_HIN_0_S89_T489_OFR_20251104&fbclid=IwZXh0bgNhZW0BMABhZGlkAasqAMsSJUxzcnRjBmFwcF9pZAwzNTA2ODU1MzE3MjgAAR4Fihf_rPgS0bjDTqDClMXvn9C9amaxm5eCArSHAcgovEqCI0XuE4c8_tJazw_aem_1ptMqyr4LTFlyAPs1YVuYQ&utm_campaign=brak&utm_content=oferta_mercure-szczyrk-resort_hotel-ind_foto_TRA_PL_OSO_FBO_FBOO_NON_SPRZ_PHO_HIN_0_S89_T489_OFR_20251104&utm_medium=organic-social&utm_source=facebook_facebook&utm_term=brak_sprzedaz_ind___'),
	   ('https://travelist.pl/magazyn/top-10-najlepszych-polskich-uzdrowisk-na-jesienna-regeneracje/?utm_campaign=brak&utm_source=facebook_facebook&utm_medium=organic-social&utm_term=brak_inspiracja_ranking-uzdrowisk-2025___&utm_content=magazyn_ranking-uzdrowisk-2025-mag-wyniki_akcja-specjalna_foto_TRA_PL_OSO_FBO_FBOO_NON_INSP_PHO_ASP_0_U98_T695_MAG_20251013&MWID=TRA_PL_OSO_FBO_FBOO_NON_INSP_PHO_ASP_0_U98_T695_MAG_20251013&fbclid=IwZXh0bgNhZW0BMABhZGlkAaso4W4is7xzcnRjBmFwcF9pZAwzNTA2ODU1MzE3MjgAAR5uPqCZmdlKj44uq7TO93xJfn8ZJKaRtXTUdi1EBGEFhKItvGd3YKloW4-NkQ_aem_BmYeQ5cCAeq_ByUobm24og'),
	   ('https://travelist.pl/130007/polska-szczyrk-mercure-szczyrk-resort/?MWID=TRA_PL_OSO_FBO_FBOO_NON_SPRZ_PHO_HIN_0_S89_T489_OFR_20251104&fbclid=IwZXh0bgNhZW0BMABhZGlkAasqAMsSJUxzcnRjBmFwcF9pZAwzNTA2ODU1MzE3MjgAAR6zqFJBNJM16okFeRxGLncaGNbJMvyRCGXGUWFCX0HW4qzbmygAHp9-RDeoeg_aem_KrKwWhoV-2nK3IPq5fzQLw&utm_campaign=brak&utm_content=oferta_mercure-szczyrk-resort_hotel-ind_foto_TRA_PL_OSO_FBO_FBOO_NON_SPRZ_PHO_HIN_0_S89_T489_OFR_20251104&utm_medium=organic-social&utm_source=facebook_facebook&utm_term=brak_sprzedaz_ind___'),
	   ('https://travelist.pl/odkryj/black-month?utm_campaign=brak&utm_source=facebook_instagram&utm_medium=organic-social&utm_term=brak_sprzedaz_black-month-2025___&utm_content=kanal_black-month-2025-kanal-glowny_akcja-specjalna_video_TRA_PL_OSO_FBO_INST_NON_SPRZ_VID_ASP_0_U104_T701_CHN_20251105&MWID=TRA_PL_OSO_FBO_INST_NON_SPRZ_VID_ASP_0_U104_T701_CHN_20251105&fbclid=IwZXh0bgNhZW0BMABhZGlkAasqAIoCahxzcnRjBmFwcF9pZAwzNTA2ODU1MzE3MjgAAR4mToQO5Hgaq5UjWUJ5hSz-JDZeemorOOqrpuEkAEvE3dB5eWf6CZ_aoI0Sqw_aem_1IF3MWd4XcvCzSY9Dyk3VA&utm_id=120207590387490748'),
	   ('https://travelist.pl/hotele?MWID=TRA_PL_PSO_FBO_0000_REM_5037_DYN_CAR_0_MIX_0010_285_20240702_salelipi&b=001&h=001_2001&noleadrequired=on&utm_campaign=remarketing&utm_content=1200x1200_dynamiczna_karuzela_dynamiczna_TRA_PL_PSO_FBO_0000_REM_5037_DYN_CAR_0_MIX_0010_285_20240702_salelipi&utm_medium=paid-social&utm_source=facebook&utm_term=remarketing_dta-standard_mix_salelipiec_fb-rem&x003=')


-- utm_campaign, utm_medium, utm_source, utm_term, utm_content, click_id;

;

SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_utm_referrer_marker__step01__pre_parse_data
;

USE WAREHOUSE pipe_xlarge
;

SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_utm_referrer_marker__step02__utm_referrer_marker
WHERE event_tstamp::DATE = '2025-11-10'
  AND attributed_user_id = '2439876'


SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_utm_referrer_marker__step03__persist_partition_marker
WHERE event_tstamp::DATE = '2025-11-10'
  AND attributed_user_id = '2439876'
  AND stitched_identity_type = 'tvl_user_id'



SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_utm_referrer_marker
WHERE event_hash IN (
					 '5cf4ac3e49c4d2bd708c298bd8a7e9bc25adff9b474bf4ecb2cfd16942ea7958',
					 '9ebf71bd722407043f51139ac996ea1c7e34e1d1befe43162417e8d37b01c6fc',
					 'e7611bed25b76cd890d1ce5a1aa14992d6de021c94b27e15778de983268ec532',
					 '122108eaf97b6cb695032820a79651994845a6a2c861faa9faea23d19a8984ed',
					 'aafd89ca76af454623224fd5a530ff59e442ef28ebd4f491e4ae3d23f6519c65',
					 'd6e780ca2ece4ff7488f663727e274c80d08df6484bff9191e74f8bd64060489',
					 '200d58c7fba062fdfece30ff4693838f90a15d683b5465bff830e74973052dd2',
					 'ddd881dae1763add9fe1fd68adedafab9b81b5d10a3bec40f2a41f3f4b380f89',
					 '1390c4cd21574bb98ca0f6812e9cda7e3dde80ccd9e1f23de6166843c3dc626e',
					 'e831bbb0b5597f91bcabc07a629771b5ffee3df71b01fe9d46ecdc4dcee1fc48',
					 'd5bac4f6192c73e08ac45fb7091e15176489dfb8b38bcbc5cf6204ff5d33c42f',
					 'd4d6138ed65ea269b196e9388fa15406f3b0ad8eaeeaac68fa61765a9d59b9cf',
					 '5b70a465fad5af9e58192f17b0e630f72c3cdf3ccd2d85b1e7b999b21e421d93',
					 'd03dbfd355d431f0c1f3814bfea61bc8e16693e02dbbca9a9c1c4c8ba9269273',
					 'ad31a150588f858e1dab23df552d6a29ece79bc2a49872a574dafd5f5be0fe11',
					 '633af268d59dea0aa02b70defe6794c0e41e682f9995d1dd0e81b7f9d0b5e041'
	)
;


SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_utm_referrer_marker
WHERE event_hash IN (
					 '5cf4ac3e49c4d2bd708c298bd8a7e9bc25adff9b474bf4ecb2cfd16942ea7958',
					 '9ebf71bd722407043f51139ac996ea1c7e34e1d1befe43162417e8d37b01c6fc',
					 'e7611bed25b76cd890d1ce5a1aa14992d6de021c94b27e15778de983268ec532',
					 '122108eaf97b6cb695032820a79651994845a6a2c861faa9faea23d19a8984ed',
					 'aafd89ca76af454623224fd5a530ff59e442ef28ebd4f491e4ae3d23f6519c65',
					 'd6e780ca2ece4ff7488f663727e274c80d08df6484bff9191e74f8bd64060489',
					 '200d58c7fba062fdfece30ff4693838f90a15d683b5465bff830e74973052dd2',
					 'ddd881dae1763add9fe1fd68adedafab9b81b5d10a3bec40f2a41f3f4b380f89',
					 '1390c4cd21574bb98ca0f6812e9cda7e3dde80ccd9e1f23de6166843c3dc626e',
					 'e831bbb0b5597f91bcabc07a629771b5ffee3df71b01fe9d46ecdc4dcee1fc48',
					 'd5bac4f6192c73e08ac45fb7091e15176489dfb8b38bcbc5cf6204ff5d33c42f',
					 'd4d6138ed65ea269b196e9388fa15406f3b0ad8eaeeaac68fa61765a9d59b9cf',
					 '5b70a465fad5af9e58192f17b0e630f72c3cdf3ccd2d85b1e7b999b21e421d93',
					 'd03dbfd355d431f0c1f3814bfea61bc8e16693e02dbbca9a9c1c4c8ba9269273',
					 'ad31a150588f858e1dab23df552d6a29ece79bc2a49872a574dafd5f5be0fe11',
					 '633af268d59dea0aa02b70defe6794c0e41e682f9995d1dd0e81b7f9d0b5e041'
	)
;

/*
rerun 03_touchification since beginning of scv
self_describing_task --include 'biapp/task_catalogue/dv/dwh/scv/03_touchification/03_touchification.py'  --method 'run' --start '2022-12-01 00:00:00' --end '2022-12-01 00:00:00'
*/

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.travelbird_mysql.orders_orderproperty
	CLONE latest_vault.travelbird_mysql.orders_orderproperty
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_sale
	CLONE data_vault_mvp.dwh.se_sale
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_attributes
	CLONE data_vault_mvp.dwh.user_attributes
;

/*
rerun events of interest datasets from yesterday

self_describing_task --include 'biapp/task_catalogue/dv/dwh/scv/04_events_of_interest/01_module_touched_spvs.py'  --method 'run' --start '2025-11-11 00:00:00' --end '2025-11-11 00:00:00'
self_describing_task --include 'biapp/task_catalogue/dv/dwh/scv/04_events_of_interest/02_module_touched_transactions.py'  --method 'run' --start '2025-11-11 00:00:00' --end '2025-11-11 00:00:00'
self_describing_task --include 'biapp/task_catalogue/dv/dwh/scv/04_events_of_interest/03_module_touched_searches.py'  --method 'run' --start '2025-11-11 00:00:00' --end '2025-11-11 00:00:00'
self_describing_task --include 'biapp/task_catalogue/dv/dwh/scv/04_events_of_interest/04_module_touched_app_installs.py'  --method 'run' --start '2025-11-11 00:00:00' --end '2025-11-11 00:00:00'
self_describing_task --include 'biapp/task_catalogue/dv/dwh/scv/04_events_of_interest/06_module_touched_booking_form_views.py'  --method 'run' --start '2025-11-11 00:00:00' --end '2025-11-11 00:00:00'
self_describing_task --include 'biapp/task_catalogue/dv/dwh/scv/04_events_of_interest/07_module_touched_in_app_notification_events.py'  --method 'run' --start '2025-11-11 00:00:00' --end '2025-11-11 00:00:00'
self_describing_task --include 'biapp/task_catalogue/dv/dwh/scv/04_events_of_interest/08_module_touched_pay_button_clicks.py'  --method 'run' --start '2025-11-11 00:00:00' --end '2025-11-11 00:00:00'
self_describing_task --include 'biapp/task_catalogue/dv/dwh/scv/04_events_of_interest/09_module_events_of_interest.py'  --method 'run' --start '2025-11-11 00:00:00' --end '2025-11-11 00:00:00'
self_describing_task --include 'biapp/task_catalogue/dv/dwh/scv/05_touch_basic_attributes/00_anomalous_user_dates.py'  --method 'run' --start '2025-11-11 00:00:00' --end '2025-11-11 00:00:00'
self_describing_task --include 'biapp/task_catalogue/dv/dwh/scv/05_touch_basic_attributes/01_module_touch_basic_attributes.py'  --method 'run' --start '2025-11-11 00:00:00' --end '2025-11-11 00:00:00'
self_describing_task --include 'biapp/task_catalogue/dv/dwh/scv/06_touch_channelling/01_module_touch_utm_referrer.py'  --method 'run' --start '2025-11-11 00:00:00' --end '2025-11-11 00:00:00'
self_describing_task --include 'biapp/task_catalogue/dv/dwh/scv/06_touch_channelling/02_module_touch_marketing_channel.py'  --method 'run' --start '2025-11-11 00:00:00' --end '2025-11-11 00:00:00'
*/

-- dev
SELECT
	mtba.touch_se_brand,
	COUNT(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes mtba
WHERE mtba.touch_start_tstamp::DATE <= '2025-11-09'
GROUP BY 1
;


-- prod
SELECT
	mtba.touch_se_brand,
	COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
WHERE mtba.touch_start_tstamp::DATE <= '2025-11-09'
GROUP BY 1
;


USE WAREHOUSE pipe_xlarge
;

WITH
	dev AS (
		SELECT
			mtba.touch_se_brand,
			mtba.touch_start_tstamp::DATE          AS session_start_date,
			mtmc.touch_mkt_channel,
			mtmc.touch_affiliate_territory,
			COUNT(*)                               AS sessions,
			SUM(IFF(mtba.touch_has_booking, 1, 0)) AS sessions_with_booking
		FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes mtba
		INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution mta
			ON mtba.touch_id = mta.touch_id
			AND mta.attribution_model = 'last non direct'
		INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel mtmc
			ON mta.attributed_touch_id = mtmc.touch_id
		WHERE mtba.touch_start_tstamp::DATE <= '2025-11-09'
		GROUP BY ALL
	),
	prod AS (

-- prod
		SELECT
			mtba.touch_se_brand,
			mtba.touch_start_tstamp::DATE          AS session_start_date,
			mtmc.touch_mkt_channel,
			mtmc.touch_affiliate_territory,
			COUNT(*)                               AS sessions,
			SUM(IFF(mtba.touch_has_booking, 1, 0)) AS sessions_with_booking
		FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
		INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_attribution mta
			ON mtba.touch_id = mta.touch_id
			AND mta.attribution_model = 'last non direct'
		INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mtmc
			ON mta.attributed_touch_id = mtmc.touch_id
		WHERE mtba.touch_start_tstamp::DATE <= '2025-11-09'
		GROUP BY ALL
	)
SELECT
	COALESCE(dev.touch_se_brand, prod.touch_se_brand)                       AS touch_se_brand,
	COALESCE(dev.session_start_date, prod.session_start_date)               AS session_start_date,
	COALESCE(dev.touch_mkt_channel, prod.touch_mkt_channel)                 AS touch_mkt_channel,
	COALESCE(dev.touch_affiliate_territory, prod.touch_affiliate_territory) AS touch_affiliate_territory,
	prod.sessions                                                           AS prod_sessions,
	dev.sessions                                                            AS dev_sessions,
	prod.sessions_with_booking                                              AS prod_sessions_with_booking,
	dev.sessions_with_booking                                               AS dev_sessions_with_booking,
FROM dev
FULL OUTER JOIN prod
	ON dev.touch_se_brand = prod.touch_se_brand
	AND dev.session_start_date = prod.session_start_date
	AND dev.touch_mkt_channel = prod.touch_mkt_channel
	AND dev.touch_affiliate_territory = prod.touch_affiliate_territory
;

--prod
SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
WHERE mtba.attributed_user_id = '2439876' AND mtba.touch_se_brand = 'Travelist'
;

-- dev
SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes mtba
WHERE mtba.attributed_user_id = '2439876' AND mtba.touch_se_brand = 'Travelist'
;


/*
Sessions missing in dev
--TOUCH_ID
5cf4ac3e49c4d2bd708c298bd8a7e9bc25adff9b474bf4ecb2cfd16942ea7958
e7611bed25b76cd890d1ce5a1aa14992d6de021c94b27e15778de983268ec532
*/

SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification mt
WHERE mt.event_hash IN (
						'5cf4ac3e49c4d2bd708c298bd8a7e9bc25adff9b474bf4ecb2cfd16942ea7958',
						'e7611bed25b76cd890d1ce5a1aa14992d6de021c94b27e15778de983268ec532'
	)
  AND mt.stitched_identity_type = 'tvl_user_id'
  AND mt.event_tstamp::DATE = '2025-11-10'
;


SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_touchification mt
WHERE mt.event_hash IN (
						'5cf4ac3e49c4d2bd708c298bd8a7e9bc25adff9b474bf4ecb2cfd16942ea7958',
						'e7611bed25b76cd890d1ce5a1aa14992d6de021c94b27e15778de983268ec532'
	)
  AND mt.stitched_identity_type = 'tvl_user_id'
  AND mt.event_tstamp::DATE = '2025-11-10'
;

-- 5cf4ac3e49c4d2bd708c298bd8a7e9bc25adff9b474bf4ecb2cfd16942ea7958

SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes mtba
WHERE touch_id = '5cf4ac3e49c4d2bd708c298bd8a7e9bc25adff9b474bf4ecb2cfd16942ea7958'
  AND mtba.touch_se_brand = 'Travelist'
  AND mtba.touch_start_tstamp >= CURRENT_DATE - 30
;



SELECT
	COUNT(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification t
WHERE t.updated_at >= TIMESTAMPADD('day', -1, '2025-11-10 02:00:00'::TIMESTAMP)

-- 47,041,282
-- 2,999,904,452


SELECT
	COUNT(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification mt
;

SELECT
	COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touchification mt
;


SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes_clone__step01__get_source_batch
WHERE touch_id = '5cf4ac3e49c4d2bd708c298bd8a7e9bc25adff9b474bf4ecb2cfd16942ea7958'
; -- is here

SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes_clone__step02__model_agg_values
WHERE touch_id = '5cf4ac3e49c4d2bd708c298bd8a7e9bc25adff9b474bf4ecb2cfd16942ea7958'
; -- not here


SELECT
	--aggregate values
	t.touch_id,
	t.attributed_user_id                                                      AS attributed_user_id,
	t.stitched_identity_type                                                  AS stitched_identity_type,
	-- logged in se_brand
	IFF(MAX(e.se_user_id) IS NOT NULL, TRUE, FALSE)                           AS se_touch_logged_in,  --if the user has logged in at any time during this session
	-- logged in tvl brand
	IFF(MAX(e.tvl_user_id) IS NOT NULL, TRUE, FALSE)                          AS tvl_touch_logged_in, --if the user has logged in at any time during this session
	IFF(se_touch_logged_in = TRUE OR tvl_touch_logged_in = TRUE, TRUE, FALSE) AS touch_logged_in,
	MIN(e.event_tstamp)                                                       AS touch_start_tstamp,
	MAX(e.event_tstamp)                                                       AS touch_end_tstamp,
	TIMEDIFF(SECONDS, MIN(e.event_tstamp), MAX(e.event_tstamp))               AS touch_duration_seconds,
	-- internal se_user checks
	MAX(e.is_internal_ip_address_event)                                       AS is_internal_ip_address,
	-- checking on email address stitched type -- TBC if this is worth it (expensive)
	MAX(IFF(stitched_identity_type = 'email_address' AND SPLIT_PART(attributed_user_id, '@', -1) = 'secretescapes.com',
			TRUE,
			FALSE))                                                           AS is_se_email_domain_user,
	MAX(IFF(ua.internal_se_user = TRUE, TRUE, FALSE))                         AS internal_se_user_check,
	IFF(is_internal_ip_address OR is_se_email_domain_user OR internal_se_user_check, TRUE,
		FALSE)                                                                AS is_se_internal_touch,
	MAX(IFF(t.event_index_within_touch = '1' AND
			(e.se_user_id IS NOT NULL OR e.tvl_user_id IS NOT NULL), TRUE,
			FALSE))                                                           AS first_event_has_user_id_populated,
	SUM(IFF(e.useragent = 'ApacheBench/2.3', 1, 0)) > 0                       AS page_load_testing,
	-- event counts
	COUNT(*)                                                                  AS touch_event_count,
	COUNT(DISTINCT meoi.event_hash)                                           AS touch_event_of_interest_count,
	SUM(IFF(e.event_name = 'page_view', 1, 0))                                AS num_web_page_views,
	SUM(IFF(e.event_name = 'screen_view', 1, 0))                              AS num_app_screen_views,
	num_web_page_views + num_app_screen_views                                 AS num_page_views,
	SUM(IFF(e.event_name = 'page_view' AND e.is_server_side_event IS DISTINCT FROM TRUE, 1,
			0))                                                               AS num_web_client_side_page_views,
	SUM(IFF(e.event_name = 'screen_view' AND e.is_server_side_event IS DISTINCT FROM TRUE, 1,
			0))                                                               AS num_app_client_side_screen_views,
	num_web_client_side_page_views + num_app_client_side_screen_views         AS num_client_side_page_views,
	-- page views:
	SUM(IFF(meoi.event_subcategory = 'SPV', 1, 0))                            AS num_spvs,
	-- searches:
	SUM(IFF(meoi.event_subcategory = 'search', 1, 0))                         AS num_searches,
	-- user searches
	SUM(IFF(meoi.event_subcategory = 'search' AND meoi.triggered_by = 'user', 1,
			0))                                                               AS num_user_searches,
	-- bfvs:
	SUM(IFF(meoi.event_subcategory = 'booking_form_view', 1, 0))              AS num_bfvs,
	-- payment button clicks
	SUM(IFF(meoi.event_category = 'pay_button_click', 1, 0))                  AS num_pay_button_clicks,
	-- trxs:
	SUM(IFF(meoi.event_category = 'transaction', 1, 0))                       AS num_trxs,
	IFF(num_trxs >= 1, TRUE, FALSE)                                           AS touch_has_booking,
	-- app installs:
	SUM(IFF(meoi.event_category = 'app install event', 1, 0))                 AS num_app_installs,
	-- app notification events:
	SUM(IFF(meoi.event_category = 'app notification event' AND meoi.event_subcategory = 'in_app_open', 1,
			0))                                                               AS num_app_notification_events_in_app_opens,
	SUM(IFF(meoi.event_category = 'app notification event' AND meoi.event_subcategory = 'in_app_click', 1,
			0))                                                               AS num_app_notification_events_in_app_clicks,
	-- engaged session definition from product analytics
	IFF(num_trxs >= 1 OR (num_spvs + num_searches + num_bfvs) >= 2, TRUE,
		FALSE)                                                                AS is_engaged_session_events_of_interest,
	-- engaged session definition from commerical insights
	IFF(touch_event_count >= 3 OR touch_event_of_interest_count >= 1, TRUE,
		FALSE)                                                                AS is_engaged_session,
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification t
INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes_clone__step01__get_source_batch batch
	ON t.touch_id = batch.touch_id
INNER JOIN hygiene_vault_mvp_dev_robin.snowplow.event_stream e
	ON t.event_hash = e.event_hash
LEFT JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_events_of_interest meoi
	ON meoi.event_hash = t.event_hash
LEFT JOIN data_vault_mvp_dev_robin.dwh.user_attributes ua
	ON TRY_TO_NUMBER(t.attributed_user_id) = ua.shiro_user_id
WHERE t.touch_id = '5cf4ac3e49c4d2bd708c298bd8a7e9bc25adff9b474bf4ecb2cfd16942ea7958'
GROUP BY 1, 2, 3
;

SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification mt
INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes_clone__step01__get_source_batch batch
	ON mt.touch_id = batch.touch_id
INNER JOIN hygiene_vault_mvp_dev_robin.snowplow.event_stream e
	ON mt.event_hash = e.event_hash
WHERE mt.touch_id = '5cf4ac3e49c4d2bd708c298bd8a7e9bc25adff9b474bf4ecb2cfd16942ea7958'

CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream CLONE hygiene_vault_mvp.snowplow.event_stream
;

-- check session channel count

-- snapshot the schema post sessionisation oauth changes
CREATE OR REPLACE SCHEMA data_vault_mvp_dev_robin.single_customer_view_stg_tvl_sessionisation_change CLONE data_vault_mvp_dev_robin.single_customer_view_stg
;

-- continue development to adjust trivago inflated sessions

SELECT
	mtba.touch_landing_page,
	mtba.touch_landing_pagepath
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes mtba
WHERE mtba.touch_start_tstamp >= CURRENT_DATE - 10
  AND mtba.touch_se_brand = 'Travelist'
  AND mtba.touch_landing_page LIKE '%graph%'
;

SELECT
	DATE_TRUNC(WEEK, mtba.touch_start_tstamp)                     AS wc_date,
	COUNT(*)                                                      AS sessions,
	SUM(IFF(mtba.touch_landing_pagepath = '/api/graphql/', 1, 0)) AS graphql_sessions,
	SUM(IFF(mtba.touch_landing_pagepath = '/api/graphql/' AND mtba.touch_landing_page LIKE '%extapi%', 1,
			0))                                                   AS graphql_extapi_sessions,
	SUM(IFF(mtba.touch_landing_pagepath = '/api/graphql/' AND
			PARSE_URL(mtba.touch_landing_page, 1)['parameters']['extapi']::VARCHAR = '1', 1,
			0))                                                   AS graphql_extapi_sessions,
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes mtba
WHERE mtba.touch_start_tstamp >= '2025-01-01'
  AND mtba.touch_se_brand = 'Travelist'
GROUP BY ALL
;

-- Update se tech logic and then rerun

SELECT
	DATE_TRUNC(WEEK, mtba.touch_start_tstamp)                        AS wc_date,
	COUNT(*)                                                         AS sessions,
	SUM(IFF(UPPER(mtba.touch_hostname_territory) = 'PL', 1, 0))      AS pl_sessions,
	SUM(IFF(UPPER(mtba.touch_hostname_territory) = 'SE TECH', 1, 0)) AS se_tech_sessions,

FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes mtba
WHERE mtba.touch_start_tstamp >= '2025-01-01'
  AND mtba.touch_se_brand = 'Travelist'
GROUP BY ALL
;



SELECT
	DATE_TRUNC(WEEK, mtba.touch_start_tstamp)                     AS wc_date,
	mtba.touch_hostname_territory,
	COUNT(*)                                                      AS sessions,
	SUM(IFF(mtba.touch_landing_pagepath = '/api/graphql/', 1, 0)) AS graphql_sessions,
	SUM(IFF(mtba.touch_landing_pagepath = '/api/graphql/' AND mtba.touch_landing_page LIKE '%extapi%', 1,
			0))                                                   AS graphql_extapi_sessions,
	SUM(IFF(mtba.touch_landing_pagepath = '/api/graphql/' AND
			PARSE_URL(mtba.touch_landing_page, 1)['parameters']['extapi']::VARCHAR = '1', 1,
			0))                                                   AS graphql_extapi_sessions,
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes mtba
WHERE mtba.touch_start_tstamp >= '2025-01-01'
  AND mtba.touch_se_brand = 'Travelist'
GROUP BY ALL
;


SELECT
	DATE_TRUNC(WEEK, mtba.touch_start_tstamp) AS wc_date,
	mtba.touch_hostname_territory,
	mtba.touch_landing_page,
	mtba.touch_landing_pagepath
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes mtba
WHERE mtba.touch_start_tstamp >= '2025-01-01'
  AND mtba.touch_se_brand = 'Travelist'
  AND mtba.touch_hostname_territory = 'PL'
  AND mtba.touch_landing_pagepath = '/api/graphql/'
  AND PARSE_URL(mtba.touch_landing_page, 1)['parameters']['extapi']::VARCHAR = '1'
;

self_describing_task --include 'biapp/task_catalogue/dv/dwh/scv/05_touch_basic_attributes/01_module_touch_basic_attributes.py'  --method 'run' --start '2025-11-10 00:00:00' --end '2025-11-10 00:00:00'



USE ROLE pipelinerunner
;

CREATE SCHEMA data_vault_mvp.stg_single_customer_view__spi_7762 CLONE data_vault_mvp.single_customer_view_stg
;


USE WAREHOUSE pipe_xlarge
;

WITH
	backup AS (
		SELECT
			mtba.touch_se_brand,
			mtba.touch_start_tstamp::DATE          AS session_start_date,
			mtmc.touch_mkt_channel,
			mtmc.touch_affiliate_territory,
			COUNT(*)                               AS sessions,
			SUM(IFF(mtba.touch_has_booking, 1, 0)) AS sessions_with_booking
		FROM data_vault_mvp.stg_single_customer_view__spi_7762.module_touch_basic_attributes mtba
		INNER JOIN data_vault_mvp.stg_single_customer_view__spi_7762.module_touch_attribution mta
			ON mtba.touch_id = mta.touch_id
			AND mta.attribution_model = 'last non direct'
		INNER JOIN data_vault_mvp.stg_single_customer_view__spi_7762.module_touch_marketing_channel mtmc
			ON mta.attributed_touch_id = mtmc.touch_id
		WHERE mtba.touch_start_tstamp::DATE <= '2025-11-09'
		GROUP BY ALL
	),
	prod AS (

-- prod
		SELECT
			mtba.touch_se_brand,
			mtba.touch_start_tstamp::DATE          AS session_start_date,
			mtmc.touch_mkt_channel,
			mtmc.touch_affiliate_territory,
			COUNT(*)                               AS sessions,
			SUM(IFF(mtba.touch_has_booking, 1, 0)) AS sessions_with_booking
		FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
		INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_attribution mta
			ON mtba.touch_id = mta.touch_id
			AND mta.attribution_model = 'last non direct'
		INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mtmc
			ON mta.attributed_touch_id = mtmc.touch_id
		WHERE mtba.touch_start_tstamp::DATE <= '2025-11-09'
		GROUP BY ALL
	)
SELECT
	COALESCE(backup.touch_se_brand, prod.touch_se_brand)                       AS touch_se_brand,
	COALESCE(backup.session_start_date, prod.session_start_date)               AS session_start_date,
	COALESCE(backup.touch_mkt_channel, prod.touch_mkt_channel)                 AS touch_mkt_channel,
	COALESCE(backup.touch_affiliate_territory, prod.touch_affiliate_territory) AS touch_affiliate_territory,
	prod.sessions                                                              AS prod_sessions,
	backup.sessions                                                            AS backup_sessions,
	prod.sessions_with_booking                                                 AS prod_sessions_with_booking,
	backup.sessions_with_booking                                               AS backup_sessions_with_booking,
FROM backup
FULL OUTER JOIN prod
	ON backup.touch_se_brand = prod.touch_se_brand
	AND backup.session_start_date = prod.session_start_date
	AND backup.touch_mkt_channel = prod.touch_mkt_channel
	AND backup.touch_affiliate_territory = prod.touch_affiliate_territory
;
------------------------------------------------------------------------------------------------------------------------

USE ROLE pipelinerunner
;

CREATE SCHEMA data_vault_mvp.stg_single_customer_view__spi_7762 CLONE data_vault_mvp.single_customer_view_stg
;

USE ROLE personal_role__robinpatel
;


TRUNCATE data_vault_mvp.single_customer_view_stg.module_url_hostname
;


MERGE INTO data_vault_mvp.single_customer_view_stg.module_url_hostname AS target
	USING (
		WITH
			extract_hostname AS (
				SELECT
					url,
					parsed_url['host']::VARCHAR AS url_hostname
				FROM data_vault_mvp.single_customer_view_stg.module_unique_urls
				WHERE is_valid_url = TRUE
--              AND schedule_tstamp >= TO_DATE(TIMESTAMPADD('day', -1, '2025-10-31 02:00:00'::TIMESTAMP))
			)

		SELECT DISTINCT
			url,
			url_hostname,
			-- internal and payment gateway flag required to identify which referrers to ignore in touchification
			-- internal defined as hostnames that SE track in Snowplow
			CASE
				WHEN
					url_hostname LIKE 'webmail.%' OR
					url_hostname LIKE '%.email' OR
					url_hostname LIKE 'email.%' OR
					url_hostname LIKE '%.email.%'
					THEN 'email'

				WHEN
					url_hostname LIKE '%.secretescapes.%' OR
					url_hostname LIKE '%.evasionssecretes.%' OR
					url_hostname = 'escapes.travelbook.de' OR
					url_hostname = 'api.secretescapes.com' OR
					url_hostname LIKE '%.fs-staging.escapes.tech' OR
					url_hostname = 'www.optimizelyedit.com' OR
					url_hostname = 'cdn.secretescapes.com' OR
					url_hostname = 'secretescapes--c.eu12.visual.force.com' OR
					url_hostname = 'secretescapes.my.salesforce.com' OR
					url_hostname = 'cms.secretescapes.com' OR
					url_hostname = 'escapes.jetsetter.com' OR
					url_hostname LIKE '%travelbird.%' OR
					url_hostname LIKE '%travelist.pl' OR
					url_hostname = 'holidays.pigsback.com' OR
					url_hostname = 'www.travista.de' OR
					url_hostname = 'www.mycityvenueescapes.com' OR
					url_hostname = 'admin.co.uk.sales.secretescapes.com'
					THEN 'internal'

				WHEN
					(url_hostname LIKE '%.facebook.%' AND url LIKE '%oauth%') OR --facebook oauth logins
					url_hostname LIKE
					'accounts.google.%' --google oauth login eg. accounts.google.com, accounts.google.pl, accounts.google.cz

					THEN 'oauth'

				WHEN
					url_hostname = 'www.guardianescapes.com' OR
					url_hostname = 'www.gilttravel.com' OR
					url_hostname = 'www.hand-picked.telegraph.co.uk' OR
					url_hostname = 'escapes.radiotimes.com' OR
					url_hostname = 'escapes.timeout.com' OR
					url_hostname = 'www.independentescapes.com' OR
					url_hostname = 'www.confidentialescapes.co.uk' OR
					url_hostname = 'www.eveningstandardescapes.com' OR
					url_hostname = 'asap.shermanstravel.com' OR
					url_hostname = 'www.lateluxury.com' OR
					url_hostname = 'secretescapes.urlaubsguru.de'
					THEN 'whitelabel'

				WHEN
					-- Secret Escapes payment providers
					url_hostname = 'www.paypal.com' OR
					url_hostname = 'secure.worldpay.com' OR
					url_hostname = 'secure.bidverdrd.com' OR
					url_hostname = '3d-secure.pluscard.de' OR
					url_hostname = 'mastercardsecurecode.sparkassen-kreditkarten.de' OR
					url_hostname = '3d-secure.postbank.de' OR
					url_hostname = 'german-3dsecure.wlp-acs.com' OR
					url_hostname = '3d-secure-code.de' OR
					url_hostname = 'search.f-secure.com' OR

						-- Travelist payment providers
					url_hostname = ('e.blik.com') OR
					url_hostname = ('eblik.pl') OR
					url_hostname LIKE ('%payu.com') OR
					url_hostname = ('auth.pkobp.pl') OR
					url_hostname LIKE ('%pekao24.pl') OR
					url_hostname = ('www.centrum24.pl') OR
					url_hostname = ('psd2.bankmillenium.pl') OR
					url_hostname LIKE ('%inteligo.pl') OR
					url_hostname = ('ca24.credit-agricole.pl') OR
					url_hostname = ('pbn.paybynet.com.pl') OR
					url_hostname LIKE ('%bnpparibas.pl') OR
					url_hostname = ('login.nestbank.pl') OR
					url_hostname LIKE ('%mbank.pl') OR
					url_hostname = ('login.ingbank.pl') OR
					url_hostname LIKE ('%aliorbank.pl') OR
					url_hostname LIKE ('%citibankonline.pl') OR
					url_hostname LIKE ('%bankmillennium.pl') OR
					url_hostname LIKE ('%velobank.pl') OR
					url_hostname = ('interpay.pkobp.pl') OR
					url_hostname = ('www.ipko.pl') OR
					url_hostname = ('ingbusiness.pl') OR
					url_hostname = ('bosbank24.pl') OR
					url_hostname = ('plusbank24.pl') OR
					url_hostname = ('www.pekaobiznes24.pl') OR
					url_hostname LIKE ('%.cui.pl') OR
					url_hostname LIKE ('%.lubuskibs.pl') OR
					url_hostname = ('bswschowa24.pl') OR
					url_hostname = ('www.bankmillennium.pl') OR
					url_hostname = ('ebsd.pl') OR
					url_hostname = ('ebp.bsolesnica.net') OR
					url_hostname = ('ebo.bsjarocin.pl') OR
					url_hostname = ('ebo.bslesnica.pl') OR
					url_hostname = ('login.gbsbank.pl') OR
					url_hostname = ('ebo.bspawlowice.pl') OR
					url_hostname = ('ebp.bsplonsk.pl') OR
					url_hostname = ('bslubniany.cui.pl') OR
					url_hostname LIKE ('%.bs-suchedniow.com.pl') OR
					url_hostname = ('ebobank.bsjl.pl') OR
					url_hostname = ('ebo.bsrymanow.pl') OR
					url_hostname = ('ib.bsmiedzna.pl') OR
					url_hostname LIKE ('%.bsbrodnica.pl') OR
					url_hostname = ('e-bsjaroslaw.cui.pl') OR
					url_hostname = ('net-bank.bszgierz.pl') OR
					url_hostname = ('ilowabank.cui.pl') OR
					url_hostname = ('ebank.bsszczytno.pl') OR
					url_hostname = ('ebo.bskonskie.pl') OR
					url_hostname = ('bslubartow24.pl') OR
					url_hostname = ('online.bankbps.pl')
					THEN 'payment_gateway'

				WHEN
					url_hostname LIKE '%.google.%' OR
					url_hostname LIKE '%.bing.%' OR
					url_hostname LIKE '%.duckduckgo.%' OR
					url_hostname LIKE '%.ecosia.%' OR
					url_hostname LIKE '%.aol.%' OR
					url_hostname LIKE '%.aolsearch.%'
					THEN 'search'

				WHEN
					url_hostname LIKE '%.pinterest.%' OR
					url_hostname LIKE '%.facebook.%' OR
					url_hostname = 'instagram.com'
					THEN 'social'

				WHEN
					LOWER(url_hostname) REGEXP
					'(.*(web|db-loadtesting|sandbox).*\\.secretescapes\\.com|\\.*\\.fs-staging\\.escapes\\.tech|[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}|.*\\.amazonaws\\.com)'
					THEN 'SE TECH'

				ELSE 'unknown'
			END AS url_medium
		FROM extract_hostname
		WHERE url_hostname IS NOT NULL
	) AS batch ON target.url = batch.url
	WHEN NOT MATCHED
		THEN INSERT VALUES ('2025-10-31 02:00:00',
							'2025-11-13 14:43:17',
							'ScriptOperator__/usr/local/airflow/dags/biapp/task_catalogue/dv/dwh/scv/01_url_manipulation/02_01_module_url_hostname.py__20251031T020000__daily_at_02h00',
							CURRENT_TIMESTAMP()::TIMESTAMP,
							CURRENT_TIMESTAMP()::TIMESTAMP,
							batch.url,
							batch.url_hostname,
							batch.url_medium)
;


SELECT
	COUNT(*)
FROM data_vault_mvp.stg_single_customer_view__spi_7762.module_url_hostname muh

SELECT
	COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_url_hostname muh

SELECT
	muh.url_medium,
	COUNT(*)
FROM data_vault_mvp.stg_single_customer_view__spi_7762.module_url_hostname muh
WHERE url LIKE '%accounts.google%'
GROUP BY 1

SELECT
	muh.url_medium,
	COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_url_hostname muh
WHERE url LIKE '%accounts.google%'
GROUP BY 1;


SELECT
	DATE_TRUNC(WEEK, mtba.touch_start_tstamp) AS wc_date,
	mtba.touch_hostname_territory,
	mtba.touch_landing_page,
	mtba.touch_landing_pagepath
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
WHERE mtba.touch_start_tstamp >= '2025-01-01'
  AND mtba.touch_se_brand = 'Travelist'
--   AND mtba.touch_hostname_territory = 'PL'
  AND mtba.touch_landing_pagepath = '/api/graphql/'
  AND PARSE_URL(mtba.touch_landing_page, 1)['parameters']['extapi']::VARCHAR = '1'