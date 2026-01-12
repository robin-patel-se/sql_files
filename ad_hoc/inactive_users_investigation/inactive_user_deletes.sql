SELECT
	MIN(loaded_at)
FROM raw_vault.cms_mysql.shiro_user su
;

SELECT
-- 	DATE_PART(YEAR, ua.signup_tstamp)        AS sign_up_year,
DATE_PART(YEAR, ua.membership_last_updated)  AS year,
DATE_PART(MONTH, ua.membership_last_updated) AS month,
COUNT(*)
FROM se.data.se_user_attributes ua
WHERE ua.membership_account_status = 'DELETED'
GROUP BY 1, 2
;

USE WAREHOUSE pipe_2xlarge
;

WITH
	list_of_users AS (
		SELECT
			ua.shiro_user_id,
			ua.signup_tstamp,
			ua.last_pageview_tstamp,
			ua.email,
			ua.membership_last_updated,
			ua.membership_account_status,
			ua.original_affiliate_name,
			ua.original_affiliate_brand,
			ua.original_affiliate_territory
		FROM se.data_pii.se_user_attributes ua
		WHERE ua.membership_account_status = 'DELETED'
	),
	first_ingest AS (
		SELECT
			id          AS shiro_user_id,
			su.username AS first_email
		FROM raw_vault.cms_mysql.shiro_user su
		QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY loaded_at) = 1
	),
	model_data AS (
		SELECT
			lou.*,
			fi.first_email
		FROM list_of_users lou
			LEFT JOIN first_ingest fi ON lou.shiro_user_id = fi.shiro_user_id
	)
-- SELECT
-- 	DATE_PART(YEAR, md.membership_last_updated)  AS year,
-- -- 	DATE_PART(MONTH, md.membership_last_updated) AS month,
-- 	DATE_PART(YEAR, md.signup_tstamp)  AS sign_up_year,
-- 	COUNT(*)
-- FROM model_data md
-- GROUP BY 1, 2
-- SELECT * FROM model_data

SELECT
	COUNT(*)                                                AS total_users,
	SUM(IFF(md.email = md.first_email, 1, 0))               AS users_without_original_email,
	SUM(IFF(md.email != md.first_email, 1, 0))              AS users_with_original_email,
	SUM(IFF(md.first_email LIKE '%@deleted.account', 1, 0)) AS users_without_original_email2,
	SUM(IFF(md.first_email NOT LIKE '%@deleted.account' AND md.first_email LIKE '%@%', 1,
			0))                                             AS users_with_original_email2
FROM model_data md
;


SELECT *
FROM latest_vault.iterable.lists l
;

SELECT *
FROM latest_vault.iterable.users u
;

SELECT *
FROM latest_vault.information_schema.tables t
WHERE t.table_schema = 'ITERABLE'
;

SELECT *
FROM latest_vault.iterable.users u
WHERE u.list_id = 1357309
; --- 5,835,196 users in this list


WITH
	suppressed_emails AS (
		SELECT DISTINCT
			u.email_address
		FROM latest_vault.iterable.users u
		WHERE u.list_id = 1357309
	)
SELECT
	ua.shiro_user_id,
	ua.email,
	ua.email_opt_in,
	ua.email_opt_in_status
FROM data_vault_mvp.dwh.user_attributes ua
	INNER JOIN suppressed_emails se ON ua.email = se.email_address
WHERE ua.email_opt_in_status IS DISTINCT FROM 'opted out'

-- 152,171 -- opted in that are in the suppression list

------------------------------------------------------------------------------------------------------------------------
-- split 2020 deletes by their age


SELECT
	DATE_PART(YEAR, iu.created_at) AS year,
	source,
	COUNT(DISTINCT iu.shiro_user_id)
FROM data_vault_mvp.dwh.inactive_users iu
GROUP BY 1, 2
;


SELECT
	ua.original_affiliate_name,
	COUNT(*)
FROM data_vault_mvp.dwh.user_attributes ua
WHERE DATE_PART(YEAR, ua.membership_last_updated) = 2022
  AND ua.membership_account_status = 'DELETED'
GROUP BY 1
;

SELECT
	fcb.booking_completed_date,
	fcb.territory,
	COUNT(fcb.booking_id)                               AS bookings,
	SUM(fcb.margin_gross_of_toms_gbp_constant_currency) AS marign
FROM se.data.fact_complete_booking fcb
WHERE fcb.booking_completed_date >= CURRENT_DATE - 30
GROUP BY 1, 2
;

------------------------------------------------------------------------------------------------------------------------
-- check policy sensitivity

SELECT
	shiro_user_id,
	GREATEST(IFNULL(ura.last_session_end_tstamp, '1970-01-01'),
			 IFNULL(ura.last_email_open_tstamp, '1970-01-01'),
			 IFNULL(ura.last_purchase_tstamp, '1970-01-01')) AS last_activity_date
FROM data_vault_mvp.dwh.user_recent_activities ura
;



WITH
	deleted_users AS (
-- get a list of all deleted users from user_attributes table
		SELECT
			ua.shiro_user_id,
			ua.membership_account_status,
			-- proxy for deleted date as the membership status updates
			-- some have no last updated - users from 2011 so adding 1970 catch all
			COALESCE(ua.membership_last_updated::DATE, '1970-01-01') AS deleted_date,
			ua.signup_tstamp::DATE                                   AS sign_up_date,
			DATEDIFF(YEAR, sign_up_date, deleted_date)               AS age_when_deleted,
			-- working out last activity data for all deleted users
			-- this will only be used where we can't find this information from our pipeline logic
			-- i.e. for pre camilla inactive users dataset going live
			-- for this reason (to try and relate it as closely to our old logic, I have not included credits / vouchers as were not present previously)
			GREATEST(ua.signup_tstamp,
					 ura.last_session_end_tstamp,
					 ura.last_email_open_tstamp,
					 ura.last_purchase_tstamp)                       AS last_activity_date,
			DATEDIFF(YEAR, last_activity_date::DATE, deleted_date)   AS years_between_last_activity_and_deleted_date,
			ua.original_affiliate_name,
			ua.original_affiliate_territory,
			CASE
				WHEN
						original_affiliate_name IN
						(
						 'HotelsOne',
						 'PF2014 Smart',
						 'PF2013 Smart',
						 'PF2017 Smart',
						 'PF 2016 Smart',
						 'PF2015 Smart',
						 'PF2012 Smart',
						 'PF 2018 Smart',
						 'PF 2014 Luxury',
						 'PF2017 Luxury',
						 'PF2015 Luxury',
						 'PF2016 Luxury',
						 'PF 2018 Luxe',
						 'PF2013 Luxury',
						 'PF2012 Luxury',
						 'Society 19 Travel',
						 'Gilt',
						 'SweepsJan2018',
						 'Condé Nast Traveller',
						 'Vacationist',
						 'Hotel Guru Offers', -- UK WL terminated
						 'the Skimm',
						 'Travelbook Escapes',
						 'Jetsetter Escapes',
						 'Tatler Travel Club',
						 'Roomer Luxury',
						 'Miami Herald',
						 'Time Out USA'
							)
					THEN 'affiliate_deleted'
				WHEN
						original_affiliate_name IN
						(
						 'TravelBird AT',
						 'TravelBird NL',
						 'TravelBird BE-NL',
						 'TravelBird BE-FR')
					THEN 'tech_bulk_upload_error_tbc'
				ELSE NULL
			END                                                      AS affiliate_deletion_type,
			CASE
				WHEN
						original_affiliate_territory IN
						('CZ',
						 'DK',
						 'ES',
						 'FR',
						 'HK',
						 'HU',
						 'ID',
						 'IE',
						 'MY',
						 'PH',
						 'SE_TEMP',
						 'SG',
						 'SK',
						 'US')
					THEN 'closed_posa'
				ELSE NULL
			END                                                      AS posa_deletion_type
		FROM data_vault_mvp.dwh.user_attributes ua
			LEFT JOIN data_vault_mvp.dwh.user_recent_activities ura ON ura.shiro_user_id = ua.shiro_user_id
		WHERE membership_account_status = 'DELETED'
	),
-- looking at our inactive users feed running daily (ignoring bulk update from Iterable)
	inactive_users_feed AS
		(
			SELECT
				shiro_user_id,
				last_activity_tstamp::DATE          AS last_activity_date,
				source,
				years_since_last_activity_date,
				'data_platform_inactive_users_feed' AS deletion_type
			FROM data_vault_mvp.dwh.inactive_users
			WHERE created_at::DATE > '2021-11-29'
-- there are instances where user has activity on the day we want to delete so Tech don't delete
-- so we take the v latest version of this data
			QUALIFY ROW_NUMBER() OVER (PARTITION BY shiro_user_id ORDER BY run_tstamp DESC) = 1
		),
-- when we first ran this process in pipeline - we sent a huge number of users to be deleted to Tech
	inactive_users_migration_run AS (
		SELECT
			shiro_user_id,
			last_activity_tstamp::DATE                AS last_activity_date,
			source,
			years_since_last_activity_date,
			created_at                                AS date_deletion_requested,
			'first_inactive_users_run_post_migration' AS deletion_type
		FROM data_vault_mvp.dwh.inactive_users
		WHERE created_at::DATE = '2021-11-29'
	),
-- combine this data together using deleted users as a base
-- some users may come under the bulk migration and following days -
-- in that case take from the bulk -- bc understanding is they should have been deleted previously
	model_data AS (
		SELECT
			du.shiro_user_id,
			du.membership_account_status,
			du.original_affiliate_name,
			du.original_affiliate_territory,
			du.sign_up_date,
			du.deleted_date,
			du.age_when_deleted,
			du.last_activity_date,                                                                    -- using old process
			du.years_between_last_activity_and_deleted_date,
			-- combining the inactive users logic together - first bulk and then daily job
			COALESCE(iumr.last_activity_date, iuf.last_activity_date) AS inactive_users_logic_last_activity_date,
			COALESCE(iumr.source, iuf.source)                         AS inactive_users_logic_source, -- bookers / non-bookers
			COALESCE(iumr.years_since_last_activity_date,
					 iuf.years_since_last_activity_date)              AS inactive_users_logic_years_since_last_activity_date,
			COALESCE(du.posa_deletion_type, du.affiliate_deletion_type, iumr.deletion_type, iuf.deletion_type,
					 'no_data_visibility')                            AS deletion_type
		FROM deleted_users du
			LEFT JOIN inactive_users_feed iuf ON iuf.shiro_user_id = du.shiro_user_id
			LEFT JOIN inactive_users_migration_run iumr ON iumr.shiro_user_id = du.shiro_user_id
	),
	enhance_no_data_visibility AS (
		SELECT
			shiro_user_id,
			membership_account_status,
			deleted_date,
			sign_up_date,
			age_when_deleted,
			original_affiliate_name,
			inactive_users_logic_source,
			deletion_type,
			--- if not in our user logic take it from using user attributes / recent activity logic calculated in first step
			IFF(deletion_type = 'no_data_visibility', last_activity_date,
				inactive_users_logic_last_activity_date)             AS last_activity_date,
			IFF(deletion_type = 'no_data_visibility', years_between_last_activity_and_deleted_date,
				inactive_users_logic_years_since_last_activity_date) AS years_since_last_activity
		FROM model_data
	)
SELECT
--YEAR(deleted_date) AS deleted_year,
deletion_type,
-- inactive_users_logic_source,
COUNT(*) AS num_users
FROM enhance_no_data_visibility
WHERE enhance_no_data_visibility.deleted_date::DATE >= '2020-03-01' -- change here
  AND enhance_no_data_visibility.deleted_date::DATE <= '2020-03-31'-- change here
GROUP BY 1
;


CREATE OR REPLACE TRANSIENT TABLE collab.muse.deleted_user_list AS (
	WITH
		deleted_users AS (
-- get a list of all deleted users from user_attributes table
			SELECT
				ua.shiro_user_id,
				ua.membership_account_status,
				-- proxy for deleted date as the membership status updates
				-- some have no last updated - users from 2011 so adding 1970 catch all
				COALESCE(ua.membership_last_updated::DATE, '1970-01-01') AS deleted_date,
				ua.signup_tstamp::DATE                                   AS sign_up_date,
				DATEDIFF(YEAR, sign_up_date, deleted_date)               AS age_when_deleted,
				-- working out last activity data for all deleted users
				-- this will only be used where we can't find this information from our pipeline logic
				-- i.e. for pre camilla inactive users dataset going live
				-- for this reason (to try and relate it as closely to our old logic, I have not included credits / vouchers as were not present previously)
				GREATEST(ua.signup_tstamp,
						 ura.last_session_end_tstamp,
						 ura.last_email_open_tstamp,
						 ura.last_purchase_tstamp)                       AS last_activity_date,
				ura.last_purchase_tstamp,
				DATEDIFF(YEAR, last_activity_date::DATE, deleted_date)   AS years_between_last_activity_and_deleted_date,
				ua.original_affiliate_name,
				ua.original_affiliate_territory,
				CASE
					WHEN
							original_affiliate_name IN
							(
							 'HotelsOne',
							 'PF2014 Smart',
							 'PF2013 Smart',
							 'PF2017 Smart',
							 'PF 2016 Smart',
							 'PF2015 Smart',
							 'PF2012 Smart',
							 'PF 2018 Smart',
							 'PF 2014 Luxury',
							 'PF2017 Luxury',
							 'PF2015 Luxury',
							 'PF2016 Luxury',
							 'PF 2018 Luxe',
							 'PF2013 Luxury',
							 'PF2012 Luxury',
							 'Society 19 Travel',
							 'Gilt',
							 'SweepsJan2018',
							 'Condé Nast Traveller',
							 'Vacationist',
							 'Hotel Guru Offers', -- UK WL terminated
							 'the Skimm',
							 'Travelbook Escapes',
							 'Jetsetter Escapes',
							 'Tatler Travel Club',
							 'Roomer Luxury',
							 'Miami Herald',
							 'Time Out USA'
								)
						THEN 'affiliate_deleted'
					WHEN
							original_affiliate_name IN
							(
							 'TravelBird AT',
							 'TravelBird NL',
							 'TravelBird BE-NL',
							 'TravelBird BE-FR')
						THEN 'tech_bulk_upload_error_tbc'
				END                                                      AS affiliate_deletion_type,
				CASE
					WHEN
							original_affiliate_territory IN
							('CZ',
							 'DK',
							 'ES',
							 'FR',
							 'HK',
							 'HU',
							 'ID',
							 'IE',
							 'MY',
							 'PH',
							 'SE_TEMP',
							 'SG',
							 'SK',
							 'US')
						THEN 'closed_posa'
				END                                                      AS posa_deletion_type
			FROM data_vault_mvp.dwh.user_attributes ua
				LEFT JOIN data_vault_mvp.dwh.user_recent_activities ura ON ura.shiro_user_id = ua.shiro_user_id
			WHERE membership_account_status = 'DELETED'
		),
-- looking at our inactive users feed running daily (ignoring bulk update from Iterable)
		inactive_users_feed AS
			(
				SELECT
					shiro_user_id,
					last_activity_tstamp::DATE          AS last_activity_date,
					source,
					years_since_last_activity_date,
					'data_platform_inactive_users_feed' AS deletion_type
				FROM data_vault_mvp.dwh.inactive_users
				WHERE created_at::DATE > '2021-11-29'
-- there are instances where user has activity on the day we want to delete so Tech don't delete
-- so we take the v latest version of this data
				QUALIFY ROW_NUMBER() OVER (PARTITION BY shiro_user_id ORDER BY run_tstamp DESC) = 1
			),
-- when we first ran this process in pipeline - we sent a huge number of users to be deleted to Tech
		inactive_users_migration_run AS (
			SELECT
				shiro_user_id,
				last_activity_tstamp::DATE                AS last_activity_date,
				source,
				years_since_last_activity_date,
				created_at                                AS date_deletion_requested,
				'first_inactive_users_run_post_migration' AS deletion_type
			FROM data_vault_mvp.dwh.inactive_users
			WHERE created_at::DATE = '2021-11-29'
		),
-- combine this data together using deleted users as a base
-- some users may come under the bulk migration and following days -
-- in that case take from the bulk -- bc understanding is they should have been deleted previously
		model_data AS (
			SELECT
				du.shiro_user_id,
				du.membership_account_status,
				du.original_affiliate_name,
				du.original_affiliate_territory,
				du.sign_up_date,
				du.deleted_date,
				du.age_when_deleted,
				du.last_activity_date,                                                                    -- using old process
				du.last_purchase_tstamp,
				du.years_between_last_activity_and_deleted_date,
				-- combining the inactive users logic together - first bulk and then daily job
				COALESCE(iumr.last_activity_date, iuf.last_activity_date) AS inactive_users_logic_last_activity_date,
				COALESCE(iumr.source, iuf.source)                         AS inactive_users_logic_source, -- bookers / non-bookers
				COALESCE(iumr.years_since_last_activity_date,
						 iuf.years_since_last_activity_date)              AS inactive_users_logic_years_since_last_activity_date,
				COALESCE(du.posa_deletion_type, du.affiliate_deletion_type, iumr.deletion_type, iuf.deletion_type,
						 'no_data_visibility')                            AS deletion_type
			FROM deleted_users du
				LEFT JOIN inactive_users_feed iuf ON iuf.shiro_user_id = du.shiro_user_id
				LEFT JOIN inactive_users_migration_run iumr ON iumr.shiro_user_id = du.shiro_user_id
		),
		enhance_no_data_visibility AS (
			SELECT
				shiro_user_id,
				membership_account_status,
				deleted_date,
				sign_up_date,
				age_when_deleted,
				original_affiliate_name,
				inactive_users_logic_source,
				deletion_type,
				model_data.last_purchase_tstamp,
				original_affiliate_territory,
				--- if not in our user logic take it from using user attributes / recent activity logic calculated in first step
				IFF(deletion_type = 'no_data_visibility', last_activity_date,
					inactive_users_logic_last_activity_date)             AS last_activity_date,
				IFF(deletion_type = 'no_data_visibility', years_between_last_activity_and_deleted_date,
					inactive_users_logic_years_since_last_activity_date) AS years_since_last_activity
			FROM model_data
		)
	SELECT
		endv.shiro_user_id,
		endv.membership_account_status,
		endv.original_affiliate_territory,
		endv.deleted_date,
		endv.sign_up_date,
		endv.age_when_deleted,
		endv.original_affiliate_name,
		endv.inactive_users_logic_source,
		endv.deletion_type,
		endv.last_activity_date,
		endv.years_since_last_activity,
		endv.last_purchase_tstamp
	FROM enhance_no_data_visibility endv

)
;

USE WAREHOUSE pipe_xlarge
;

GRANT SELECT ON TABLE collab.muse.deleted_user_list TO ROLE data_team_basic
;

SELECT
	dul.original_affiliate_territory,
	dul.deletion_type,
	COUNT(*)
FROM collab.muse.deleted_user_list dul
GROUP BY 1, 2
;

WITH
	no_activity_deleted_user AS (
		SELECT DISTINCT
			dul.shiro_user_id
		FROM collab.muse.deleted_user_list dul
		WHERE dul.deletion_type = 'no_data_visibility'
	),
	last_legacy_snowplow_activity AS (
		-- single customer view validated data only goes back to 2018,
		-- utilising incomplete snowplow data
		SELECT
			e.user_id          AS shiro_user_id,
			e.collector_tstamp AS last_event_tstamp_legacy_snowplow
		FROM snowplow.atomic.events e
		WHERE e.collector_tstamp < '2018-01-01'
		  AND TRY_TO_NUMBER(e.user_id) IS NOT NULL
		QUALIFY ROW_NUMBER() OVER (PARTITION BY e.user_id ORDER BY e.collector_tstamp DESC) = 1
	),
	last_mongo_activity AS (
		SELECT
			epv.record['u']['id']::NUMBER AS shiro_user_id,
			epv.record['c']::TIMESTAMP    AS last_event_tstamp_legacy_mongo
		FROM raw_vault_mvp.cms_mongodb.events_page_visit epv
		WHERE epv.record['u']['id']::NUMBER IS NOT NULL
		  AND epv.record['c']::TIMESTAMP <= '2018-01-01'
		QUALIFY ROW_NUMBER() OVER (PARTITION BY epv.record['u']['id']::NUMBER ORDER BY epv.record['c']::TIMESTAMP DESC) =
				1
	)
SELECT
	na.shiro_user_id,
	GREATEST(COALESCE(sla.last_event_tstamp_legacy_snowplow, '1970-01-01'),
			 COALESCE(mla.last_event_tstamp_legacy_mongo, '1970-01-01')) AS last_event_legacy_tstamp,
	sla.last_event_tstamp_legacy_snowplow,
	mla.last_event_tstamp_legacy_mongo
FROM no_activity_deleted_user na
	LEFT JOIN last_legacy_snowplow_activity sla ON na.shiro_user_id = sla.shiro_user_id
	LEFT JOIN last_mongo_activity mla ON na.shiro_user_id = mla.shiro_user_id
WHERE last_event_legacy_tstamp > '1970-01-01'
;

USE WAREHOUSE pipe_default
USE ROLE personal_role__alexhenshaw
/*
{
  "_id": {
    "$oid": "5f1aad97f62e34f67a9a208e"
  },
  "a": {
    "urlString": "es"
  },
  "au": {},
  "c": "2020-07-24 09:44:55.593000",
  "d": {
    "page": "https://www.secretescapes.com/refundable/filter",
    "referrer": "https://www.secretescapes.com/current-sales"
  },
  "t": "page_visit",
  "tr": {
    "id": "eed3b4a1899414e2842916f249cd1495"
  },
  "u": {
    "id": 2338645
  }
}*/


SELECT
	COUNT(*)
FROM data_vault_mvp.dwh.user_attributes ua
	LEFT JOIN data_vault_mvp.dwh.user_recent_activities ura ON ura.shiro_user_id = ua.shiro_user_id
WHERE membership_account_status = 'DELETED'
  AND COALESCE(last_session_end_tstamp, last_email_open_tstamp, ura.last_purchase_tstamp) IS NULL
;
--30,741,199 -- users that are deleted without any activity


WITH
	users_without_activity AS (
		SELECT
			ua.shiro_user_id,
			GREATEST(ua.signup_tstamp,
					 ura.last_session_end_tstamp,
					 ura.last_email_open_tstamp,
					 ura.last_purchase_tstamp) AS last_activity_date
		FROM data_vault_mvp.dwh.user_attributes ua
			LEFT JOIN data_vault_mvp.dwh.user_recent_activities ura ON ura.shiro_user_id = ua.shiro_user_id
		WHERE membership_account_status = 'DELETED'
		  AND COALESCE(ura.last_session_end_tstamp, ura.last_email_open_tstamp, ura.last_purchase_tstamp) IS NULL
	),
	last_legacy_snowplow_activity AS (
		-- single customer view validated data only goes back to 2018,
		-- utilising incomplete snowplow data
		SELECT
			e.user_id          AS shiro_user_id,
			e.collector_tstamp AS last_event_tstamp_legacy_snowplow
		FROM snowplow.atomic.events e
		WHERE e.collector_tstamp < '2018-01-01'
		  AND TRY_TO_NUMBER(e.user_id) IS NOT NULL
		QUALIFY ROW_NUMBER() OVER (PARTITION BY e.user_id ORDER BY e.collector_tstamp DESC) = 1
	),
	last_mongo_activity AS (
		SELECT
			epv.record['u']['id']::NUMBER AS shiro_user_id,
			epv.record['c']::TIMESTAMP    AS last_event_tstamp_legacy_mongo
		FROM raw_vault_mvp.cms_mongodb.events_page_visit epv
		WHERE epv.record['u']['id']::NUMBER IS NOT NULL
		  AND epv.record['c']::TIMESTAMP <= '2018-01-01'
		QUALIFY ROW_NUMBER() OVER (PARTITION BY epv.record['u']['id']::NUMBER ORDER BY epv.record['c']::TIMESTAMP DESC) =
				1
	)
SELECT
	na.shiro_user_id,
	GREATEST(COALESCE(sla.last_event_tstamp_legacy_snowplow, '1970-01-01'),
			 COALESCE(mla.last_event_tstamp_legacy_mongo, '1970-01-01')) AS last_event_legacy_tstamp,
	sla.last_event_tstamp_legacy_snowplow,
	mla.last_event_tstamp_legacy_mongo
FROM users_without_activity na
	LEFT JOIN last_legacy_snowplow_activity sla ON na.shiro_user_id = sla.shiro_user_id
	LEFT JOIN last_mongo_activity mla ON na.shiro_user_id = mla.shiro_user_id
WHERE last_event_legacy_tstamp > '1970-01-01'
;

-- 1,810,204


------------------------------------------------------------------------------------------------------------------------
WITH
	deleted_users AS (

		SELECT
			ua.shiro_user_id,
			ua.membership_account_status,
			-- proxy for deleted date as the membership status updates
			-- some have no last updated - users from 2011 so adding 1970 catch all
			COALESCE(ua.membership_last_updated::DATE, '1970-01-01') AS deleted_date,
			ua.signup_tstamp::DATE                                   AS sign_up_date,
			ua.original_affiliate_territory,
			ua.original_affiliate_name,
			GREATEST(ua.signup_tstamp,
					 ura.last_session_end_tstamp,
					 ura.last_email_open_tstamp,
					 ura.last_purchase_tstamp)                       AS last_activity_date_production
		FROM data_vault_mvp.dwh.user_attributes ua
			LEFT JOIN data_vault_mvp.dwh.user_recent_activities ura ON ura.shiro_user_id = ua.shiro_user_id
		WHERE ua.membership_account_status = 'DELETED'
	),
	last_legacy_snowplow_activity AS (
		-- single customer view validated data only goes back to 2018,
		-- utilising incomplete snowplow data to get better coverage over user last activity
		SELECT
			e.user_id          AS shiro_user_id,
			e.collector_tstamp AS last_event_tstamp_legacy_snowplow
		FROM snowplow.atomic.events e
		WHERE e.collector_tstamp < '2018-01-01'
		  AND TRY_TO_NUMBER(e.user_id) IS NOT NULL
		QUALIFY ROW_NUMBER() OVER (PARTITION BY e.user_id ORDER BY e.collector_tstamp DESC) = 1
	),
	last_mongo_activity AS (
		-- single customer view validated data only goes back to 2018,
		-- utilising incomplete mongo data to get better coverage over user last activity
		SELECT
			epv.record['u']['id']::NUMBER AS shiro_user_id,
			epv.record['c']::TIMESTAMP    AS last_event_tstamp_legacy_mongo
		FROM raw_vault_mvp.cms_mongodb.events_page_visit epv
		WHERE epv.record['u']['id']::NUMBER IS NOT NULL
		  AND epv.record['c']::TIMESTAMP <= '2018-01-01'
		QUALIFY ROW_NUMBER() OVER (PARTITION BY epv.record['u']['id']::NUMBER ORDER BY epv.record['c']::TIMESTAMP DESC) =
				1
	)
SELECT
	du.shiro_user_id,
	du.membership_account_status,
	du.deleted_date,
	du.sign_up_date,
	du.original_affiliate_territory,
	du.original_affiliate_name,
	du.last_activity_date_production,
	GREATEST(COALESCE(sla.last_event_tstamp_legacy_snowplow, '1970-01-01'),
			 COALESCE(mla.last_event_tstamp_legacy_mongo, '1970-01-01')) AS last_event_legacy_tstamp,
	COALESCE(du.last_activity_date_production, last_event_legacy_tstamp) AS last_activity_date,
	DATEADD(YEAR, 4, last_activity_date)                                 AS deletion_date_4_years,
	DATEADD(YEAR, 5, last_activity_date)                                 AS deletion_date_5_years,
	DATEADD(YEAR, 6, last_activity_date)                                 AS deletion_date_6_years,
	DATEADD(YEAR, 7, last_activity_date)                                 AS deletion_date_7_years
FROM deleted_users du
	LEFT JOIN last_legacy_snowplow_activity sla ON du.shiro_user_id = sla.shiro_user_id
	LEFT JOIN last_mongo_activity mla ON du.shiro_user_id = mla.shiro_user_id
WHERE last_event_legacy_tstamp > '1970-01-01'
;


SELECT DISTINCT
	dul.deletion_type
FROM collab.muse.deleted_user_list dul
;

-- data_platform_inactive_users_feed
-- first_inactive_users_run_post_migration
-- no_data_visibility


WITH
	last_legacy_snowplow_activity AS (
		-- single customer view validated data only goes back to 2018,
		-- utilising incomplete snowplow data to get better coverage over user last activity
		SELECT
			e.user_id          AS shiro_user_id,
			e.collector_tstamp AS last_event_tstamp_legacy_snowplow
		FROM snowplow.atomic.events e
		WHERE e.collector_tstamp < '2018-01-01'
		  AND TRY_TO_NUMBER(e.user_id) IS NOT NULL
		QUALIFY ROW_NUMBER() OVER (PARTITION BY e.user_id ORDER BY e.collector_tstamp DESC) = 1
	),
	last_legacy_mongo_activity AS (
		-- single customer view validated data only goes back to 2018,
		-- utilising incomplete mongo data to get better coverage over user last activity
		SELECT
			epv.record['u']['id']::NUMBER AS shiro_user_id,
			epv.record['c']::TIMESTAMP    AS last_event_tstamp_legacy_mongo
		FROM raw_vault_mvp.cms_mongodb.events_page_visit epv
		WHERE epv.record['u']['id']::NUMBER IS NOT NULL
		  AND epv.record['c']::TIMESTAMP <= '2018-01-01'
		QUALIFY ROW_NUMBER() OVER (PARTITION BY epv.record['u']['id']::NUMBER ORDER BY epv.record['c']::TIMESTAMP DESC) =
				1
	),
	model_deletion_groups AS (
		SELECT
			dul.shiro_user_id,
			dul.deleted_date,
			dul.membership_account_status,
			dul.sign_up_date,
			dul.original_affiliate_territory,
			dul.original_affiliate_name,
			dul.last_activity_date,
			dul.last_purchase_tstamp,
			llsa.last_event_tstamp_legacy_snowplow,
			llma.last_event_tstamp_legacy_mongo,
			GREATEST(dul.last_activity_date,
					 COALESCE(llsa.last_event_tstamp_legacy_snowplow, '1970-01-01'),
					 COALESCE(llma.last_event_tstamp_legacy_mongo, '1970-01-01')
				)                                                            AS greatest_activity_date,
			ROUND(DATEDIFF(DAY, dul.last_purchase_tstamp, dul.deleted_date)) AS delete_day_diff_to_purchase,
			ROUND(DATEDIFF(DAY, greatest_activity_date, dul.deleted_date))   AS delete_day_diff_greatest_activity_date
		FROM collab.muse.deleted_user_list dul
			LEFT JOIN last_legacy_snowplow_activity llsa ON dul.shiro_user_id = llsa.shiro_user_id
			LEFT JOIN last_legacy_mongo_activity llma ON dul.shiro_user_id = llma.shiro_user_id
		WHERE dul.deletion_type IN (
									'data_platform_inactive_users_feed',
									'first_inactive_users_run_post_migration',
									'no_data_visibility'
			)
	),
	modelling_days AS (
		SELECT
			mdg.shiro_user_id,
			mdg.deleted_date,
			mdg.last_activity_date,
			mdg.greatest_activity_date,
			mdg.last_purchase_tstamp,
			mdg.delete_day_diff_to_purchase,
			mdg.delete_day_diff_greatest_activity_date,
			(4 * 365) AS days_4_years,
			(5 * 365) AS days_5_years,
			(6 * 365) AS days_6_years,
			(7 * 365) AS days_7_years,
			CASE
				WHEN delete_day_diff_to_purchase IS NOT NULL AND delete_day_diff_greatest_activity_date > days_7_years
					THEN TRUE
				WHEN delete_day_diff_greatest_activity_date > days_4_years THEN TRUE
				ELSE FALSE
			END       AS valid_4_year,
			CASE
				WHEN delete_day_diff_to_purchase IS NOT NULL AND delete_day_diff_greatest_activity_date > days_7_years
					THEN TRUE
				WHEN delete_day_diff_greatest_activity_date > days_5_years THEN TRUE
				ELSE FALSE
			END       AS valid_5_year,
			CASE
				WHEN delete_day_diff_to_purchase IS NOT NULL AND delete_day_diff_greatest_activity_date > days_7_years
					THEN TRUE
				WHEN delete_day_diff_greatest_activity_date > days_6_years THEN TRUE
				ELSE FALSE
			END       AS valid_6_year,
			CASE
				WHEN delete_day_diff_to_purchase IS NOT NULL AND delete_day_diff_greatest_activity_date > days_7_years
					THEN TRUE
				WHEN delete_day_diff_greatest_activity_date > days_7_years THEN TRUE
				ELSE FALSE
			END       AS valid_7_year
		FROM model_deletion_groups mdg
	)
SELECT *
FROM modelling_days md
WHERE md.valid_5_year = FALSE
;

SELECT
	DATE_PART(YEAR, md.deleted_date)  AS year,
	DATE_PART(MONTH, md.deleted_date) AS month,
-- 	md.valid_4_year,gian
	md.valid_5_year,
-- 	md.valid_6_year,
-- 	md.valid_7_year,
	COUNT(*)                          AS users
FROM modelling_days md
GROUP BY ALL
;

SELECT
	MIN(dasl.send_date)
FROM dbt.bi_data_science__intermediate.ds_athena_send_log dasl
;


WITH
	last_legacy_snowplow_activity AS (
		-- single customer view validated data only goes back to 2018,
		-- utilising incomplete snowplow data to get better coverage over user last activity
		SELECT
			e.user_id          AS shiro_user_id,
			e.collector_tstamp AS last_event_tstamp_legacy_snowplow
		FROM snowplow.atomic.events e
		WHERE e.collector_tstamp < '2018-01-01'
		  AND TRY_TO_NUMBER(e.user_id) IS NOT NULL
		QUALIFY ROW_NUMBER() OVER (PARTITION BY e.user_id ORDER BY e.collector_tstamp DESC) = 1
	),
	last_legacy_mongo_activity AS (
		-- single customer view validated data only goes back to 2018,
		-- utilising incomplete mongo data to get better coverage over user last activity
		SELECT
			epv.record['u']['id']::NUMBER AS shiro_user_id,
			epv.record['c']::TIMESTAMP    AS last_event_tstamp_legacy_mongo
		FROM raw_vault_mvp.cms_mongodb.events_page_visit epv
		WHERE epv.record['u']['id']::NUMBER IS NOT NULL
		  AND epv.record['c']::TIMESTAMP <= '2018-01-01'
		QUALIFY ROW_NUMBER() OVER (PARTITION BY epv.record['u']['id']::NUMBER ORDER BY epv.record['c']::TIMESTAMP DESC) =
				1
	),
	model_deletion_date AS (
		SELECT
			dul.shiro_user_id,
			dul.deleted_date,
			dul.membership_account_status,
			dul.sign_up_date,
			dul.original_affiliate_territory,
			dul.original_affiliate_name,
			dul.last_activity_date,
			dul.last_purchase_tstamp,
			llsa.last_event_tstamp_legacy_snowplow,
			llma.last_event_tstamp_legacy_mongo,
			GREATEST(dul.last_activity_date,
					 COALESCE(llsa.last_event_tstamp_legacy_snowplow, '1970-01-01'),
					 COALESCE(llma.last_event_tstamp_legacy_mongo, '1970-01-01')
				)                                                AS greatest_activity_date,
			IFF(dul.last_purchase_tstamp IS NOT NULL, DATEADD(DAY, (365 * 7), greatest_activity_date),
				DATEADD(DAY, (365 * 4), greatest_activity_date)) AS deletion_date_4_years,
		    IFF(dul.last_purchase_tstamp IS NOT NULL, DATEADD(DAY, (365 * 7), greatest_activity_date),
				DATEADD(DAY, (365 * 5), greatest_activity_date)) AS deletion_date_5_years,
			IFF(dul.last_purchase_tstamp IS NOT NULL, DATEADD(DAY, (365 * 7), greatest_activity_date),
				DATEADD(DAY, (365 * 6), greatest_activity_date)) AS deletion_date_6_years,
			IFF(dul.last_purchase_tstamp IS NOT NULL, DATEADD(DAY, (365 * 7), greatest_activity_date),
				DATEADD(DAY, (365 * 7), greatest_activity_date)) AS deletion_date_7_years
		FROM collab.muse.deleted_user_list dul
			LEFT JOIN last_legacy_snowplow_activity llsa ON dul.shiro_user_id = llsa.shiro_user_id
			LEFT JOIN last_legacy_mongo_activity llma ON dul.shiro_user_id = llma.shiro_user_id
		WHERE dul.deletion_type IN (
									'data_platform_inactive_users_feed',
									'first_inactive_users_run_post_migration',
									'no_data_visibility'
			)
	)

SELECT
	DATE_PART(YEAR, mdd.deletion_date_4_years)  AS year,
	DATE_PART(MONTH, mdd.deletion_date_4_years) AS month,
	COUNT(*)                                    AS users
FROM model_deletion_date mdd
WHERE mdd.deletion_date_4_years 
GROUP BY ALL;