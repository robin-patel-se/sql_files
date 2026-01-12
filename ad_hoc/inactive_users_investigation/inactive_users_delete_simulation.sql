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
	last__legacy_mongo_activity AS (
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
	ua.shiro_user_id,
	ua.signup_tstamp,
	GREATEST(ua.signup_tstamp,
			 COALESCE(ura.last_session_end_tstamp, '1970-01-01'),
			 COALESCE(ura.last_email_open_tstamp, '1970-01-01'),
			 COALESCE(ura.last_purchase_tstamp, '1970-01-01')) AS last_activity_date_production,

FROM data_vault_mvp.dwh.user_attributes ua
	LEFT JOIN data_vault_mvp.dwh.user_recent_activities ura ON ua.shiro_user_id = ura.shiro_user_id
	LEFT JOIN last_legacy_snowplow_activity llsa ON ua.shiro_user_id = llsa.shiro_user_id
	LEFT JOIN last__legacy_mongo_activity llma ON ua.shiro_user_id = llma.shiro_user_id
