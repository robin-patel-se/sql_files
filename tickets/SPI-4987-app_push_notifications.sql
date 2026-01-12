SELECT *
FROM latest_vault.iterable.users u
;

dataset_task --include 'iterable.lists' --operation LatestRecordsOperation --method 'run' --upstream --start '2024-03-19 17:00:00' --end '2024-03-19 17:00:00'


SELECT *
FROM latest_vault_dev_robin.iterable.lists
WHERE id IN (
			 3371657,
			 3372115,
			 3372119
	)
;

dataset_task --include 'iterable.app_users' --operation LatestRecordsOperation --method 'run' --upstream --start '2024-03-19 17:00:00' --end '2024-03-19 17:00:00'


DROP TABLE hygiene_vault_dev_robin.iterable.app_users
;


dataset_task --include 'iterable.app_users' --operation HygieneOperation --method 'run'  --start '2024-03-19 17:00:00' --end '2024-03-19 17:00:00'


SELECT *
FROM hygiene_vault_dev_robin.iterable.app_users dataset_task --include 'iterable.app_users' --operation LatestRecordsOperation --method 'run'  --start '2024-03-19 17:00:00' --end '2024-03-19 17:00:00';

;

DESCRIBE TABLE hygiene_vault_dev_robin.iterable.app_users
;

DESCRIBE TABLE latest_vault_dev_robin.iterable.app_users
;

SHOW TABLES IN SCHEMA latest_vault_dev_robin.iterable
;


SELECT *
FROM latest_vault_dev_robin.iterable.app_users au
WHERE au.list_id = 3372119
;

SELECT *
FROM latest_vault_dev_robin.iterable.app_users au
WHERE au.list_id = 3372115 -- opted in
;

SELECT *
FROM latest_vault_dev_robin.iterable.app_users au
WHERE au.list_id = 3371657 -- opted out
;

SELECT
	au.email_address,
	TRUE AS has_app_installed,
	CASE
		WHEN opt_in.email_address IS NOT NULL THEN 'opted in'
		ELSE 'opted out'
	END  AS app_push_opt_in_status
FROM latest_vault_dev_robin.iterable.app_users au
	LEFT JOIN latest_vault_dev_robin.iterable.app_users opt_in
			  ON au.email_address = opt_in.email_address AND opt_in.list_id = 3372115 -- opted in list
WHERE au.list_id = 3372119 -- app installed user list
;

SELECT
	sua.email_opt_in_status
FROM se.data.se_user_attributes sua
;

DESCRIBE TABLE data_vault_mvp.dwh.user_attributes
;

SHOW TABLES IN SCHEMA data_vault_mvp.dwh
;


CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.affiliate CLONE latest_vault.cms_mysql.affiliate
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.membership CLONE latest_vault.cms_mysql.membership
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.profile CLONE latest_vault.cms_mysql.profile
;

CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_dev_robin.cms_mysql.shiro_user CLONE hygiene_vault.cms_mysql.shiro_user
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.shiro_user CLONE latest_vault.cms_mysql.shiro_user
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.subscription CLONE latest_vault.cms_mysql.subscription
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.territory CLONE latest_vault.cms_mysql.territory
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.theme CLONE latest_vault.cms_mysql.theme
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.iterable.app_users CLONE latest_vault.iterable.app_users
;

CREATE OR REPLACE TRANSIENT TABLE raw_vault_mvp_dev_robin.chiasma_sql_server.user_acquisition_platform CLONE raw_vault_mvp.chiasma_sql_server.user_acquisition_platform
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mongodb.users CLONE latest_vault.cms_mongodb.users
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_booking_review CLONE data_vault_mvp.dwh.user_booking_review
;

SELECT *
FROM latest_vault.cms_mongodb.users u
;

SELECT *
FROM data_vault_mvp.dwh.user_attributes ua
;

self_describing_task --include 'biapp/task_catalogue/dv/dwh/user_attributes/user_attributes.py'  --method 'run' --start '2024-03-19 00:00:00' --end '2024-03-19 00:00:00'


SELECT *
FROM data_vault_mvp_dev_robin.dwh.user_attributes
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_first_activities CLONE data_vault_mvp.dwh.user_first_activities
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_recent_activities CLONE data_vault_mvp.dwh.user_recent_activities
;


self_describing_task --include 'biapp/task_catalogue/se/data/dwh/se_user_attributes.py'  --method 'run' --start '2024-03-19 00:00:00' --end '2024-03-19 00:00:00'
self_describing_task --include 'biapp/task_catalogue/se/data_pii/dwh/se_user_attributes.py'  --method 'run' --start '2024-03-19 00:00:00' --end '2024-03-19 00:00:00'

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_segmentation CLONE data_vault_mvp.dwh.user_segmentation
;

self_describing_task --include 'biapp/task_catalogue/ds/user_engagement_snapshot/se_user.py'  --method 'run' --start '2024-03-19 00:00:00' --end '2024-03-19 00:00:00'

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.engagement_stg.tvl_user AS
SELECT *
FROM data_vault_mvp.engagement_stg.tvl_user
;
;


self_describing_task --include 'biapp/task_catalogue/ds/user_engagement_snapshot/user_snapshot.py'  --method 'run' --start '2024-03-19 00:00:00' --end '2024-03-19 00:00:00'


SELECT
	sua.current_affiliate_territory,
	COUNT(*)                                                                                     AS total_live_users,
	COUNT(DISTINCT IFF(sua.has_app_installed, sua.shiro_user_id, NULL))                          AS total_app_installed_users,
	total_app_installed_users / total_live_users                                                 AS percentage_users_with_app,
	COUNT(DISTINCT
		  IFF(sua.app_push_opt_in_status = 'opted in', sua.shiro_user_id, NULL))                 AS total_app_push_opt_in,
	total_app_push_opt_in / total_app_installed_users,
	COUNT(DISTINCT IFF(sua.email_opt_in_status IN ('daily', 'weekly'), sua.shiro_user_id, NULL)) AS email_opt_in_users,
	email_opt_in_users / total_live_users                                                        AS email_opt_in_rate
FROM se.data.se_user_attributes sua
WHERE sua.current_affiliate_territory IN ('UK', 'DE', 'IT')
  AND sua.membership_account_status = 'FULL_ACCOUNT'
GROUP BY 1
;

SELECT
	sua.acquisition_platform,
	sua.mongo_acquisition_source_earliest,
	sua.email_opt_in_status
FROM se.data.se_user_attributes sua
