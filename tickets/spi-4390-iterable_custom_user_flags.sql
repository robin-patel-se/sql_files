-- prototype sql now living in dbt

SELECT
	sua.shiro_user_id                                     AS user_id,
	sua.current_affiliate_territory,
	sua.membership_account_status,
	OBJECT_CONSTRUCT(
			'haulType', gcmri.haul_type,
			'holidayType', gcmri.holiday_type,
			'lastBookingSaleId', gcmri.last_booking_sale_id
		)                                                 AS custom_user_flags,
	SHA2(sua.shiro_user_id || custom_user_flags::VARCHAR) AS row_hash
FROM se.data.se_user_attributes sua
	INNER JOIN customer_insight.temp.ga_crm_most_recent_interests gcmri ON sua.shiro_user_id = gcmri.shiro_user_id
;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_custom_user_flags
;

self_describing_task --include 'biapp/task_catalogue/dv/dwh/iterable/user_profile_custom_user_flags.py'  --method 'run' --start '2023-11-08 00:00:00' --end '2023-11-08 00:00:00'

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.iterable__user_profile_custom_user_flags_20231109 CLONE data_vault_mvp_dev_robin.dwh.iterable__user_profile_custom_user_flags
;

-- find a user that has all keys populated and delete all users except that user

DELETE
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_custom_user_flags
WHERE user_id IS DISTINCT FROM 75362643
;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_custom_user_flags
;


--manufacture data for a user that is within iterable sandbox
UPDATE data_vault_mvp_dev_robin.dwh.iterable__user_profile_custom_user_flags
SET user_id = 12
WHERE user_id = 75362643
;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_custom_user_flags
;

self_describing_task --include 'biapp/task_catalogue/staging/outgoing/iterable/user_profile_custom_user_flags_sandbox/modelling.py'  --method 'run' --start '2023-11-08 00:00:00' --end '2023-11-08 00:00:00'

SELECT *
FROM unload_vault_mvp_dev_robin.iterable.user_profile_custom_user_flags_sandbox__20231107t030000__daily_at_03h00
;

-- check unload
dataset_task --include 'outgoing.iterable.user_profile_custom_user_flags_sandbox' --operation UnloadOperation --method 'run'  --start '2023-11-08 00:00:00' --end '2023-11-08 00:00:00'

dataset_task --include 'outgoing.iterable.user_profile_custom_user_flags_sandbox' --operation DistributeOperation --method 'run'  --start '2023-11-08 00:00:00' --end '2023-11-08 00:00:00'

--manufacture data for a user that is within iterable production for helen's user

UPDATE data_vault_mvp_dev_robin.dwh.iterable__user_profile_custom_user_flags
SET user_id = 79367018
WHERE user_id = 12
;

self_describing_task --include 'biapp/task_catalogue/staging/outgoing/iterable/user_profile_custom_user_flags/modelling.py'  --method 'run' --start '2023-11-08 00:00:00' --end '2023-11-08 00:00:00'

SELECT *
FROM unload_vault_mvp_dev_robin.iterable.user_profile_custom_user_flags__20231107t030000__daily_at_03h00
;

-- check unload
dataset_task --include 'outgoing.iterable.user_profile_custom_user_flags' --operation UnloadOperation --method 'run'  --start '2023-11-08 00:00:00' --end '2023-11-08 00:00:00'

dataset_task --include 'outgoing.iterable.user_profile_custom_user_flags' --operation DistributeOperation --method 'run'  --start '2023-11-08 00:00:00' --end '2023-11-08 00:00:00'

USE ROLE pipelinerunner
;

SELECT *
FROM data_vault_mvp.dwh.iterable__user_profile_custom_user_flags
;

SELECT *
FROM unload_vault_mvp.iterable.user_profile_custom_user_flags__20231108t030000__daily_at_03h00
;