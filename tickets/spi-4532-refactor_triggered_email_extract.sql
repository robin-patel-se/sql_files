SELECT triggered_email.id,
       triggered_email.version,
       triggered_email.additional_info,
       triggered_email.async_conversation_id,
       triggered_email.date_created,
       triggered_email.date_delivered,
       triggered_email.date_queued_at_provider,
       triggered_email.fail_reason,
       triggered_email.last_updated,
       triggered_email.model,
       triggered_email.request_id,
       triggered_email.retries,
       triggered_email.status,
       triggered_email.to_address,
       triggered_email.type,
       triggered_email.provider
FROM triggered_email
WHERE last_updated >= '2023-12-10 00:30:00'
  AND status IN (select distinct status from triggered_email);

dataset_task --include 'cms_mysql.triggered_email_minus_model' --operation LatestRecordsOperation --method 'run' --upstream --start '2023-12-10 00:30:00' --end '2023-12-10 00:30:00'



		SELECT
			fs.tvl_user_id,
			fs.first_session_tstamp,
			mtmc.touch_mkt_channel AS acquisition_source
		FROM first_session fs
			LEFT JOIN {sources['module_touch_basic_attributes']} mtba
					ON fs.tvl_user_id = mtba.attributed_user_id
					AND fs.first_session_tstamp = mtba.touch_start_tstamp
					AND mtba.touch_se_brand = 'Travelist'
			LEFT JOIN  {sources['module_touch_marketing_channel']} mtmc
					ON mtmc.touch_id = mtba.touch_id


SELECT * FROM latest_vault_dev_robin.cms_mysql.triggered_email;



;
USE ROLE pipelinerunner;

CALL data_vault_mvp.dwh.table_usage('scratch.robinpatel.table_usage', 'se.data.triggered_email');

SELECT * FROM scratch.robinpatel.table_usage;