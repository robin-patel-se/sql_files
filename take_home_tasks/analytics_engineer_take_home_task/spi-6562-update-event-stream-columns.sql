unstruct_event_com_secretescapes_content_element_interaction_context_1 OBJECT,
        unstruct_event_com_secretescapes_content_elements_rendered_context_1 OBJECT,
        contexts_com_secretescapes_voucher_context_1 ARRAY,
        contexts_com_snowplowanalytics_mobile_application_1 ARRAY,
        contexts_com_snowplowanalytics_mobile_screen_1 ARRAY,
        contexts_com_snowplowanalytics_snowplow_gdpr_1 ARRAY,
        unstruct_event_com_snowplowanalytics_mobile_application_install_1 OBJECT,
        unstruct_event_com_secretescapes_authorisation_event_1 OBJECT,
        load_tstamp TIMESTAMP,
        contexts_com_snowplowanalytics_snowplow_duplicate_1 ARRAY,
        contexts_com_secretescapes_marketing_context_1 ARRAY,
        contexts_nl_basjes_yauaa_context_1 ARRAY,
        unstruct_event_com_secretescapes_user_context_1 OBJECT,
        unstruct_event_com_secretescapes_search_context_1 OBJECT,
        contexts_com_snowplowanalytics_mobile_application_lifecycle_1 ARRAY,
        unstruct_event_com_snowplowanalytics_mobile_screen_end_1 OBJECT,
        contexts_com_snowplowanalytics_mobile_screen_summary_1 ARRAY,
        unstruct_event_com_snowplowanalytics_snowplow_application_foreground_1 OBJECT,
        unstruct_event_com_snowplowanalytics_snowplow_application_background_1 OBJECT


CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream CLONE hygiene_vault_mvp.snowplow.event_stream
;

ALTER TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream
	ADD COLUMN unstruct_event_com_secretescapes_content_element_interaction_context_1 OBJECT,
		unstruct_event_com_secretescapes_content_elements_rendered_context_1 OBJECT,
		contexts_com_secretescapes_voucher_context_1 ARRAY,
		contexts_com_snowplowanalytics_mobile_application_1 ARRAY,
		contexts_com_snowplowanalytics_mobile_screen_1 ARRAY,
		contexts_com_snowplowanalytics_snowplow_gdpr_1 ARRAY,
		unstruct_event_com_snowplowanalytics_mobile_application_install_1 OBJECT,
		unstruct_event_com_secretescapes_authorisation_event_1 OBJECT,
		load_tstamp TIMESTAMP,
		contexts_com_snowplowanalytics_snowplow_duplicate_1 ARRAY,
		contexts_com_secretescapes_marketing_context_1 ARRAY,
		contexts_nl_basjes_yauaa_context_1 ARRAY,
		unstruct_event_com_secretescapes_user_context_1 OBJECT,
		unstruct_event_com_secretescapes_search_context_1 OBJECT,
		contexts_com_snowplowanalytics_mobile_application_lifecycle_1 ARRAY,
		unstruct_event_com_snowplowanalytics_mobile_screen_end_1 OBJECT,
		contexts_com_snowplowanalytics_mobile_screen_summary_1 ARRAY,
		unstruct_event_com_snowplowanalytics_snowplow_application_foreground_1 OBJECT,
		unstruct_event_com_snowplowanalytics_snowplow_application_background_1 OBJECT,
		contexts_com_secretescapes_app_state_context_1 ARRAY
;

SELECT * FROm  hygiene_vault_mvp_dev_robin.snowplow.event_stream;

self_describing_task --include 'biapp/task_catalogue/staging/hygiene/snowplow/event_stream.py'  --method 'run' --start '2024-10-03 00:00:00' --end '2024-10-03 00:00:00'




DROP TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream
;

