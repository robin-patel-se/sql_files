SELECT *
FROM raw_vault_mvp.sfmc.affiliate_classification;
SELECT *
FROM raw_vault_mvp.information_schema.tables
WHERE table_schema = 'SFMC';

--affiliate_classification
SELECT GET_DDL('table', 'raw_vault_mvp.sfmc.affiliate_classification');
--athena_send_log
SELECT GET_DDL('table', 'raw_vault_mvp.sfmc.athena_send_log');
--daily_campaign_report
SELECT GET_DDL('table', 'raw_vault_mvp.sfmc.daily_campaign_report');
--daily_selection_errors
SELECT GET_DDL('table', 'raw_vault_mvp.sfmc.daily_selection_errors');
--datasources
SELECT GET_DDL('table', 'raw_vault_mvp.sfmc.datasources');
--events_bounces
SELECT GET_DDL('table', 'raw_vault_mvp.sfmc.events_bounces');
--events_clicks
SELECT GET_DDL('table', 'raw_vault_mvp.sfmc.events_clicks');
--events_opens
SELECT GET_DDL('table', 'raw_vault_mvp.sfmc.events_opens');
--events_opens_plus_inferred
SELECT GET_DDL('table', 'raw_vault_mvp.sfmc.events_opens_plus_inferred');
--events_sends
SELECT GET_DDL('table', 'raw_vault_mvp.sfmc.events_sends');
--events_spam
SELECT GET_DDL('table', 'raw_vault_mvp.sfmc.events_spam');
--events_unsubscribes
SELECT GET_DDL('table', 'raw_vault_mvp.sfmc.events_unsubscribes');
--jobs_list
SELECT GET_DDL('table', 'raw_vault_mvp.sfmc.jobs_list');
--jobs_sources
SELECT GET_DDL('table', 'raw_vault_mvp.sfmc.jobs_sources');
--net_promoter_score
SELECT GET_DDL('table', 'raw_vault_mvp.sfmc.net_promoter_score');
--push_status
SELECT GET_DDL('table', 'raw_vault_mvp.sfmc.push_status');
--subscriber_status
SELECT GET_DDL('table', 'raw_vault_mvp.sfmc.subscriber_status');

