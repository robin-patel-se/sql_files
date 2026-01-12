CREATE OR REPLACE TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream CLONE hygiene_vault_mvp.snowplow.event_stream;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_basic_touch_attributes CLONE data_vault_mvp.single_customer_view_stg.module_basic_touch_attributes;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_extracted_params CLONE data_vault_mvp.single_customer_view_stg.module_extracted_params;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_identity_associations CLONE data_vault_mvp.single_customer_view_stg.module_identity_associations;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_identity_stitching CLONE data_vault_mvp.single_customer_view_stg.module_identity_stitching;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_time_diff_marker CLONE data_vault_mvp.single_customer_view_stg.module_time_diff_marker;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs_bkup CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs_bkup;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions CLONE data_vault_mvp.single_customer_view_stg.module_touched_transactions;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchifiable_events CLONE data_vault_mvp.single_customer_view_stg.module_touchifiable_events;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification CLONE data_vault_mvp.single_customer_view_stg.module_touchification;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution CLONE data_vault_mvp.single_customer_view_stg.module_touch_attribution;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_utm_referrer CLONE data_vault_mvp.single_customer_view_stg.module_touch_utm_referrer;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_unique_urls CLONE data_vault_mvp.single_customer_view_stg.module_unique_urls;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_url_hostname CLONE data_vault_mvp.single_customer_view_stg.module_url_hostname;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_url_params CLONE data_vault_mvp.single_customer_view_stg.module_url_params;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_utm_referrer_marker CLONE data_vault_mvp.single_customer_view_stg.module_utm_referrer_marker;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_booking CLONE data_vault_mvp.dwh.se_booking;


self_describing_task --include 'staging/hygiene/snowplow/events.py'  --method 'run' --start '2021-06-29 00:00:00' --end '2021-06-29 00:00:00'
self_describing_task --include 'dv/dwh/events/00_artificial_transaction_insert/artificial_transaction_insert_se.py'  --method 'run' --start '2021-06-29 00:00:00' --end '2021-06-29 00:00:00'
self_describing_task --include 'dv/dwh/events/01_url_manipulation/01_module_unique_urls.py'  --method 'run' --start '2021-06-29 00:00:00' --end '2021-06-29 00:00:00'
self_describing_task --include 'dv/dwh/events/01_url_manipulation/02_01_module_url_hostname.py'  --method 'run' --start '2021-06-29 00:00:00' --end '2021-06-29 00:00:00'
self_describing_task --include 'dv/dwh/events/01_url_manipulation/02_02_module_url_params.py'  --method 'run' --start '2021-06-29 00:00:00' --end '2021-06-29 00:00:00'
self_describing_task --include 'dv/dwh/events/01_url_manipulation/03_module_extracted_params.py'  --method 'run' --start '2021-06-29 00:00:00' --end '2021-06-29 00:00:00'
self_describing_task --include 'dv/dwh/events/02_identity_stitching/01_module_identity_associations.py'  --method 'run' --start '2021-06-29 00:00:00' --end '2021-06-29 00:00:00'
self_describing_task --include 'dv/dwh/events/02_identity_stitching/02_module_identity_stitching.py'  --method 'run' --start '2021-06-29 00:00:00' --end '2021-06-29 00:00:00'
self_describing_task --include '/dv/dwh/events/03_touchification/01_touchifiable_events.py'  --method 'run' --start '2021-06-29 00:00:00' --end '2021-06-29 00:00:00'
self_describing_task --include 'dv/dwh/events/03_touchification/02_01_utm_or_referrer_hostname_marker.py'  --method 'run' --start '2021-06-29 00:00:00' --end '2021-06-29 00:00:00'
self_describing_task --include 'dv/dwh/events/03_touchification/02_02_time_diff_marker.py'  --method 'run' --start '2021-06-29 00:00:00' --end '2021-06-29 00:00:00'
self_describing_task --include 'dv/dwh/events/03_touchification/03_touchification.py'  --method 'run' --start '2021-06-29 00:00:00' --end '2021-06-29 00:00:00'
self_describing_task --include 'dv/dwh/events/04_touch_basic_attributes/01_module_touch_basic_attributes.py'  --method 'run' --start '2021-06-29 00:00:00' --end '2021-06-29 00:00:00'
self_describing_task --include 'dv/dwh/events/05_touch_channelling/01_module_touch_utm_referrer.py'  --method 'run' --start '2021-06-29 00:00:00' --end '2021-06-29 00:00:00'
self_describing_task --include 'dv/dwh/events/05_touch_channelling/02_module_touch_marketing_channel.py'  --method 'run' --start '2021-06-29 00:00:00' --end '2021-06-29 00:00:00'
self_describing_task --include 'dv/dwh/events/06_touch_attribution/01_module_touch_attribution.py'  --method 'run' --start '2021-06-29 00:00:00' --end '2021-06-29 00:00:00'
self_describing_task --include 'dv/dwh/events/07_events_of_interest/01_module_touched_spvs.py'  --method 'run' --start '2021-06-29 00:00:00' --end '2021-06-29 00:00:00'
self_describing_task --include 'dv/dwh/events/07_events_of_interest/02_module_touched_transactions.py'  --method 'run' --start '2021-06-29 00:00:00' --end '2021-06-29 00:00:00'

SELECT stba.touch_experience,
       COUNT(*)

FROM se.data.scv_touch_basic_attributes stba
GROUP BY 1
;

SELECT redirect_url
FROM se.data.se_sale_attributes ssa
WHERE ssa.product_configuration = 'WRD - direct'



SELECT e.event_hash,
       t.touch_id,
       e.event_tstamp,
       sa.se_sale_id,
       'web redirect' AS event_category,
       'SPV'          AS event_subcategory,
       e.se_action    AS page_url
FROM data_vault_mvp.single_customer_view_stg.module_touchification t
         INNER JOIN hygiene_vault_mvp.snowplow.event_stream e ON e.event_hash = t.event_hash
         INNER JOIN data_vault_mvp.dwh.se_sale sa ON sa.redirect_url = e.se_action
WHERE e.se_category = 'web redirect click'
  AND t.updated_at >= CURRENT_DATE - 1



SELECT mt.touch_id,
       IFF(se_category = 'web redirect click', se_action, page_url)                                         AS page_url,
       IFF(se_category = 'web redirect click', PARSE_URL(se_action, 1):scheme::VARCHAR, page_urlscheme)     AS page_urlscheme,
       IFF(se_category = 'web redirect click', PARSE_URL(se_action, 1):host::VARCHAR, page_urlhost)         AS page_urlhost,
       IFF(se_category = 'web redirect click', PARSE_URL(se_action, 1):port::VARCHAR, page_urlport)         AS page_urlport,
       IFF(se_category = 'web redirect click', PARSE_URL(se_action, 1):path::VARCHAR, page_urlpath)        AS page_urlpath,
       IFF(se_category = 'web redirect click', PARSE_URL(se_action, 1):query::VARCHAR, page_urlquery)       AS page_urlquery,
       IFF(se_category = 'web redirect click', PARSE_URL(se_action, 1):fragment::VARCHAR, page_urlfragment) AS page_urlfragment,
       es.*
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream es
         INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification mt ON es.event_hash = mt.event_hash
WHERE es.unique_browser_id = '99353423-7c46-4c30-9b68-d3994576d34c'
  AND es.event_tstamp >= CURRENT_DATE - 1
;

CREATE OR REPLACE TRANSIENT TABLE data