-- biapp/task_catalogue/dv/dwh/scv/00_event_stream_modelling/01_artificial_transaction_insert.py

USE ROLE PERSONAL_ROLE__ROBINPATEL;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.fact_booking
AS SELECT * FROM data_vault_mvp.dwh.fact_booking;

CREATE SCHEMA IF NOT EXISTS hygiene_vault_mvp_dev_robin.snowplow;

CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream
CLONE hygiene_vault_mvp.snowplow.event_stream;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream
CLONE hygiene_vault_mvp.snowplow.event_stream;

self_describing_task \
    --include 'biapp.task_catalogue.dv.dwh.scv.00_event_stream_modelling.01_artificial_transaction_insert.py' \
    --method 'run' \
    --start '2025-01-10 00:00:00' \
    --end '2025-01-10 00:00:00'


------------------------------------------------------------------------------------------------------------------------
-- biapp/task_catalogue/dv/dwh/scv/00_event_stream_modelling/02_page_screen_enrichment.py

USE ROLE PERSONAL_ROLE__ROBINPATEL;

CREATE SCHEMA IF NOT EXISTS hygiene_vault_mvp_dev_robin.snowplow;

CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream
CLONE hygiene_vault_mvp.snowplow.event_stream;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.page_screen_enrichment
CLONE data_vault_mvp.single_customer_view_stg.page_screen_enrichment;

self_describing_task \
    --include 'biapp.task_catalogue.dv.dwh.scv.00_event_stream_modelling.02_page_screen_enrichment.py' \
    --method 'run' \
    --start '2025-01-10 00:00:00' \
    --end '2025-01-10 00:00:00'

------------------------------------------------------------------------------------------------------------------------

-- biapp/task_catalogue/dv/dwh/scv/00_event_stream_modelling/03_app_push_enhancement.py

USE ROLE PERSONAL_ROLE__ROBINPATEL;

CREATE SCHEMA IF NOT EXISTS hygiene_vault_mvp_dev_robin.snowplow;

CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream
CLONE hygiene_vault_mvp.snowplow.event_stream;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.app_push_send_enhancement
CLONE data_vault_mvp.single_customer_view_stg.app_push_send_enhancement;

self_describing_task \
    --include 'biapp.task_catalogue.dv.dwh.scv.00_event_stream_modelling.03_app_push_enhancement.py' \
    --method 'run' \
    --start '2025-01-10 00:00:00' \
    --end '2025-01-10 00:00:00'


------------------------------------------------------------------------------------------------------------------------

-- module=/biapp/task_catalogue/dv/dwh/scv/01_url_manipulation/01_module_unique_urls.py make clones

USE ROLE PERSONAL_ROLE__ROBINPATEL;

CREATE SCHEMA IF NOT EXISTS hygiene_vault_mvp_dev_robin.snowplow;

CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream
CLONE hygiene_vault_mvp.snowplow.event_stream;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_unique_urls
CLONE data_vault_mvp.single_customer_view_stg.module_unique_urls;

self_describing_task \
    --include 'biapp.task_catalogue.dv.dwh.scv.01_url_manipulation.01_module_unique_urls.py' \
    --method 'run' \
    --start '2025-01-10 00:00:00' \
    --end '2025-01-10 00:00:00'

------------------------------------------------------------------------------------------------------------------------

-- biapp/task_catalogue/dv/dwh/scv/01_url_manipulation/02_01_module_url_hostname.py

USE ROLE PERSONAL_ROLE__ROBINPATEL;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_unique_urls
CLONE data_vault_mvp.single_customer_view_stg.module_unique_urls;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_url_hostname
CLONE data_vault_mvp.single_customer_view_stg.module_url_hostname;

self_describing_task \
    --include 'biapp.task_catalogue.dv.dwh.scv.01_url_manipulation.02_01_module_url_hostname.py' \
    --method 'run' \
    --start '2025-01-10 00:00:00' \
    --end '2025-01-10 00:00:00'

------------------------------------------------------------------------------------------------------------------------

-- biapp/task_catalogue/dv/dwh/scv/01_url_manipulation/02_02_module_url_params.py

USE ROLE PERSONAL_ROLE__ROBINPATEL;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_unique_urls
CLONE data_vault_mvp.single_customer_view_stg.module_unique_urls;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_url_params
CLONE data_vault_mvp.single_customer_view_stg.module_url_params;

self_describing_task \
    --include 'biapp.task_catalogue.dv.dwh.scv.01_url_manipulation.02_02_module_url_params.py' \
    --method 'run' \
    --start '2025-01-10 00:00:00' \
    --end '2025-01-10 00:00:00'

------------------------------------------------------------------------------------------------------------------------

-- biapp/task_catalogue/dv/dwh/scv/01_url_manipulation/03_module_extracted_params.py

USE ROLE PERSONAL_ROLE__ROBINPATEL;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_url_params
CLONE data_vault_mvp.single_customer_view_stg.module_url_params;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_extracted_params
CLONE data_vault_mvp.single_customer_view_stg.module_extracted_params;

self_describing_task \
    --include 'biapp.task_catalogue.dv.dwh.scv.01_url_manipulation.03_module_extracted_params.py' \
    --method 'run' \
    --start '2025-01-10 00:00:00' \
    --end '2025-01-10 00:00:00'

------------------------------------------------------------------------------------------------------------------------

-- biapp/task_catalogue/dv/dwh/scv/02_identity_stitching/01_module_identity_associations.py

USE ROLE PERSONAL_ROLE__ROBINPATEL;

CREATE SCHEMA IF NOT EXISTS hygiene_vault_mvp_dev_robin.snowplow;

CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream
CLONE hygiene_vault_mvp.snowplow.event_stream;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.cms_mysql;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.external_booking
CLONE latest_vault.cms_mysql.external_booking;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_identity_associations
CLONE data_vault_mvp.single_customer_view_stg.module_identity_associations;

self_describing_task \
    --include 'biapp.task_catalogue.dv.dwh.scv.02_identity_stitching.01_module_identity_associations.py' \
    --method 'run' \
    --start '2025-01-10 00:00:00' \
    --end '2025-01-10 00:00:00'

------------------------------------------------------------------------------------------------------------------------

-- biapp/task_catalogue/dv/dwh/scv/02_identity_stitching/02_module_identity_stitching.py

USE ROLE PERSONAL_ROLE__ROBINPATEL;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_identity_associations
CLONE data_vault_mvp.single_customer_view_stg.module_identity_associations;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_identity_stitching
CLONE data_vault_mvp.single_customer_view_stg.module_identity_stitching;

self_describing_task \
    --include 'biapp.task_catalogue.dv.dwh.scv.02_identity_stitching.02_module_identity_stitching.py' \
    --method 'run' \
    --start '2025-01-10 00:00:00' \
    --end '2025-01-10 00:00:00'

------------------------------------------------------------------------------------------------------------------------

-- biapp/task_catalogue/dv/dwh/scv/03_touchification/01_touchifiable_events.py

USE ROLE PERSONAL_ROLE__ROBINPATEL;

CREATE SCHEMA IF NOT EXISTS hygiene_vault_mvp_dev_robin.snowplow;

CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream
CLONE hygiene_vault_mvp.snowplow.event_stream;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.app_push_send_enhancement
CLONE data_vault_mvp.single_customer_view_stg.app_push_send_enhancement;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchifiable_events
CLONE data_vault_mvp.single_customer_view_stg.module_touchifiable_events;

self_describing_task \
    --include 'biapp.task_catalogue.dv.dwh.scv.03_touchification.01_touchifiable_events.py' \
    --method 'run' \
    --start '2025-01-10 00:00:00' \
    --end '2025-01-10 00:00:00'

------------------------------------------------------------------------------------------------------------------------

-- biapp/task_catalogue/dv/dwh/scv/03_touchification/02_01_utm_or_referrer_hostname_marker.py

USE ROLE PERSONAL_ROLE__ROBINPATEL;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchifiable_events
CLONE data_vault_mvp.single_customer_view_stg.module_touchifiable_events;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_identity_stitching
CLONE data_vault_mvp.single_customer_view_stg.module_identity_stitching;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_extracted_params
CLONE data_vault_mvp.single_customer_view_stg.module_extracted_params;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_url_hostname
CLONE data_vault_mvp.single_customer_view_stg.module_url_hostname;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_utm_referrer_marker
CLONE data_vault_mvp.single_customer_view_stg.module_utm_referrer_marker;

self_describing_task \
    --include 'biapp.task_catalogue.dv.dwh.scv.03_touchification.02_01_utm_or_referrer_hostname_marker.py' \
    --method 'run' \
    --start '2025-01-10 00:00:00' \
    --end '2025-01-10 00:00:00'

------------------------------------------------------------------------------------------------------------------------

-- biapp/task_catalogue/dv/dwh/scv/03_touchification/02_02_time_diff_marker.py

USE ROLE PERSONAL_ROLE__ROBINPATEL;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchifiable_events
CLONE data_vault_mvp.single_customer_view_stg.module_touchifiable_events;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_identity_stitching
CLONE data_vault_mvp.single_customer_view_stg.module_identity_stitching;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_time_diff_marker
CLONE data_vault_mvp.single_customer_view_stg.module_time_diff_marker;

self_describing_task \
    --include 'biapp.task_catalogue.dv.dwh.scv.03_touchification.02_02_time_diff_marker.py' \
    --method 'run' \
    --start '2025-01-10 00:00:00' \
    --end '2025-01-10 00:00:00'


------------------------------------------------------------------------------------------------------------------------

-- biapp/task_catalogue/dv/dwh/scv/03_touchification/03_touchification.py

USE ROLE PERSONAL_ROLE__ROBINPATEL;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_identity_stitching
CLONE data_vault_mvp.single_customer_view_stg.module_identity_stitching;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_utm_referrer_marker
CLONE data_vault_mvp.single_customer_view_stg.module_utm_referrer_marker;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_time_diff_marker
CLONE data_vault_mvp.single_customer_view_stg.module_time_diff_marker;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchifiable_events
CLONE data_vault_mvp.single_customer_view_stg.module_touchifiable_events;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
CLONE data_vault_mvp.single_customer_view_stg.module_touchification;

self_describing_task \
    --include 'biapp.task_catalogue.dv.dwh.scv.03_touchification.03_touchification.py' \
    --method 'run' \
    --start '2025-01-10 00:00:00' \
    --end '2025-01-10 00:00:00'

------------------------------------------------------------------------------------------------------------------------

-- biapp/task_catalogue/dv/dwh/scv/04_events_of_interest/01_module_touched_spvs.py

USE ROLE PERSONAL_ROLE__ROBINPATEL;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
CLONE data_vault_mvp.single_customer_view_stg.module_touchification;

CREATE SCHEMA IF NOT EXISTS hygiene_vault_mvp_dev_robin.snowplow;

CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream
CLONE hygiene_vault_mvp.snowplow.event_stream;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_sale
CLONE data_vault_mvp.dwh.se_sale;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs
CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs;

self_describing_task \
    --include 'biapp.task_catalogue.dv.dwh.scv.04_events_of_interest.01_module_touched_spvs.py' \
    --method 'run' \
    --start '2025-01-10 00:00:00' \
    --end '2025-01-10 00:00:00'

------------------------------------------------------------------------------------------------------------------------

-- biapp/task_catalogue/dv/dwh/scv/04_events_of_interest/02_module_touched_transactions.py

USE ROLE PERSONAL_ROLE__ROBINPATEL;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions
CLONE data_vault_mvp.single_customer_view_stg.module_touched_transactions;

CREATE SCHEMA IF NOT EXISTS hygiene_vault_mvp_dev_robin.snowplow;

CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream
CLONE hygiene_vault_mvp.snowplow.event_stream;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions
CLONE data_vault_mvp.single_customer_view_stg.module_touched_transactions;

self_describing_task \
    --include 'biapp.task_catalogue.dv.dwh.scv.04_events_of_interest.02_module_touched_transactions.py' \
    --method 'run' \
    --start '2025-01-10 00:00:00' \
    --end '2025-01-10 00:00:00'

------------------------------------------------------------------------------------------------------------------------

-- biapp/task_catalogue/dv/dwh/scv/04_events_of_interest/03_module_touched_searches.py

USE ROLE PERSONAL_ROLE__ROBINPATEL;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
CLONE data_vault_mvp.single_customer_view_stg.module_touchification;

CREATE SCHEMA IF NOT EXISTS hygiene_vault_mvp_dev_robin.snowplow;

CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream
CLONE hygiene_vault_mvp.snowplow.event_stream;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_searches
CLONE data_vault_mvp.single_customer_view_stg.module_touched_searches;

self_describing_task \
    --include 'biapp.task_catalogue.dv.dwh.scv.04_events_of_interest.03_module_touched_searches.py' \
    --method 'run' \
    --start '2025-01-10 00:00:00' \
    --end '2025-01-10 00:00:00'

------------------------------------------------------------------------------------------------------------------------

-- biapp/task_catalogue/dv/dwh/scv/04_events_of_interest/04_module_touched_app_installs.py

USE ROLE PERSONAL_ROLE__ROBINPATEL;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
CLONE data_vault_mvp.single_customer_view_stg.module_touchification;

CREATE SCHEMA IF NOT EXISTS hygiene_vault_mvp_dev_robin.snowplow;

CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream
CLONE hygiene_vault_mvp.snowplow.event_stream;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_app_installs
CLONE data_vault_mvp.single_customer_view_stg.module_touched_app_installs;

self_describing_task \
    --include 'biapp.task_catalogue.dv.dwh.scv.04_events_of_interest.04_module_touched_app_installs.py' \
    --method 'run' \
    --start '2025-01-10 00:00:00' \
    --end '2025-01-10 00:00:00'

------------------------------------------------------------------------------------------------------------------------

-- biapp/task_catalogue/dv/dwh/scv/04_events_of_interest/08_module_touched_in_app_notification_events.py

USE ROLE PERSONAL_ROLE__ROBINPATEL;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
CLONE data_vault_mvp.single_customer_view_stg.module_touchification;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchifiable_events
CLONE data_vault_mvp.single_customer_view_stg.module_touchifiable_events;

CREATE SCHEMA IF NOT EXISTS hygiene_vault_mvp_dev_robin.snowplow;

CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream
CLONE hygiene_vault_mvp.snowplow.event_stream;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_in_app_notification_events
CLONE data_vault_mvp.single_customer_view_stg.module_touched_in_app_notification_events;

self_describing_task \
    --include 'biapp.task_catalogue.dv.dwh.scv.04_events_of_interest.08_module_touched_in_app_notification_events.py' \
    --method 'run' \
    --start '2025-01-10 00:00:00' \
    --end '2025-01-10 00:00:00'

------------------------------------------------------------------------------------------------------------------------

-- biapp/task_catalogue/dv/dwh/scv/06_touch_channelling/01_module_touch_utm_referrer.py

USE ROLE PERSONAL_ROLE__ROBINPATEL;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_extracted_params
CLONE data_vault_mvp.single_customer_view_stg.module_extracted_params;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_url_hostname
CLONE data_vault_mvp.single_customer_view_stg.module_url_hostname;

-- optional statement to create the module target table --
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_utm_referrer
CLONE data_vault_mvp.single_customer_view_stg.module_touch_utm_referrer;

self_describing_task \
    --include 'biapp.task_catalogue.dv.dwh.scv.06_touch_channelling.01_module_touch_utm_referrer.py' \
    --method 'run' \
    --start '2025-01-10 00:00:00' \
    --end '2025-01-10 00:00:00'

------------------------------------------------------------------------------------------------------------------------
