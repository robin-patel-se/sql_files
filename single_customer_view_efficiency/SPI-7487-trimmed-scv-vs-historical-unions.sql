-- module=/biapp/task_catalogue/scv_historical/event_stream_union/historical_event_stream.py make clones

CREATE SCHEMA IF NOT EXISTS single_customer_view_historical_dev_robin.event_stream_2025_07_01;

CREATE OR REPLACE TRANSIENT TABLE single_customer_view_historical_dev_robin.event_stream_2025_07_01.event_stream
CLONE single_customer_view_historical.event_stream_2025_07_01.event_stream;



-- module=/biapp/task_catalogue/scv_historical/scv_union/historical_page_screen_enrichment.py make clones


CREATE SCHEMA IF NOT EXISTS single_customer_view_historical_dev_robin.single_customer_view_2025_07_01;

CREATE OR REPLACE TRANSIENT TABLE single_customer_view_historical_dev_robin.single_customer_view_2025_07_01.page_screen_enrichment
CLONE single_customer_view_historical.single_customer_view_2025_07_01.page_screen_enrichment;


-- module=/biapp/task_catalogue/scv_historical/scv_union/historical_session_events_link.py make clones

CREATE SCHEMA IF NOT EXISTS single_customer_view_historical_dev_robin.single_customer_view_2025_07_01;

CREATE OR REPLACE TRANSIENT TABLE single_customer_view_historical_dev_robin.single_customer_view_2025_07_01.module_touchification
CLONE single_customer_view_historical.single_customer_view_2025_07_01.module_touchification;

-- module=/biapp/task_catalogue/scv_historical/scv_union/historical_touch_attribution.py make clones

CREATE SCHEMA IF NOT EXISTS single_customer_view_historical_dev_robin.single_customer_view_2025_07_01;

CREATE OR REPLACE TRANSIENT TABLE single_customer_view_historical_dev_robin.single_customer_view_2025_07_01.module_touch_attribution
CLONE single_customer_view_historical.single_customer_view_2025_07_01.module_touch_attribution;

-- module=/biapp/task_catalogue/scv_historical/scv_union/historical_touch_basic_attributes.py make clones

CREATE SCHEMA IF NOT EXISTS single_customer_view_historical_dev_robin.single_customer_view_2025_07_01;

CREATE OR REPLACE TRANSIENT TABLE single_customer_view_historical_dev_robin.single_customer_view_2025_07_01.module_touch_basic_attributes
CLONE single_customer_view_historical.single_customer_view_2025_07_01.module_touch_basic_attributes;

-- module=/biapp/task_catalogue/scv_historical/scv_union/historical_touch_events_of_interest.py make clones

CREATE SCHEMA IF NOT EXISTS single_customer_view_historical_dev_robin.single_customer_view_2025_07_01;

CREATE OR REPLACE TRANSIENT TABLE single_customer_view_historical_dev_robin.single_customer_view_2025_07_01.module_events_of_interest
CLONE single_customer_view_historical.single_customer_view_2025_07_01.module_events_of_interest;

-- module=/biapp/task_catalogue/scv_historical/scv_union/historical_touch_marketing_channel.py make clones

CREATE SCHEMA IF NOT EXISTS single_customer_view_historical_dev_robin.single_customer_view_2025_07_01;

CREATE OR REPLACE TRANSIENT TABLE single_customer_view_historical_dev_robin.single_customer_view_2025_07_01.module_touch_marketing_channel
CLONE single_customer_view_historical.single_customer_view_2025_07_01.module_touch_marketing_channel;

-- module=/biapp/task_catalogue/scv_historical/scv_union/historical_touched_app_installs.py make clones

CREATE SCHEMA IF NOT EXISTS single_customer_view_historical_dev_robin.single_customer_view_2025_07_01;

CREATE OR REPLACE TRANSIENT TABLE single_customer_view_historical_dev_robin.single_customer_view_2025_07_01.module_touched_app_installs
CLONE single_customer_view_historical.single_customer_view_2025_07_01.module_touched_app_installs;

-- module=/biapp/task_catalogue/scv_historical/scv_union/historical_touched_booking_form_views.py make clones

CREATE SCHEMA IF NOT EXISTS single_customer_view_historical_dev_robin.single_customer_view_2025_07_01;

CREATE OR REPLACE TRANSIENT TABLE single_customer_view_historical_dev_robin.single_customer_view_2025_07_01.module_touched_booking_form_views
CLONE single_customer_view_historical.single_customer_view_2025_07_01.module_touched_booking_form_views;

-- module=/biapp/task_catalogue/scv_historical/scv_union/historical_touched_feature_flags.py make clones

CREATE SCHEMA IF NOT EXISTS single_customer_view_historical_dev_robin.single_customer_view_2025_07_01;

CREATE OR REPLACE TRANSIENT TABLE single_customer_view_historical_dev_robin.single_customer_view_2025_07_01.module_touched_feature_flags
CLONE single_customer_view_historical.single_customer_view_2025_07_01.module_touched_feature_flags;

-- module=/biapp/task_catalogue/scv_historical/scv_union/historical_touched_in_app_notification_events.py make clones

CREATE SCHEMA IF NOT EXISTS single_customer_view_historical_dev_robin.single_customer_view_2025_07_01;

CREATE OR REPLACE TRANSIENT TABLE single_customer_view_historical_dev_robin.single_customer_view_2025_07_01.module_touched_in_app_notification_events
CLONE single_customer_view_historical.single_customer_view_2025_07_01.module_touched_in_app_notification_events;

-- module=/biapp/task_catalogue/scv_historical/scv_union/historical_touched_pay_button_clicks.py make clones

CREATE SCHEMA IF NOT EXISTS single_customer_view_historical_dev_robin.single_customer_view_2025_07_01;

CREATE OR REPLACE TRANSIENT TABLE single_customer_view_historical_dev_robin.single_customer_view_2025_07_01.module_touched_pay_button_clicks
CLONE single_customer_view_historical.single_customer_view_2025_07_01.module_touched_pay_button_clicks;

-- module=/biapp/task_catalogue/scv_historical/scv_union/historical_touched_searches.py make clones

CREATE SCHEMA IF NOT EXISTS single_customer_view_historical_dev_robin.single_customer_view_2025_07_01;

CREATE OR REPLACE TRANSIENT TABLE single_customer_view_historical_dev_robin.single_customer_view_2025_07_01.module_touched_searches
CLONE single_customer_view_historical.single_customer_view_2025_07_01.module_touched_searches;

-- module=/biapp/task_catalogue/scv_historical/scv_union/historical_touched_spv.py make clones

CREATE SCHEMA IF NOT EXISTS single_customer_view_historical_dev_robin.single_customer_view_2025_07_01;

CREATE OR REPLACE TRANSIENT TABLE single_customer_view_historical_dev_robin.single_customer_view_2025_07_01.module_touched_spvs
CLONE single_customer_view_historical.single_customer_view_2025_07_01.module_touched_spvs;

-- module=/biapp/task_catalogue/scv_historical/scv_union/historical_touched_transactions.py make clones

CREATE SCHEMA IF NOT EXISTS single_customer_view_historical_dev_robin.single_customer_view_2025_07_01;

CREATE OR REPLACE TRANSIENT TABLE single_customer_view_historical_dev_robin.single_customer_view_2025_07_01.module_touched_transactions
CLONE single_customer_view_historical.single_customer_view_2025_07_01.module_touched_transactions;



self_describing_task --include 'scv_historical/event_stream_union/historical_event_stream.py'  --method 'run' --start '2025-07-01 00:00:00' --end '2025-07-01 00:00:00'
self_describing_task --include 'scv_historical/scv_union/historical_page_screen_enrichment.py'  --method 'run' --start '2025-07-01 00:00:00' --end '2025-07-01 00:00:00'
self_describing_task --include 'scv_historical/scv_union/historical_session_events_link.py'  --method 'run' --start '2025-07-01 00:00:00' --end '2025-07-01 00:00:00'
self_describing_task --include 'scv_historical/scv_union/historical_touch_attribution.py'  --method 'run' --start '2025-07-01 00:00:00' --end '2025-07-01 00:00:00'
self_describing_task --include 'scv_historical/scv_union/historical_touch_basic_attributes.py'  --method 'run' --start '2025-07-01 00:00:00' --end '2025-07-01 00:00:00'
self_describing_task --include 'scv_historical/scv_union/historical_touch_events_of_interest.py'  --method 'run' --start '2025-07-01 00:00:00' --end '2025-07-01 00:00:00'
self_describing_task --include 'scv_historical/scv_union/historical_touch_marketing_channel.py'  --method 'run' --start '2025-07-01 00:00:00' --end '2025-07-01 00:00:00'
self_describing_task --include 'scv_historical/scv_union/historical_touched_app_installs.py'  --method 'run' --start '2025-07-01 00:00:00' --end '2025-07-01 00:00:00'
self_describing_task --include 'scv_historical/scv_union/historical_touched_booking_form_views.py'  --method 'run' --start '2025-07-01 00:00:00' --end '2025-07-01 00:00:00'
self_describing_task --include 'scv_historical/scv_union/historical_touched_feature_flags.py'  --method 'run' --start '2025-07-01 00:00:00' --end '2025-07-01 00:00:00'
self_describing_task --include 'scv_historical/scv_union/historical_touched_in_app_notification_events.py'  --method 'run' --start '2025-07-01 00:00:00' --end '2025-07-01 00:00:00'
self_describing_task --include 'scv_historical/scv_union/historical_touched_pay_button_clicks.py'  --method 'run' --start '2025-07-01 00:00:00' --end '2025-07-01 00:00:00'
self_describing_task --include 'scv_historical/scv_union/historical_touched_searches.py'  --method 'run' --start '2025-07-01 00:00:00' --end '2025-07-01 00:00:00'
self_describing_task --include 'scv_historical/scv_union/historical_touched_spv.py'  --method 'run' --start '2025-07-01 00:00:00' --end '2025-07-01 00:00:00'
self_describing_task --include 'scv_historical/scv_union/historical_touched_transactions.py'  --method 'run' --start '2025-07-01 00:00:00' --end '2025-07-01 00:00:00'



