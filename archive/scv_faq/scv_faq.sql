SELECT * FROM hygiene_vault_mvp.snowplow.event_stream

SELECT DISTINCT  touch_start_tstamp FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;

SELECT * FROM se.data.scv

SELECT * FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs
WHERE CURRENT_DATE BETWEEN start_date AND end_date;

SELECT * FROM se.data.dim_sale;
SELECT * FROM se.data.fact_complete_booking;