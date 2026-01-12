USE WAREHOUSE pipe_xlarge;


SELECT max(event_tstamp)
FROM collab.data.key_metrics_rec_stg_mongo__events_collection
LIMIT 100;

SELECT DISTINCT page_host_brand
FROM collab.data.key_metrics_rec_stg_mongo__events_collection
LIMIT 100;



SELECT e.event_tstamp,
       e.page_url
FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs s
         INNER JOIN hygiene_vault_mvp.snowplow.event_stream e ON s.event_hash = e.event_hash
WHERE e.event_tstamp >= '2020-02-28'
  AND e.event_tstamp <= '2020-03-03';



SELECT *
FROM raw_vault_mvp.travelbird_catalogue.sale_visits_by_state_and_date;
<--

SELECT e.event_tstamp::DATE,
       e.page_url,
       s.se_sale_id
FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs s
         INNER JOIN hygiene_vault_mvp.snowplow.event_stream e ON s.event_hash = e.event_hash
WHERE e.event_tstamp >= '2020-02-28'
  AND e.event_tstamp <= '2020-03-15'
  AND e.v_tracker LIKE 'py-%';

SELECT e.event_tstamp::DATE,
       count(*)
FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs s
         INNER JOIN hygiene_vault_mvp.snowplow.event_stream e ON s.event_hash = e.event_hash
WHERE e.event_tstamp >= '2020-02-28'
  AND e.event_tstamp <= '2020-03-15'
  AND e.v_tracker LIKE 'py-%'
GROUP BY 1;


SELECT date,
       sum(user_visits)
FROM raw_vault_mvp.travelbird_catalogue.sale_visits_by_state_and_date
WHERE date >= '2020-01-01'
GROUP BY 1;

SELECT DISTINCT sale_id
FROM raw_vault_mvp.travelbird_catalogue.sale_visits_by_state_and_date
WHERE date >= '2020-02-28'
