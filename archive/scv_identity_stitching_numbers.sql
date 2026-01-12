--on may 20th
SELECT COUNT(*) FROM snowplow.atomic.events;
--all atomic events

SELECT COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touchification;
--count of events sessionised, 1774057307

SELECT COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touchification t
         INNER JOIN hygiene_vault_mvp.snowplow.event_stream e ON e.event_hash = t.event_hash
WHERE t.stitched_identity_type = 'se_user_id';
--count of events with a user id assigned --1637045088

SELECT COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touchification t
         INNER JOIN hygiene_vault_mvp.snowplow.event_stream e ON e.event_hash = t.event_hash
WHERE t.stitched_identity_type = 'se_user_id'
AND e.se_user_id IS NULL;
--count of events with a user id assigned where the event stream didn't have one -- 96034418