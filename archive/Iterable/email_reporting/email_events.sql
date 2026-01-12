SELECT eb.bounce_message,
       eb.campaign_id,
       eb.event_created_at,
       eb.email,
       eb.message_id,
       eb.recipient_state,
       eb.template_id,
       eb.record
FROM latest_vault.iterable.email_bounce eb;

DROP TABLE latest_vault_dev_robin.iterable.email_bounce;
CREATE OR REPLACE VIEW latest_vault_dev_robin.iterable.email_bounce AS
SELECT *
FROM latest_vault.iterable.email_bounce;

self_describing_task --include 'dv/dwh/email/email_events/email_bounce_event.py'  --method 'run' --start '2021-12-08 00:00:00' --end '2021-12-08 00:00:00'

SELECT *
FROM data_vault_mvp_dev_robin.dwh.email_bounce_event;

self_describing_task --include 'se/data/crm/crm_events_bounces.py'  --method 'run' --start '2021-12-08 00:00:00' --end '2021-12-08 00:00:00'
self_describing_task --include 'se/data_pii/crm/crm_events_bounces.py'  --method 'run' --start '2021-12-08 00:00:00' --end '2021-12-08 00:00:00'


SELECT *
FROM data_vault_mvp_dev_robin.dwh.email_bounce_event;

------------------------------------------------------------------------------------------------------------------------
SELECT ec.campaign_id,
       ec.event_created_at,
       ec.email,
       ec.message_id,
       ec.recipient_state,
       ec.template_id,
       ec.record
FROM latest_vault.iterable.email_complaint ec

------------------------------------------------------------------------------------------------------------------------

SELECT * FROM