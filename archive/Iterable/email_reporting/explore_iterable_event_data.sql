-- job lists

SELECT *
FROM hygiene_snapshot_vault_mvp.sfmc.jobs_list jl;

-- where's this in iterable?

------------------------------------------------------------------------------------------------------------------------
--send events
SELECT *
FROM hygiene_snapshot_vault_mvp.sfmc.events_sends es;

SELECT es.campaign_id, -- the equivalent of send_id
       es.catalog_collection_count,
       es.catalog_lookup_count,
       es.channel_id,  --  what is this? (marketing or transactional)
       es.content_id,
       es.event_created_at,
       es.email,
       es.event_name,
       es.message_bus_id,
       es.message_id,
       es.message_type_id,
       es.product_recommendation_count,
       es.template_id,
       es.transactional_data,
       ua.shiro_user_id
FROM latest_vault.iterable.email_send es
    LEFT JOIN data_vault_mvp.dwh.user_attributes ua ON LOWER(es.email) = LOWER(ua.email)
-- WHERE ua.email IS NULL
;

-- email
-- event_created_at
-- message_id - what is this?
-- message_type_id - what is this?

------------------------------------------------------------------------------------------------------------------------

-- open events
SELECT *
FROM hygiene_snapshot_vault_mvp.sfmc.events_opens_plus_inferred eopi;

SELECT eo.campaign_id,
       eo.city,
       eo.country,
       eo.event_created_at,
       eo.email,
       eo.event_name,
       eo.ip,
       eo.message_id,
       eo.region,
       eo.template_id,
       eo.user_agent,
       eo.user_agent_device,
       ua.shiro_user_id
FROM latest_vault.iterable.email_open eo
    INNER JOIN data_vault_mvp.dwh.user_attributes ua ON LOWER(eo.email) = LOWER(ua.email);


------------------------------------------------------------------------------------------------------------------------

-- click events

SELECT *
FROM hygiene_snapshot_vault_mvp.sfmc.events_clicks ec;


SELECT ec.campaign_id,
       ec.city,
       ec.content_id,
       ec.country,
       ec.event_created_at,
       ec.email,
       ec.event_name,
       ec.href_index,
       ec.ip,
       ec.message_id,
       ec.region,
       ec.template_id,
       ec.url,
       ec.user_agent,
       ec.user_agent_device,
       ua.shiro_user_id
FROM latest_vault.iterable.email_click ec
    INNER JOIN data_vault_mvp.dwh.user_attributes ua ON LOWER(ec.email) = LOWER(ua.email);


------------------------------------------------------------------------------------------------------------------------

-- unsubscribe events

SELECT *
FROM hygiene_snapshot_vault_mvp.sfmc.events_unsubscribes eu;


SELECT eu.bounce_message,
       eu.campaign_id,
       eu.channel_ids,
       eu.event_created_at,
       eu.email,
       eu.email_list_ids,
       eu.event_name,
       eu.message_id,
       eu.recipient_state,
       eu.template_id,
       eu.unsub_source,
       ua.shiro_user_id
FROM latest_vault.iterable.email_unsubscribe eu
    INNER JOIN data_vault_mvp.dwh.user_attributes ua ON LOWER(eu.email) = LOWER(ua.email);

-- note a lot of unsubscribe events without a campaign id


------------------------------------------------------------------------------------------------------------------------

--distinct list of campaign ids
CREATE OR REPLACE VIEW collab.iterable_data.email_performance COPY GRANTS AS
(
WITH grain AS (
    SELECT DISTINCT es.campaign_id, es.event_created_at::DATE AS date
    FROM latest_vault.iterable.email_send es

    UNION

    SELECT DISTINCT eo.campaign_id, eo.event_created_at::DATE AS date
    FROM latest_vault.iterable.email_open eo

    UNION
    SELECT DISTINCT ec.campaign_id, ec.event_created_at::DATE AS date
    FROM latest_vault.iterable.email_click ec

    UNION

    SELECT DISTINCT eu.campaign_id, eu.event_created_at::DATE AS date
    FROM latest_vault.iterable.email_unsubscribe eu
    WHERE eu.campaign_id IS NOT NULL

    UNION

    SELECT DISTINCT eb.campaign_id, eb.event_created_at::DATE AS date
    FROM latest_vault.iterable.email_bounce eb

    UNION

    SELECT DISTINCT ecp.campaign_id, ecp.event_created_at::DATE AS date
    FROM latest_vault.iterable.email_complaint ecp

    UNION

    SELECT DISTINCT ess.campaign_id, ess.event_created_at::DATE AS date
    FROM latest_vault.iterable.email_send_skip ess
),
     agg_sends AS (
         SELECT es.event_created_at::DATE        AS send_date,
                es.campaign_id,
                COUNT(*)                         AS sends,
                COUNT(DISTINCT ua.shiro_user_id) AS unique_sends
         FROM latest_vault.iterable.email_send es
             INNER JOIN data_vault_mvp.dwh.user_attributes ua ON LOWER(es.email) = LOWER(ua.email)
         GROUP BY 1, 2

     ),
     agg_opens AS (
         SELECT eo.event_created_at::DATE        AS open_date,
                eo.campaign_id,
                COUNT(*)                         AS opens,
                COUNT(DISTINCT ua.shiro_user_id) AS unique_opens
         FROM latest_vault.iterable.email_open eo
             INNER JOIN data_vault_mvp.dwh.user_attributes ua ON LOWER(eo.email) = LOWER(ua.email)
         GROUP BY 1, 2
     ),
     agg_clicks AS (
         SELECT ec.event_created_at::DATE        AS click_date,
                ec.campaign_id,
                COUNT(*)                         AS clicks,
                COUNT(DISTINCT ua.shiro_user_id) AS unique_clicks
         FROM latest_vault.iterable.email_click ec
             INNER JOIN data_vault_mvp.dwh.user_attributes ua ON LOWER(ec.email) = LOWER(ua.email)
         GROUP BY 1, 2
     ),
     agg_unsubscribes AS (
         SELECT eu.event_created_at::DATE        AS unsubscribe_date,
                eu.campaign_id,
                COUNT(*)                         AS unsubscribes,
                COUNT(DISTINCT ua.shiro_user_id) AS unique_unsubscribes
         FROM latest_vault.iterable.email_unsubscribe eu
             INNER JOIN data_vault_mvp.dwh.user_attributes ua ON LOWER(eu.email) = LOWER(ua.email)
         WHERE eu.campaign_id IS NOT NULL
         GROUP BY 1, 2
     ),
     agg_bounces AS (
         SELECT eb.event_created_at::DATE        AS bounce_date,
                eb.campaign_id,
                COUNT(*)                         AS bounces,
                COUNT(DISTINCT ua.shiro_user_id) AS unique_bounces
         FROM latest_vault.iterable.email_bounce eb
             INNER JOIN data_vault_mvp.dwh.user_attributes ua ON LOWER(eb.email) = LOWER(ua.email)
         GROUP BY 1, 2
     ),
     agg_complaints AS (
         SELECT ecp.event_created_at::DATE       AS complaint_date,
                ecp.campaign_id,
                COUNT(*)                         AS complaints,
                COUNT(DISTINCT ua.shiro_user_id) AS unique_complaints
         FROM latest_vault.iterable.email_complaint ecp
             INNER JOIN data_vault_mvp.dwh.user_attributes ua ON LOWER(ecp.email) = LOWER(ua.email)
         GROUP BY 1, 2
     ),
     agg_send_skip AS (
         SELECT ess.event_created_at::DATE       AS send_skip_date,
                ess.campaign_id,
                COUNT(*)                         AS send_skips,
                COUNT(DISTINCT ua.shiro_user_id) AS unique_send_skips
         FROM latest_vault.iterable.email_send_skip ess
             INNER JOIN data_vault_mvp.dwh.user_attributes ua ON LOWER(ess.email) = LOWER(ua.email)
         GROUP BY 1, 2
     )
SELECT g.campaign_id,
       g.date,
       c.name as email_name,
       c.mapped_crm_date,
       c.mapped_territory,
       c.mapped_objective,
       c.mapped_platform,
       c.mapped_campaign,
       ags.sends,
       ags.unique_sends,
       ago.opens,
       ago.unique_opens,
       agc.clicks,
       agc.unique_clicks,
       agu.unsubscribes,
       agu.unique_unsubscribes,
       agb.bounces,
       agb.unique_bounces,
       agcp.complaints,
       agcp.unique_complaints,
       agss.send_skips,
       agss.unique_send_skips
FROM grain g
    LEFT JOIN latest_vault.iterable.campaign c ON g.campaign_id = c.id
    LEFT JOIN agg_sends ags ON g.campaign_id = ags.campaign_id AND g.date = ags.send_date
    LEFT JOIN agg_opens ago ON g.campaign_id = ago.campaign_id AND g.date = ago.open_date
    LEFT JOIN agg_clicks agc ON g.campaign_id = agc.campaign_id AND g.date = agc.click_date
    LEFT JOIN agg_unsubscribes agu ON g.campaign_id = agu.campaign_id AND g.date = agu.unsubscribe_date
    LEFT JOIN agg_bounces agb ON g.campaign_id = agb.campaign_id AND g.date = agb.bounce_date
    LEFT JOIN agg_complaints agcp ON g.campaign_id = agcp.campaign_id AND g.date = agcp.complaint_date
    LEFT JOIN agg_send_skip agss ON g.campaign_id = agss.campaign_id AND g.date = agss.send_skip_date
    )
;

SELECT *
FROM collab.iterable_data.email_performance ep;

SELECT ep.campaign_id,
       ep.mapped_crm_date,
       ep.mapped_territory,
       ep.mapped_objective,
       ep.mapped_platform,
       ep.mapped_campaign,
       SUM(ep.sends)               AS sends,
       SUM(ep.unique_sends)        AS unique_sends,
       SUM(ep.opens)               AS opens,
       SUM(ep.unique_opens)        AS unique_opens,
       SUM(ep.clicks)              AS clicks,
       SUM(ep.unique_clicks)       AS unique_clicks,
       SUM(ep.unsubscribes)        AS unsubscribes,
       SUM(ep.unique_unsubscribes) AS unique_unsubscribes,
       SUM(ep.bounces)             AS bounces,
       SUM(ep.unique_bounces)      AS unique_bounces,
       SUM(ep.complaints)          AS complaints,
       SUM(ep.unique_complaints)   AS unique_complaints,
       SUM(ep.send_skips)          AS send_skips,
       SUM(ep.unique_send_skips)   AS unique_send_skips
FROM collab.iterable_data.email_performance ep
GROUP BY 1, 2, 3, 4, 5, 6;


SELECT ep.date,
       SUM(ep.sends)               AS sends,
       SUM(ep.unique_sends)        AS unique_sends,
       SUM(ep.opens)               AS opens,
       SUM(ep.unique_opens)        AS unique_opens,
       SUM(ep.clicks)              AS clicks,
       SUM(ep.unique_clicks)       AS unique_clicks,
       SUM(ep.unsubscribes)        AS unsubscribes,
       SUM(ep.unique_unsubscribes) AS unique_unsubscribes,
       SUM(ep.bounces)             AS bounces,
       SUM(ep.unique_bounces)      AS unique_bounces,
       SUM(ep.complaints)          AS complaints,
       SUM(ep.unique_complaints)   AS unique_complaints,
       SUM(ep.send_skips)          AS send_skips,
       SUM(ep.unique_send_skips)   AS unique_send_skips
FROM collab.iterable_data.email_performance ep
GROUP BY 1;


GRANT SELECT ON TABLE collab.iterable_data.email_performance TO ROLE personal_role__jenniferbirks;
GRANT SELECT ON TABLE collab.iterable_data.email_performance TO ROLE data_team_basic;
GRANT SELECT ON TABLE collab.iterable_data.email_performance TO ROLE personal_role__claraduta;
GRANT SELECT ON TABLE collab.iterable_data.email_performance TO ROLE personal_role__apoorvakapavarapu;
GRANT SELECT ON TABLE collab.iterable_data.email_performance TO ROLE personal_role__bendeavin;


SELECT *
FROM latest_vault.iterable.campaign c;