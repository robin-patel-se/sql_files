USE WAREHOUSE pipe_xlarge;

SELECT event_tstamp::DATE,
       count(*)                                                      AS spvs,
       sum(CASE WHEN is_robot_spider_event = TRUE THEN 1 ELSE 0 END) AS robot_spvs
FROM hygiene_vault_mvp.snowplow.event_stream e
WHERE event_tstamp >= '2020-02-28'
  AND v_tracker LIKE 'py-%'
  AND (--line in sand between client side and server side tracking
        (--client side tracking, prior implementation/validation
                e.collector_tstamp < '2020-02-28 00:00:00'
                AND (
                        e.page_urlpath LIKE '%/sale'
                        OR
                        e.page_urlpath LIKE '%/sale-%' --eg. /sale-ihp, /sale-hotel for connected sales
                    -- need to adjust for new definitions of spv e.g. travel bird booking flow
                    )
                AND
                e.device_platform != 'native app' --explicitly remove native app (as app offer pages appear like web SPVs)
                AND e.is_server_side_event = FALSE -- exclude non validated ss events
            )
        OR
        (--server side tracking, post implementation/validation
                e.collector_tstamp >= '2020-02-28 00:00:00'
                AND e.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR = 'sale'
                AND
                e.device_platform != 'native app' --explicitly remove native app (as app offer pages appear like web SPVs)
            --TODO need to adjust this for when TB product goes live on native app, we will need to count the wrapped mweb
            --product
                AND e.is_server_side_event = TRUE
            )
    )
GROUP BY 1;


SELECT event_tstamp::DATE,
       se_sale_id,
       count(*)                                                      AS spvs,
       sum(CASE WHEN is_robot_spider_event = TRUE THEN 1 ELSE 0 END) AS robot_spvs
FROM hygiene_vault_mvp.snowplow.event_stream e
WHERE event_tstamp >= '2020-02-28'
  AND v_tracker LIKE 'py-%'
  AND (--line in sand between client side and server side tracking
        (--client side tracking, prior implementation/validation
                e.collector_tstamp < '2020-02-28 00:00:00'
                AND (
                        e.page_urlpath LIKE '%/sale'
                        OR
                        e.page_urlpath LIKE '%/sale-%' --eg. /sale-ihp, /sale-hotel for connected sales
                    -- need to adjust for new definitions of spv e.g. travel bird booking flow
                    )
                AND
                e.device_platform != 'native app' --explicitly remove native app (as app offer pages appear like web SPVs)
                AND e.is_server_side_event = FALSE -- exclude non validated ss events
            )
        OR
        (--server side tracking, post implementation/validation
                e.collector_tstamp >= '2020-02-28 00:00:00'
                AND e.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR = 'sale'
                AND
                e.device_platform != 'native app' --explicitly remove native app (as app offer pages appear like web SPVs)
            --TODO need to adjust this for when TB product goes live on native app, we will need to count the wrapped mweb
            --product
                AND e.is_server_side_event = TRUE
            )
    )
GROUP BY 1, 2;