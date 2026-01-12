USE WAREHOUSE pipe_xlarge;
USE SCHEMA data_vault_mvp.single_customer_view_stg;

--found that overall spv tracking has increased by 10-15% following switch to server side, investigating the
-- accuracy of the number

--comparison of cs and ss spvs
SELECT e.event_tstamp::DATE,
       SUM(CASE WHEN e.is_server_side_event = TRUE THEN 1 END)  AS ss_spvs,
       SUM(CASE WHEN e.is_server_side_event = FALSE THEN 1 END) AS cs_spvs,
       count(*)                                                 AS spvs
FROM hygiene_vault_mvp.snowplow.event_stream e
WHERE e.event_name = 'page_view'
  AND e.se_sale_id IS NOT NULL
  AND e.event_tstamp >= '2020-02-28 00:00:00'
  AND e.is_robot_spider_event = FALSE
  AND (
            (--cs spvs
                    e.page_urlpath LIKE '%/sale'
                    OR
                    e.page_urlpath LIKE '%/sale-%' --eg. /sale-ihp, /sale-hotel for connected sales
                -- need to adjust for new definitions of spv e.g. travel bird booking flow
                )
            AND
            e.device_platform != 'native app' --explicitly remove native app (as app offer pages appear like web SPVs)
            AND e.is_server_side_event = FALSE -- exclude non validated ss events

        OR
            (--ss spvs
                        e.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR = 'sale'
                    AND
                        e.device_platform != 'native app' --explicitly remove native app (as app offer pages appear like web SPVs)
                --TODO need to adjust this for when TB product goes live on native app, we will need to count the wrapped mweb
                --product
                    AND e.is_server_side_event = TRUE)
    )
GROUP BY 1
ORDER BY 1
;

------------------------------------------------------------------------------------------------------------------------
--need to strip out tb and wl
SELECT u.url,
       u.url_hostname,
       u.url_medium,
       e.*
FROM hygiene_vault_mvp.snowplow.event_stream e
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_url_hostname u ON e.page_url = u.url
WHERE e.event_name = 'page_view'
  AND e.se_sale_id IS NOT NULL
  AND e.event_tstamp >= '2020-02-28 00:00:00'
  AND e.is_robot_spider_event = FALSE
  AND (--ss spvs
            e.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR = 'sale'
        AND
            e.device_platform != 'native app' --explicitly remove native app (as app offer pages appear like web SPVs)
    --TODO need to adjust this for when TB product goes live on native app, we will need to count the wrapped mweb
    --product
        AND e.is_server_side_event = TRUE);

SELECT DISTINCT u.url_hostname,
                u.url_medium
FROM hygiene_vault_mvp.snowplow.event_stream e
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_url_hostname u ON e.page_url = u.url
WHERE e.event_name = 'page_view'
  AND e.se_sale_id IS NOT NULL
  AND e.event_tstamp >= '2020-02-28 00:00:00'
  AND e.is_robot_spider_event = FALSE
  AND (
            (--cs spvs
                    e.page_urlpath LIKE '%/sale'
                    OR
                    e.page_urlpath LIKE '%/sale-%' --eg. /sale-ihp, /sale-hotel for connected sales
                -- need to adjust for new definitions of spv e.g. travel bird booking flow
                )
            AND
            e.device_platform != 'native app' --explicitly remove native app (as app offer pages appear like web SPVs)
            AND e.is_server_side_event = FALSE -- exclude non validated ss events

        OR
            (--ss spvs
                        e.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR = 'sale'
                    AND
                        e.device_platform != 'native app' --explicitly remove native app (as app offer pages appear like web SPVs)
                --TODO need to adjust this for when TB product goes live on native app, we will need to count the wrapped mweb
                --product
                    AND e.is_server_side_event = TRUE)
    )
  --need to compare cs and ss like for like so need to strip out wl and tb
  AND u.url_hostname NOT IN (
    --remove travelbird
                             'sales.travelbird.dk',
                             'travelbird.ch',
                             'travelbird.dk',
                             'sales.travelbird.be',
                             'travelbird.at',
                             'travelbird.be',
                             'fr.travelbird.be',
                             'sales.travelbird.de',
                             'sales.travelbird.nl',
                             'travelbird.fi',
                             'livetest.sales.travelbird.nl',
                             'travelbird.de',
                             'sales.fr.travelbird.be',
                             'travelbird.no',
                             'travelbird.nl',
                             'travelbird.se',
                             'travelbird.fr',

    --remove travelist
                             'oferty.travelist.pl',
                             'zagranica.travelist.pl',

    --remove pigsback
                             'holidays.pigsback.com',
    --remove unclassified WLs
                             'teletext.secretescapes.com',
                             'escapes.travelbook.de',
                             'independent.secretescapes.com',
                             'escapes.jetsetter.com'
    )
  --remove staging/dev
  AND u.url_hostname NOT REGEXP '((web\\d\\d|applitool-affiliate|api)\\..*|.*\\.tech)'
  --remove additional unclassified url hostnames and classified whitelabels
  AND u.url_medium != 'unknown'
  AND u.url_medium != 'whitelabel'
;


------------------------------------------------------------------------------------------------------------------------
--ss vs cs aggregate numbers by hostname
SELECT e.event_tstamp::DATE,
       u.url_hostname,
       SUM(CASE WHEN e.is_server_side_event = TRUE THEN 1 END)  AS ss_spvs,
       SUM(CASE WHEN e.is_server_side_event = FALSE THEN 1 END) AS cs_spvs,
       count(*)                                                 AS spvs
FROM hygiene_vault_mvp.snowplow.event_stream e
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_url_hostname u ON e.page_url = u.url
WHERE e.event_name = 'page_view'
  AND e.se_sale_id IS NOT NULL
  AND e.event_tstamp >= '2020-02-28 00:00:00'
  AND e.is_robot_spider_event = FALSE

  AND (
            (--cs spvs
                    e.page_urlpath LIKE '%/sale'
                    OR
                    e.page_urlpath LIKE '%/sale-%' --eg. /sale-ihp, /sale-hotel for connected sales
                -- need to adjust for new definitions of spv e.g. travel bird booking flow
                )
            AND
            e.device_platform != 'native app' --explicitly remove native app (as app offer pages appear like web SPVs)
            AND e.is_server_side_event = FALSE -- exclude non validated ss events

        OR
            (--ss spvs
                        e.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR = 'sale'
                    AND
                        e.device_platform != 'native app' --explicitly remove native app (as app offer pages appear like web SPVs)
                --TODO need to adjust this for when TB product goes live on native app, we will need to count the wrapped mweb
                --product
                    AND e.is_server_side_event = TRUE)
    )
  --need to compare cs and ss like for like so need to strip out wl and tb
  AND u.url_hostname NOT IN (
    --remove travelbird
                             'sales.travelbird.dk',
                             'travelbird.ch',
                             'travelbird.dk',
                             'sales.travelbird.be',
                             'travelbird.at',
                             'travelbird.be',
                             'fr.travelbird.be',
                             'sales.travelbird.de',
                             'sales.travelbird.nl',
                             'travelbird.fi',
                             'livetest.sales.travelbird.nl',
                             'travelbird.de',
                             'sales.fr.travelbird.be',
                             'travelbird.no',
                             'travelbird.nl',
                             'travelbird.se',
                             'travelbird.fr',

    --remove travelist
                             'oferty.travelist.pl',
                             'zagranica.travelist.pl',

    --remove pigsback
                             'holidays.pigsback.com',
    --remove unclassified WLs
                             'teletext.secretescapes.com',
                             'escapes.travelbook.de',
                             'independent.secretescapes.com',
                             'escapes.jetsetter.com'
    )
  --remove staging/dev
  AND u.url_hostname NOT REGEXP '((web\\d\\d|applitool-affiliate|api)\\..*|.*\\.tech)'
  --remove additional unclassified url hostnames and classified whitelabels
  AND u.url_medium != 'unknown'
  AND u.url_medium != 'whitelabel'

GROUP BY 1, 2
ORDER BY 1
;

------------------------------------------------------------------------------------------------------------------------
--inspecting events for users

SELECT e.is_server_side_event,
       e.device_platform,
       e.event_tstamp,
       e.se_user_id,
       e.page_url,
       e.unique_browser_id,
       e.cookie_id,
       e.se_sale_id
FROM hygiene_vault_mvp.snowplow.event_stream e
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_url_hostname u ON e.page_url = u.url
WHERE e.event_name = 'page_view'
  AND e.se_sale_id IS NOT NULL
  AND e.event_tstamp >= '2020-02-28 00:00:00'
  AND e.is_robot_spider_event = FALSE

  AND (
            (--cs spvs
                    e.page_urlpath LIKE '%/sale'
                    OR
                    e.page_urlpath LIKE '%/sale-%' --eg. /sale-ihp, /sale-hotel for connected sales
                -- need to adjust for new definitions of spv e.g. travel bird booking flow
                )
            AND
            e.device_platform != 'native app' --explicitly remove native app (as app offer pages appear like web SPVs)
            AND e.is_server_side_event = FALSE -- exclude non validated ss events

        OR
            (--ss spvs
                        e.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR = 'sale'
                    AND
                        e.device_platform != 'native app' --explicitly remove native app (as app offer pages appear like web SPVs)
                --TODO need to adjust this for when TB product goes live on native app, we will need to count the wrapped mweb
                --product
                    AND e.is_server_side_event = TRUE)
    )
  --check se.com
  AND u.url_hostname = 'www.secretescapes.com'
  --check a handful of user events
  AND se_user_id IN ('29633649'
    )
ORDER BY page_url, event_tstamp;


SELECT 'https://www.secretescapes.com/heythrop-park-chipping-norton-oxfordshire/sale?email=&dayExperiences=&checkin=25%2F04%2F2020&checkout=26%2F04%2F2020',
       REGEXP_REPLACE(REGEXP_REPLACE(
                              'https://www.secretescapes.com/heythrop-park-chipping-norton-oxfordshire/sale?email=&dayExperiences=&checkin=25%2F04%2F2020&checkout=26%2F04%2F2020',
                              '%2F', '%252F'), '%3D', '%253D');

------------------------------------------------------------------------------------------------------------------------


SELECT se_user_id,
       --REGEXP_REPLACE(REGEXP_REPLACE(page_url, '%2F', '%252F'), '%3D', '%253D') as page_url,
--        date_trunc(DAY, event_tstamp)                          AS day,
       SUM(CASE WHEN is_server_side_event = TRUE THEN 1 ELSE 0 END)  AS ss_spvs,
       SUM(CASE WHEN is_server_side_event = FALSE THEN 1 ELSE 0 END) AS cs_spvs,
       ss_spvs - cs_spvs                                             AS diff


FROM hygiene_vault_mvp.snowplow.event_stream e
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_url_hostname u ON e.page_url = u.url
WHERE e.event_name = 'page_view'
  AND e.se_sale_id IS NOT NULL
  AND e.event_tstamp >= '2020-02-28 00:00:00'
  AND e.is_robot_spider_event = FALSE

  AND (
            (--cs spvs
                    e.page_urlpath LIKE '%/sale'
                    OR
                    e.page_urlpath LIKE '%/sale-%' --eg. /sale-ihp, /sale-hotel for connected sales
                -- need to adjust for new definitions of spv e.g. travel bird booking flow
                )
            AND
            e.device_platform != 'native app' --explicitly remove native app (as app offer pages appear like web SPVs)
            AND e.is_server_side_event = FALSE -- exclude non validated ss events

        OR
            (--ss spvs
                        e.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR = 'sale'
                    AND
                        e.device_platform != 'native app' --explicitly remove native app (as app offer pages appear like web SPVs)
                --TODO need to adjust this for when TB product goes live on native app, we will need to count the wrapped mweb
                --product
                    AND e.is_server_side_event = TRUE)
    )
  --check se.com
  AND u.url_hostname = 'www.secretescapes.com'

GROUP BY 1
ORDER BY abs(diff) DESC;

SELECT COUNT(DISTINCT se_user_id)                            AS users,
       sum(CASE WHEN ss_spvs > 0 AND cs_spvs = 0 THEN 1 END) AS users_blocking_cs
FROM (
         SELECT se_user_id,
                --REGEXP_REPLACE(REGEXP_REPLACE(page_url, '%2F', '%252F'), '%3D', '%253D') as page_url,
--        date_trunc(DAY, event_tstamp)                          AS day,
                SUM(CASE WHEN is_server_side_event = TRUE THEN 1 ELSE 0 END)  AS ss_spvs,
                SUM(CASE WHEN is_server_side_event = FALSE THEN 1 ELSE 0 END) AS cs_spvs,
                ss_spvs - cs_spvs                                             AS diff


         FROM hygiene_vault_mvp.snowplow.event_stream e
                  INNER JOIN data_vault_mvp.single_customer_view_stg.module_url_hostname u ON e.page_url = u.url
         WHERE e.event_name = 'page_view'
           AND e.se_sale_id IS NOT NULL
           AND e.event_tstamp >= '2020-02-28 00:00:00'
           AND e.is_robot_spider_event = FALSE

           AND (
                     (--cs spvs
                             e.page_urlpath LIKE '%/sale'
                             OR
                             e.page_urlpath LIKE '%/sale-%' --eg. /sale-ihp, /sale-hotel for connected sales
                         -- need to adjust for new definitions of spv e.g. travel bird booking flow
                         )
                     AND
                     e.device_platform != 'native app' --explicitly remove native app (as app offer pages appear like web SPVs)
                     AND e.is_server_side_event = FALSE -- exclude non validated ss events

                 OR
                     (--ss spvs
                                 e.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR = 'sale'
                             AND
                                 e.device_platform != 'native app' --explicitly remove native app (as app offer pages appear like web SPVs)
                         --TODO need to adjust this for when TB product goes live on native app, we will need to count the wrapped mweb
                         --product
                             AND e.is_server_side_event = TRUE)
             )
           --check se.com
--            AND u.url_hostname = 'www.secretescapes.com'
         GROUP BY 1
         ORDER BY abs(diff)
             DESC
     ); -- 574245 users, 18506 blocking cs = 3% for 'www.secretescapes.com'

SELECT se_user_id,
       REGEXP_REPLACE(REGEXP_REPLACE(page_url, '%2F', '%252F'), '%3D', '%253D') AS page_url,
       date_trunc(DAY, event_tstamp)                                            AS day,
       SUM(CASE WHEN is_server_side_event = TRUE THEN 1 END)                    AS ss_spvs,
       SUM(CASE WHEN is_server_side_event = FALSE THEN 1 END)                   AS cs_spvs

FROM hygiene_vault_mvp.snowplow.event_stream e
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_url_hostname u ON e.page_url = u.url
WHERE e.event_name = 'page_view'
  AND e.se_sale_id IS NOT NULL
  AND e.event_tstamp >= '2020-02-28 00:00:00'
  AND e.is_robot_spider_event = FALSE

  AND (
            (--cs spvs
                    e.page_urlpath LIKE '%/sale'
                    OR
                    e.page_urlpath LIKE '%/sale-%' --eg. /sale-ihp, /sale-hotel for connected sales
                -- need to adjust for new definitions of spv e.g. travel bird booking flow
                )
            AND
            e.device_platform != 'native app' --explicitly remove native app (as app offer pages appear like web SPVs)
            AND e.is_server_side_event = FALSE -- exclude non validated ss events

        OR
            (--ss spvs
                        e.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR = 'sale'
                    AND
                        e.device_platform != 'native app' --explicitly remove native app (as app offer pages appear like web SPVs)
                --TODO need to adjust this for when TB product goes live on native app, we will need to count the wrapped mweb
                --product
                    AND e.is_server_side_event = TRUE)
    )
  --check se.com
  AND u.url_hostname = 'www.secretescapes.com'
  AND se_user_id IN (
    '28371496'
    )
GROUP BY 1, 2, 3
ORDER BY 1, 2;
