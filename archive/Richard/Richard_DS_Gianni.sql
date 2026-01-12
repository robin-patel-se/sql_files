CREATE OR REPLACE TRANSIENT TABLE
    scratch.gianniraftis.ds_exclusion_output_comparison
AS
    (
        WITH t AS (
            (
                --    Exclude first deal in sent e-mails (possibly a manual inclusion, e-mail possibly not opened) -- determines subject line of the email -- therefore repeated -- 7 day rule -- assume always been differ
                -- because this deal determines the subject line which should never repeat
                WITH send_logs AS (
                    SELECT ROW_NUMBER() OVER (PARTITION BY es.send_log__job_id, es.user_id
                        ORDER BY es.send_log__section, es.send_log__position_in_section) AS email_position
                         , es.*
                    FROM data_vault_mvp.athena.email_sends es
                )
                SELECT sl.user_id
                     , DATEDIFF('day', CURRENT_DATE, TO_DATE(sl.send_log__date)) - 1 AS days_ago
                     , sl.email_position                                             AS planning_position
                     , sl.deal_id                                                    AS item_id
                     , sl.territory_id
                FROM send_logs sl
                         -- limit to users for whom we generate predictions
                         JOIN (
                                  SELECT dds.user_id, dds.territory_id
                                  FROM data_science.operational_output.daily_deals_selections dds
                                  WHERE dds.planning_date >= CURRENT_DATE - 8
                                  GROUP BY 1, 2
                              ) ds
                              ON sl.user_id::VARCHAR = ds.user_id::VARCHAR
                                  AND sl.territory_id = ds.territory_id
                WHERE (TO_DATE(sl.send_log__date) BETWEEN CURRENT_DATE - 7 AND CURRENT_DATE)
                  AND sl.email_position = 1 -- only first deal which determines subject line
                  AND COALESCE(sl.error_log__error_1, -- sale_id is not valid or cannot be found
                               sl.error_log__error_2, -- sale card build process error
                               sl.error_log__error_3, -- user is ineligible to receive marketing communication
                               sl.error_log__error_4, -- user is in a journey and suppressed
                    --sl.ERROR_LOG__ERROR_5,  -- user does not have current selections
                               0) = 0
            )
            UNION ALL
            (
                --    Exclude first ten deals in opened e-mails (possibly including manual inclusions)
                WITH email_open_log AS (
                    SELECT asl.subscriber_key                          AS user_id
                         , TO_DATE(eo.event_tstamp)                    AS event_date
                         , ROW_NUMBER() OVER (PARTITION BY asl.job_id, asl.subscriber_key, eo.event_hash
                        ORDER BY asl.section, asl.position_in_section) AS email_position
                         , asl.deal_id                                 AS item_id
                         , es.territory_id
                    FROM hygiene_snapshot_vault_mvp.sfmc.events_opens_plus_inferred eo
                             INNER JOIN hygiene_snapshot_vault_mvp.sfmc.athena_send_log asl
                                        ON eo.subscriber_key::VARCHAR = asl.subscriber_key::VARCHAR
                                            AND eo.send_id = asl.job_id
                             INNER JOIN (
                                            SELECT es.send_log__job_id, es.user_id, es.territory_id
                                            FROM data_vault_mvp.athena.email_sends es
                                            GROUP BY 1, 2, 3
                                        ) es
                                        ON asl.subscriber_key::VARCHAR = es.user_id::VARCHAR
                                            AND asl.job_id = es.send_log__job_id
                    WHERE eo.event_date >= '2020-10-06' --hard date when athena went live
                      AND eo.event_tstamp >= CURRENT_DATE - 8
                )
                SELECT eol.user_id
                     , DATEDIFF('day', CURRENT_DATE, eol.event_date) - 1 AS days_ago
                     , eol.email_position                                AS planning_position
                     , eol.item_id
                     , eol.territory_id
                FROM email_open_log eol
                WHERE (eol.event_date BETWEEN CURRENT_DATE - 7 AND CURRENT_DATE)
                  AND eol.email_position <= 10 -- count starts at 1, i.e. first ten deals
            )
            UNION ALL
            (
                --     Exclude previous selections used on homepage ("recommended for you" section)
                WITH homepage_visits AS (
                    SELECT TRY_TO_NUMBER(es.user_id) AS user_id
                         , TO_DATE(es.event_tstamp)  AS event_date
                    FROM hygiene_vault_mvp.snowplow.event_stream es
                    WHERE es.event_tstamp > CURRENT_DATE - 8
                      AND (RLIKE(es.page_urlpath, '.*/current-sales.?', 'i') -- UK
                        OR RLIKE(es.page_urlpath, '.*/aktuelle-angebote.?', 'i') -- DE
                        OR RLIKE(es.page_urlpath, '.*/offerte-in-corso.?', 'i') -- IT
                        OR RLIKE(es.page_urlpath, '.*/aktuella-kampanjer.?', 'i') -- Sweden
                        OR RLIKE(es.page_urlpath, '.*/aanbiedingen.?', 'i') -- NL, BE
                        OR RLIKE(es.page_urlpath, '.*/nuvaerende-salg.?', 'i') -- DK (current sales)
                        OR RLIKE(es.page_urlpath, '.*/aktuelle-tilbud.?', 'i') -- DK (current offers)
                        OR RLIKE(es.page_urlpath, '.*/ventas-actuales.?', 'i') -- ES (current sales)
                        )
                    GROUP BY 1, 2
                )
                SELECT ds.user_id
                     , DATEDIFF('day', CURRENT_DATE, ds.planning_date) - 1 AS days_ago
                     , ds.planning_position                                AS planning_position
                     , ds.deal_id                                          AS item_id
                     , ds.territory_id
                FROM data_science.operational_output.daily_deals_selections ds
                         JOIN homepage_visits hv
                              ON ds.planning_date = hv.event_date
                                  AND ds.user_id::VARCHAR = hv.user_id::VARCHAR
                WHERE (ds.planning_date BETWEEN CURRENT_DATE - 7 AND CURRENT_DATE)
                  AND ds.planning_position <= 8 -- first 9 recommendations
                  AND ds.error_code = 0
            )
            UNION ALL
            (
                -- exclude previous selections appearing in "Handpicked for you" section of apps
                WITH app_homepage_visits AS (
                    SELECT es.user_id
                         , TO_DATE(es.event_tstamp) AS event_date
                    FROM hygiene_vault_mvp.snowplow.event_stream es
                    WHERE es.event_tstamp > CURRENT_DATE - 8
                      AND es.event_name = 'screen_view'
                      AND es.contexts_com_secretescapes_screen_context_1[0]['screen_name'] = 'homepage'
                    GROUP BY 1, 2
                )
                SELECT ds.user_id
                     , DATEDIFF('day', CURRENT_DATE, ds.planning_date) - 1 AS days_ago
                     , ds.planning_position                                AS planning_position
                     , ds.deal_id                                          AS item_id
                     , ds.territory_id
                FROM data_science.operational_output.daily_deals_selections ds
                         JOIN app_homepage_visits ahv
                              ON ds.planning_date = ahv.event_date
                                  AND ds.user_id::VARCHAR = ahv.user_id::VARCHAR
                WHERE (ds.planning_date BETWEEN CURRENT_DATE - 7 AND CURRENT_DATE)
                  AND ds.planning_position <= 4 -- first 5 recommendations
                  AND ds.error_code = 0
            )
            UNION ALL
            (
                -- exclude full top 10 selections when user clicked "view all deals" in section "Handpicked for you"
                WITH app_handpicked_visits AS (
                    SELECT es.user_id
                         , TO_DATE(es.event_tstamp) AS event_date
                    FROM hygiene_vault_mvp.snowplow.event_stream es
                    WHERE es.event_tstamp > CURRENT_DATE - 8
                      AND es.event_name = 'screen_view'
                      AND es.contexts_com_secretescapes_screen_context_1[0]['screen_name'] = 'homepage collection'
                      AND es.contexts_com_secretescapes_screen_context_1[0]['screen_id'] IN
                          ('Hand-picked for you', 'Hand-picked for you (20)',
                           'Für Sie ausgewählt', 'Für Sie ausgewählt (20)',
                           'Selezionate per te', 'Selezionate per te (20)')
                    GROUP BY 1, 2
                )
                SELECT ds.user_id
                     , DATEDIFF('day', CURRENT_DATE, ds.planning_date) - 1 AS days_ago
                     , ds.planning_position                                AS planning_position
                     , ds.deal_id                                          AS item_id
                     , ds.territory_id
                FROM data_science.operational_output.daily_deals_selections ds
                         JOIN app_handpicked_visits ahav
                              ON ds.planning_date = ahav.event_date
                                  AND ds.user_id::VARCHAR = ahav.user_id::VARCHAR
                WHERE (ds.planning_date BETWEEN CURRENT_DATE - 7 AND CURRENT_DATE)
                  -- all 10 recommendations
                  AND ds.error_code = 0
            )
            -- TODO: exclude 1 top selection appearing in push notification (Sundays) (currently only about 10-15% of users use apps)
-- TODO: of whom only 20-25% have push notifications enabled. Revisit this, when this problem affects more users.
            UNION ALL
            (
                --     Exclude deals ordered in the last 7 days:
                SELECT DISTINCT
                       b.shiro_user_id::VARCHAR                                             AS user_id
                     , DATEDIFF('day', CURRENT_DATE, TO_DATE(b.booking_completed_date)) - 1 AS days_ago
                     , 1                                                                    AS planning_position
                     , b.sale_id                                                            AS item_id
                     , t.id                                                                 AS territory_id
                FROM se.data.fact_complete_booking b
                         LEFT JOIN se.data.dim_sale ds
                                   ON b.sale_id = ds.se_sale_id
                         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.territory_snapshot t
                                   ON ds.posa_territory = t.name
                WHERE booking_status = 'COMPLETE'
                  AND b.booking_completed_date >= CURRENT_DATE - 7
            )
        )
-- deduplicate entries
        SELECT t.user_id,
               t.days_ago,
               MIN(t.planning_position) AS planning_position,
               t.item_id,
               t.territory_id
        FROM t
        GROUP BY t.user_id, t.days_ago, t.item_id, t.territory_id
    );



WITH email_open_log AS (
    SELECT asl.subscriber_key                          AS user_id
         , TO_DATE(eo.event_tstamp)                    AS event_date
         , ROW_NUMBER() OVER (PARTITION BY asl.job_id, asl.subscriber_key, eo.event_hash
        ORDER BY asl.section, asl.position_in_section, asl.log_date) AS email_position
         , asl.deal_id                                 AS item_id
         , es.territory_id
    FROM hygiene_snapshot_vault_mvp.sfmc.events_opens_plus_inferred eo
             INNER JOIN hygiene_snapshot_vault_mvp.sfmc.athena_send_log asl
                        ON eo.subscriber_key::VARCHAR = asl.subscriber_key::VARCHAR
                            AND eo.send_id = asl.job_id
             INNER JOIN (
                            SELECT es.send_log__job_id, es.user_id, es.territory_id
                            FROM data_vault_mvp.athena.email_sends es
                            GROUP BY 1, 2, 3
                        ) es
                        ON asl.subscriber_key::VARCHAR = es.user_id::VARCHAR
                            AND asl.job_id = es.send_log__job_id
    WHERE eo.event_date >= '2020-10-06' --hard date when athena went live
      AND eo.event_tstamp >= CURRENT_DATE - 8
)
SELECT eol.user_id
     , DATEDIFF('day', CURRENT_DATE, eol.event_date) - 1 AS days_ago
     , eol.email_position                                AS planning_position
     , eol.item_id
     , eol.territory_id
FROM email_open_log eol
WHERE (eol.event_date BETWEEN CURRENT_DATE - 7 AND CURRENT_DATE)
  AND eol.email_position <= 10 -- count starts at 1, i.e. first ten deals
  AND user_id = 17345224
  AND item_id = 'A32975'
  AND days_ago = -7;