USE WAREHOUSE pipe_xlarge;
--Total run time: 5 min 47s on a LARGE warehouse

CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.send_logs_source AS (
    SELECT es.send_log__job_id,
           es.send_log__date,
           es.send_log__section,
           es.send_log__position_in_section,
           es.user_id,
           es.deal_id                                                 AS item_id,
           DATEDIFF('day', CURRENT_DATE, es.send_log__date::date) - 1 AS days_ago,
           es.territory_id,
           es.error_log__error_1, -- sale_id is not valid or cannot be found
           es.error_log__error_2, -- sale card build process error
           es.error_log__error_3, -- user is ineligible to receive marketing communication
           es.error_log__error_4, -- user is in a journey and suppressed
           es.error_log__error_5, -- user does not have current selections
           COALESCE(es.error_log__error_1, -- sale_id is not valid or cannot be found
                    es.error_log__error_2, -- sale card build process error
                    es.error_log__error_3, -- user is ineligible to receive marketing communication
                    es.error_log__error_4, -- user is in a journey and suppressed
           --es.ERROR_LOG__ERROR_5,  -- user does not have current selections
                    0)                                                AS errors
    FROM data_vault_mvp.athena.email_sends es
    WHERE es.send_log__date::date >= CURRENT_DATE - 7
      AND es.send_log__date::date <= CURRENT_DATE
);

CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.send_logs_qualify AS (
    SELECT sl.user_id,
           sl.days_ago,
           sl.item_id,
           sl.territory_id,
           sl.errors
    FROM scratch.robinpatel.send_logs_source sl
        QUALIFY ROW_NUMBER() OVER (PARTITION BY sl.send_log__job_id, sl.user_id
            ORDER BY sl.send_log__section, sl.send_log__position_in_section) = 1
);

CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.daily_deals_selections_source AS
    (
        SELECT ds.user_id,
               DATEDIFF('day', CURRENT_DATE, ds.planning_date) - 1 AS days_ago,
               ds.planning_position                                AS planning_position,
               ds.deal_id                                          AS item_id,
               ds.territory_id,
               ds.planning_date,
               ds.error_code
        FROM data_science.operational_output.daily_deals_selections ds
        WHERE ds.planning_date >= CURRENT_DATE - 8
    );

--Exclude first deal in sent e-mails (possibly a manual inclusion, e-mail possibly not opened) -- determines subject line of the email -- therefore repeated -- 7 day rule -- assume always been differ
--because this deal determines the subject line which should never repeat
CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.email_exclusions AS
    (
        WITH daily_deals_selection AS
                 (
                     SELECT DISTINCT
                            dds.user_id,
                            dds.territory_id
                     FROM scratch.robinpatel.daily_deals_selections_source dds
                 )
        SELECT DISTINCT
               dds.user_id,
               sl.days_ago,
               1 AS planning_position, --This is protected by the qualify above
               sl.item_id,
               dds.territory_id
        FROM scratch.robinpatel.send_logs_qualify sl
                 INNER JOIN daily_deals_selection dds ON dds.user_id = sl.user_id
            AND dds.territory_id = sl.territory_id
        WHERE sl.errors = 0
    );

CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.email_open_source AS (
    SELECT asl.subscriber_key::VARCHAR AS user_id,
           eo.event_date,
           asl.job_id,
           eo.event_hash,
           asl.section,
           asl.position_in_section,
           asl.deal_id                 AS item_id,
           asl.log_date
    FROM hygiene_snapshot_vault_mvp.sfmc.events_opens_plus_inferred eo
             INNER JOIN hygiene_snapshot_vault_mvp.sfmc.athena_send_log asl
                        ON eo.subscriber_key::varchar = asl.subscriber_key::varchar
                            AND eo.send_id = asl.job_id
    WHERE --eo.event_date >= '2020-10-06' --hard date when athena went live (not needed as we are filter below?)
        eo.event_date >= CURRENT_DATE - 7
      AND eo.event_date <= CURRENT_DATE
);

CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.email_open_top_10_exclusions AS (
--Exclude first ten deals in opened e-mails (possibly including manual inclusions)
    WITH athena_sends_agg AS (
        SELECT DISTINCT
               es.send_log__job_id,
               es.user_id,
               es.territory_id
--                ROW_NUMBER() OVER (PARTITION BY es.send_log__job_id, es.user_id ORDER BY es.send_log__section, es.send_log__position_in_section) AS planning_position
        FROM data_vault_mvp.athena.email_sends es
    )
    SELECT eo.user_id,
           DATEDIFF('day', CURRENT_DATE, eo.event_date) - 1 AS days_ago,
           ROW_NUMBER() OVER (PARTITION BY eo.job_id, eo.user_id, eo.event_hash
               ORDER BY eo.section, eo.position_in_section) AS planning_position,
--            asa.planning_position,
           eo.item_id,
           asa.territory_id
    FROM scratch.robinpatel.email_open_source eo
             INNER JOIN athena_sends_agg asa ON eo.user_id = asa.user_id AND eo.job_id = asa.send_log__job_id
--     WHERE asa.planning_position <= 10
        QUALIFY planning_position <= 10
);

CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.event_stream_source AS (
    SELECT es.event_tstamp::date                                               AS event_date,
           TRY_TO_NUMBER(es.user_id)                                           AS user_id,
           es.page_urlpath,
           es.event_name,
           es.contexts_com_secretescapes_screen_context_1[0]['screen_name']    AS screen_name,
           es.contexts_com_secretescapes_screen_context_1[0]['screen_id']      AS screen_id,
           CASE
               WHEN RLIKE(es.page_urlpath, '.*/current-sales.?', 'i') THEN 'UK'
               WHEN RLIKE(es.page_urlpath, '.*/aktuelle-angebote.?', 'i') THEN 'DE'
               WHEN RLIKE(es.page_urlpath, '.*/offerte-in-corso.?', 'i') THEN 'IT'
               WHEN RLIKE(es.page_urlpath, '.*/aktuella-kampanjer.?', 'i') THEN 'SWEDEN'
               WHEN RLIKE(es.page_urlpath, '.*/aanbiedingen.?', 'i') THEN 'NL_BE'
               WHEN RLIKE(es.page_urlpath, '.*/nuvaerende-salg.?', 'i') THEN 'DK_CS' -- DK (current sales)
               WHEN RLIKE(es.page_urlpath, '.*/aktuelle-tilbud.?', 'i') THEN 'DK_CO' -- DK (current offers)
               WHEN RLIKE(es.page_urlpath, '.*/ventas-actuales.?', 'i') THEN 'ES_CS' -- ES (current sales)
               END                                                             AS homepage,
           IFF(screen_id IN
               ('Hand-picked for you', 'Hand-picked for you (20)',
                'Für Sie ausgewählt', 'Für Sie ausgewählt (20)',
                'Selezionate per te', 'Selezionate per te (20)'), TRUE, FALSE) AS handpicked
    FROM hygiene_vault_mvp.snowplow.event_stream es
    WHERE event_date >= CURRENT_DATE - 7
      AND event_date <= CURRENT_DATE
);
--Exclude previous selections used on homepage ("recommended for you" section)
CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.homepage_visits_exclusions AS (
    WITH distinct_event_stream AS
             (
                 SELECT DISTINCT
                        user_id,
                        event_date
                 FROM scratch.robinpatel.event_stream_source
                 WHERE homepage IS NOT NULL
             )
    SELECT ds.user_id,
           ds.days_ago,
           ds.planning_position,
           ds.item_id,
           ds.territory_id
    FROM scratch.robinpatel.daily_deals_selections_source ds
             INNER JOIN distinct_event_stream es
                        ON ds.planning_date = es.event_date
                            AND ds.user_id = es.user_id
    WHERE ds.planning_position <= 8 -- first 9 recommendations
      AND ds.error_code = 0
);

CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.handpicked_app_exclusions AS (
    WITH distinct_event_stream AS
             (
                 SELECT DISTINCT
                        user_id,
                        event_date
                 FROM scratch.robinpatel.event_stream_source
                 WHERE screen_name = 'homepage'
                   AND event_name = 'screen_view'
             )
    SELECT ds.user_id,
           ds.days_ago,
           ds.planning_position,
           ds.item_id,
           ds.territory_id
    FROM scratch.robinpatel.daily_deals_selections_source ds
             INNER JOIN distinct_event_stream es
                        ON ds.planning_date = es.event_date
                            AND ds.user_id = es.user_id
    WHERE ds.planning_position <= 4 -- first 5 recommendations
      AND ds.error_code = 0
);
-- exclude full top 10 selections when user clicked "view all deals" in section "Handpicked for you"
CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.top_10_app_exclusions AS (
    WITH distinct_event_stream AS
             (
                 SELECT DISTINCT
                        user_id,
                        event_date
                 FROM scratch.robinpatel.event_stream_source
                 WHERE screen_name = 'homepage collection'
                   AND handpicked
             )
    SELECT ds.user_id,
           ds.days_ago,
           ds.planning_position,
           ds.item_id,
           ds.territory_id
    FROM scratch.robinpatel.daily_deals_selections_source ds
             INNER JOIN distinct_event_stream es
                        ON ds.planning_date = es.event_date
                            AND ds.user_id = es.user_id
    WHERE -- all 10 recommendations
          ds.error_code = 0
);

-- TODO: exclude 1 top selection appearing in push notification (Sundays) (currently only about 10-15% of users use apps)
-- TODO: of whom only 20-25% have push notifications enabled. Revisit this, when this problem affects more users.
--Exclude deals ordered in the last 7 days:
CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.deals_booked_exclusions AS (
    SELECT DISTINCT
           b.shiro_user_id::VARCHAR                                             AS user_id,
           DATEDIFF('day', CURRENT_DATE, TO_DATE(b.booking_completed_date)) - 1 AS days_ago,
           1                                                                    AS planning_position,
           b.sale_id                                                            AS item_id,
           t.id                                                                 AS territory_id
    FROM se.data.fact_complete_booking b
             LEFT JOIN se.data.dim_sale ds
                       ON b.sale_id = ds.se_sale_id
             LEFT JOIN data_vault_mvp.cms_mysql_snapshots.territory_snapshot t
                       ON ds.posa_territory = t.name
    WHERE b.booking_status = 'COMPLETE' --This will exclude any travelbird bookings, do you want to remove this?
      AND b.booking_completed_date >= CURRENT_DATE - 7
);

CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.full_list_exclusions AS (
    WITH union_all_exclusion AS (
        SELECT user_id,
               days_ago,
               planning_position,
               item_id,
               territory_id
        FROM scratch.robinpatel.email_exclusions
        UNION ALL
        SELECT user_id,
               days_ago,
               planning_position,
               item_id,
               territory_id
        FROM scratch.robinpatel.email_open_top_10_exclusions
        UNION ALL
        SELECT user_id,
               days_ago,
               planning_position,
               item_id,
               territory_id
        FROM scratch.robinpatel.homepage_visits_exclusions
        UNION ALL
        SELECT user_id,
               days_ago,
               planning_position,
               item_id,
               territory_id
        FROM scratch.robinpatel.handpicked_app_exclusions
        UNION ALL
        SELECT user_id,
               days_ago,
               planning_position,
               item_id,
               territory_id
        FROM scratch.robinpatel.top_10_app_exclusions
        UNION ALL
        SELECT user_id,
               days_ago,
               planning_position,
               item_id,
               territory_id
        FROM scratch.robinpatel.deals_booked_exclusions
    )
-- deduplicate entries
    SELECT ue.user_id,
           ue.days_ago,
           MIN(ue.planning_position) AS planning_position,
           ue.item_id,
           ue.territory_id
    FROM union_all_exclusion ue
    GROUP BY 1, 2, 4, 5
);

--row count 103,502,347
SELECT COUNT(*)
FROM scratch.robinpatel.full_list_exclusions;

--row count 103,502,347
SELECT COUNT(*)
FROM scratch.gianniraftis.ds_exclusion_output_comparison;

-- row count 103,503,950
SELECT a.*, b.user_id
FROM scratch.robinpatel.full_list_exclusions a
         LEFT JOIN scratch.robinpatel.ds_exclusion_output_comparison b ON a.user_id = b.user_id
    AND a.territory_id = b.territory_id
    AND a.days_ago = b.days_ago
    AND a.item_id = b.item_id
    AND a.planning_position = b.planning_position
WHERE b.user_id IS NULL
  AND a.user_id = 75182144

SELECT *
FROM scratch.robinpatel.full_list_exclusions
WHERE user_id = 75182144
  AND item_id = 'A27972'
  AND days_ago = -4

SELECT *
FROM scratch.gianniraftis.ds_exclusion_output_comparison
WHERE user_id = 17345224
  AND item_id = 'A32975'
  AND days_ago = -7;


SELECT *
FROM scratch.robinpatel.email_exclusions
WHERE user_id = 17345224
  AND item_id = 'A32975'
  AND days_ago = -7
;

SELECT *
FROM scratch.robinpatel.email_open_top_10_exclusions
WHERE user_id = 17345224
  AND item_id = 'A32975'
  AND days_ago = -7;

SELECT *
FROM scratch.robinpatel.homepage_visits_exclusions
WHERE user_id = 17345224
  AND item_id = 'A32975'
  AND days_ago = -7;

SELECT *
FROM scratch.robinpatel.handpicked_app_exclusions
WHERE user_id = 17345224
  AND item_id = 'A32975'
  AND days_ago = -7;

SELECT *
FROM scratch.robinpatel.top_10_app_exclusions
WHERE user_id = 17345224
  AND item_id = 'A32975'
  AND days_ago = -7;

SELECT *
FROM scratch.robinpatel.deals_booked_exclusions
WHERE user_id = 17345224
  AND item_id = 'A32975'
  AND days_ago = -7;

SELECT eo.user_id,
       DATEDIFF('day', CURRENT_DATE, eo.event_date) - 1 AS days_ago,
       ROW_NUMBER() OVER (PARTITION BY eo.job_id, eo.user_id, eo.event_hash
           ORDER BY eo.section, eo.position_in_section) AS planning_position,
       eo.item_id
FROM scratch.robinpatel.email_open_source eo;


SELECT *
FROM scratch.gianniraftis.ds_exclusion_output_comparison
MINUS
SELECT *
FROM scratch.robinpatel.full_list_exclusions

------------------------------------------------------------------------------------------------------------------------

WITH athena_sends_agg AS (
    SELECT es.send_log__job_id,
           es.user_id,
           es.territory_id,
           es.deal_id                                                                                                                       AS sale_id,
           ROW_NUMBER() OVER (PARTITION BY es.send_log__job_id, es.user_id ORDER BY es.send_log__section, es.send_log__position_in_section) AS planning_position
    FROM data_vault_mvp.athena.email_sends es
    WHERE es.user_id = 17345224
),
     next_q AS (
         SELECT eo.user_id,
                asa.send_log__job_id,
                eo.event_hash,
                DATEDIFF('day', CURRENT_DATE, eo.event_date) - 1 AS days_ago,
                asa.planning_position,
--            asa.planning_position,
                eo.item_id,
                asa.territory_id
         FROM scratch.robinpatel.email_open_source eo
                  INNER JOIN athena_sends_agg asa ON eo.user_id = asa.user_id AND eo.job_id = asa.send_log__job_id AND asa.sale_id = eo.item_id
         WHERE asa.planning_position <= 10
--              QUALIFY planning_position <= 10
     )
SELECT *
FROM next_q
WHERE next_q.item_id = 'A32975'



WITH athena_sends_agg AS (
    SELECT DISTINCT
           es.send_log__job_id,
           es.user_id,
           es.territory_id
--                ROW_NUMBER() OVER (PARTITION BY es.send_log__job_id, es.user_id ORDER BY es.send_log__section, es.send_log__position_in_section) AS planning_position
    FROM data_vault_mvp.athena.email_sends es
),
     next_q AS (
         SELECT eo.*,
--                 eo.user_id,
                DATEDIFF('day', CURRENT_DATE, eo.event_date) - 1 AS days_ago,
                ROW_NUMBER() OVER (PARTITION BY eo.job_id, eo.user_id, eo.event_hash
                    ORDER BY eo.section, eo.position_in_section, eo.log_date) AS planning_position,
--            asa.planning_position,
--                 eo.item_id,
                asa.territory_id
         FROM scratch.robinpatel.email_open_source eo
                  INNER JOIN athena_sends_agg asa ON eo.user_id = asa.user_id AND eo.job_id = asa.send_log__job_id
--     WHERE asa.planning_position <= 10
             QUALIFY planning_position <= 10
     )
SELECT *
FROM next_q
WHERE next_q.user_id = 17345224
  AND next_q.item_id = 'A32975';

SELECT *
FROM scratch.robinpatel.email_open_source eo
WHERE user_id = 17345224
  AND item_id = 'A32975';


SELECT * FROM raw_vault_mvp.broadway_travel.wrd_booking;