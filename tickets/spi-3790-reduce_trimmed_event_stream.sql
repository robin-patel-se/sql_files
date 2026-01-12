------------------------------------------------------------------------------------------------------------------------
-- further investigation 2023-04-26
-- check if we were to reduce the comparisons into separate queries would it improve run time
USE ROLE personal_role__robinpatel;
USE WAREHOUSE pipe_large; --same as the job
SELECT
    e.event_hash,
    e.is_robot_spider_event,
    e.is_internal_ip_address_event,
    e.is_server_side_event,

    e.event_tstamp,
    c.yesterday       AS event_tstamp_yesterday,
    c.today_last_week AS event_tstamp_today_last_week,
    c.today_ly        AS event_tstamp_today_ly,
    c.today_lly       AS event_tstamp_today_lly,
    c.today_2019      AS event_tstamp_today_2019,
    e.se_sale_id,

    e.app_id,
    e.collector_tstamp,
    e.page_url,
    e.page_urlpath,
    e.se_category,
    e.contexts_com_secretescapes_sale_page_context_1,
    e.contexts_com_secretescapes_screen_context_1,
    e.contexts_com_secretescapes_content_context_1,
    e.contexts_com_secretescapes_secret_escapes_sale_context_1,
    e.contexts_com_secretescapes_user_context_1,
    e.contexts_com_secretescapes_product_display_context_1,
    e.contexts_com_secretescapes_search_context_1

FROM hygiene_vault_mvp.snowplow.event_stream e
    INNER JOIN data_vault_mvp.dwh.se_calendar c ON e.event_tstamp::DATE = c.date_value
WHERE
  -- explicit list of events we care about
    (
                e.event_name IN ('page_view', 'screen_view')
            OR e.se_category = 'web redirect click' -- for web redirect spvs
            OR e.contexts_com_secretescapes_search_context_1 IS NOT NULL -- search events
        )
  -- remove robots
  AND e.is_robot_spider_event = FALSE
  -- remove se office ip addresses
  AND e.is_internal_ip_address_event = FALSE
  -- trim early events
  AND e.event_tstamp >= '2019-01-01'
  -- calendar dates we need to include for comparative numbers
  AND (
        c.today
        OR c.yesterday
        OR c.today_last_week
        OR c.today_ly
        OR c.today_lly
        OR c.today_2019
    )
;

-- baseline query (running as is) runs quite quickly
-- theory being that perhaps the loading into the table is taking the time
-- test moving step 1 into the insert step

DROP TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream;

CREATE OR REPLACE VIEW hygiene_vault_mvp_dev_robin.snowplow.event_stream AS
SELECT *
FROM hygiene_vault_mvp.snowplow.event_stream es;


self_describing_task --include 'biapp/task_catalogue/dv/dwh/events/trimmed_event_stream.py'  --method 'run' --start '2023-04-26 09:00:00' --end '2023-04-26 09:00:00'

------------------------------------------------------------------------------------------------------------------------
-- try hard coding the filter logic rather than joining to se_calendar
-- today_2019

SET date_value = '2023-04-26';

SELECT
        IFF(
                    YEAR(DATE_TRUNC(WEEK, DATEADD('day', -(YEAR(CURRENT_DATE) - 2019) * 365.25, CURRENT_DATE))) = 2019,
                    DATE_TRUNC('week', TO_DATE($date_value)) =
                    DATE_TRUNC(WEEK, DATEADD('day', -(YEAR(CURRENT_DATE) - 2019) * 365.25, CURRENT_DATE)), FALSE)
        AND DAYOFWEEKISO(TO_DATE($date_value)) = DAYOFWEEKISO(CURRENT_DATE);

SET date_value = '2021-04-01';

SELECT
    date_value
FROM se.data.se_calendar sc
WHERE sc.today_2019; --2019-04-24




SELECT
    TO_DATE($date_value)                                 AS input_date,
    DATE_TRUNC(WEEK, input_date)                         AS input_date_week,
    DAYOFWEEKISO(input_date)                             AS input_date_day_of_week,
    input_date - ((YEAR(input_date) - 2019) * 365.25)    AS input_date_in_2019,
    DAYOFWEEKISO(input_date_in_2019)                     AS input_date_in_2019_day_of_week,
    DATE_TRUNC(WEEK, input_date_in_2019)                 AS input_date_in_2019_week,
    input_date_in_2019_week + input_date_day_of_week - 1 AS test;


