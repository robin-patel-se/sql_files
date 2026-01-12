SELECT *
FROM se.data.scv_touched_transactions stt
         INNER JOIN se.data.fact_complete_booking fcb ON stt.booking_id = fcb.booking_id
WHERE stt.booking_id IN (
                         'A1936762',
                         'A1943510',
                         'A1948983',
                         'A1949953',
                         'A1950238',
                         'A1955322',
                         'A1955895',
                         'A1956169',
                         'A1957620',
                         'A1959043',
                         'A1961457',
                         'A1962589',
                         'A1964304',
                         'A1964514',
                         'A1966541',
                         'A1967718',
                         'A1968373',
                         'A1969222',
                         'A1969579',
                         'A1969841',
                         'A1970593',
                         'A1971045',
                         'A1971514',
                         'A1971513',
                         'A1971692',
                         'A1971722',
                         'A1972422',
                         'A1973692',
                         'A1974696',
                         'A1974874',
                         'A1974871',
                         'A1975101',
                         'A1975646',
                         'A1977282',
                         'A1977306',
                         'A3074605',
                         'A3074713',
                         'A1977784',
                         'A3075320',
                         'A3075772',
                         'A3076061',
                         'A3076183',
                         'A1956762',
                         'A1957510',
                         'A1957558',
                         'A1957582',
                         'A1957790',
                         'A1960960',
                         'A1966362',
                         'A1966290',
                         'A1968984',
                         'A1969870',
                         'A1970669',
                         'A1972856'
    );

USE WAREHOUSE pipe_xlarge;

SELECT *
FROM se.data.scv_touched_transactions stt
         INNER JOIN se.data.fact_complete_booking fcb ON stt.booking_id = fcb.booking_id
WHERE stt.booking_id = 'A3076183';

SELECT *
FROM se.data_pii.scv_touch_basic_attributes stba
         INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
WHERE stba.attributed_user_id = '38312374'


SELECT *
FROM se.data.scv_touch_attribution sta
         INNER JOIN se.data.scv_touch_marketing_channel stmc
                    ON sta.attributed_touch_id = stmc.touch_id AND sta.attribution_model = 'last paid'
WHERE sta.touch_id = '371d3dc0c8410b3bf15e2b6e7a5e08cdb9a948c920b2a88c44e546f3b37ad6d5';



SELECT stmc.touch_mkt_channel,
       DATE_TRUNC(MONTH, stba.touch_start_tstamp) AS month,
       COUNT(*)
FROM se.data.scv_touch_marketing_channel stmc
         INNER JOIN se.data.scv_touch_basic_attributes stba ON stmc.touch_id = stba.touch_id
WHERE stmc.touch_landing_page LIKE '%criteo%'
GROUP BY 1, 2;


--A1977306 transaction id shows as email and is in criteo's list
USE WAREHOUSE pipe_xlarge;

SELECT stt.touch_id,
       stt.event_tstamp,
       stt.booking_id,
       stba.attributed_user_id,
       stmc.touch_mkt_channel AS last_click_channel
FROM se.data.scv_touched_transactions stt
         INNER JOIN se.data.scv_touch_marketing_channel stmc ON stt.touch_id = stmc.touch_id
         INNER JOIN se.data_pii.scv_touch_basic_attributes stba ON stt.touch_id = stba.touch_id
WHERE stt.booking_id = 'A1977306';


SELECT *
FROM se.data_pii.scv_touch_basic_attributes stba
WHERE stba.attributed_user_id = '29373383'
--392 sessions in total, booking took place on the 22 of august, 30 day attribution window


SELECT stba.touch_id,
       stba.attributed_user_id,
       stba.touch_experience,
       stba.touch_start_tstamp,
       stba.touch_has_booking,
       stba.touch_landing_page,
       stmc.touch_mkt_channel AS last_click_channel,
       s.touch_mkt_channel    AS last_paid_channel,
       s2.touch_mkt_channel AS last_non_direct_channel
FROM se.data_pii.scv_touch_basic_attributes stba
         INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
         INNER JOIN se.data.scv_touch_attribution sta ON stba.touch_id = sta.touch_id AND sta.attribution_model = 'last paid'
         INNER JOIN se.data.scv_touch_marketing_channel s ON sta.attributed_touch_id = s.touch_id
         INNER JOIN se.data.scv_touch_attribution sta2 ON stba.touch_id = sta2.touch_id AND sta2.attribution_model = 'last non direct'
         INNER JOIN se.data.scv_touch_marketing_channel s2 ON sta2.attributed_touch_id = s2.touch_id
WHERE stba.attributed_user_id = '29373383'
  AND stba.touch_start_tstamp >= DATEADD(DAY, -30, '2020-08-22')
  AND stba.touch_start_tstamp <= '2020-08-23';



SELECT *
FROM se.data.scv_touch_attribution sta
WHERE sta.attribution_model = 'last paid'
  AND sta.touch_id = '0ba33b1a9ef3b99d3306e3fc8d89b7803bdd08f6c0762abe5e3b8f65fea06416';


SELECT *
FROM se.data.scv_touch_attribution sta
WHERE sta.attribution_model = 'last non direct'
  AND sta.touch_id = '0ba33b1a9ef3b99d3306e3fc8d89b7803bdd08f6c0762abe5e3b8f65fea06416';


SELECT COUNT(*)
FROM (
         SELECT touch_id, COUNT(*)
         FROM se.data.scv_touch_attribution sta
         WHERE sta.attribution_model = 'last paid'
         GROUP BY 1
         HAVING COUNT(*) > 1
     );

SELECT *
FROM se.data.scv_touch_basic_attributes stba
WHERE stba.touch_id = 'a34b679b88b6803dad35181b53521e74a5e3f2630425d0d066b7183e4a7e315c';

self_describing_task --include 'dv/dwh/events/06_touch_attribution/01_module_touch_attribution.py'  --method 'run' --start '2021-02-09 00:00:00' --end '2021-02-09 00:00:00'


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.single_customer_view_stg.module_touch_attribution_20210210 CLONE data_vault_mvp.single_customer_view_stg.module_touch_attribution;

DELETE FROM data_vault_mvp.single_customer_view_stg.module_touch_attribution mta WHERE mta.attribution_model = 'last paid';

MERGE INTO data_vault_mvp.single_customer_view_stg.module_touch_attribution AS target
    USING (
             WITH all_touches_from_users AS (
                 --create a proxy touch id and touch tstamp and nullify it if the touch is mkt channel is not paid
                 SELECT c.touch_id,
                        b.touch_start_tstamp,
                        c.touch_mkt_channel,
                        c.attributed_user_id,
                        CASE
                            --don't nullify if first touch
                            WHEN LAG(c.touch_mkt_channel)
                                     OVER (PARTITION BY c.attributed_user_id ORDER BY b.touch_start_tstamp) IS NULL
                                THEN c.touch_id
                            --nullify if is a not a paid channel
                            WHEN c.touch_mkt_channel NOT IN (
                                                             'PPC - Brand',
                                                             'PPC - Non Brand CPA',
                                                             'PPC - Non Brand CPL',
                                                             'PPC - Undefined',
                                                             'Display CPA',
                                                             'Display CPL',
                                                             'Paid Social CPA',
                                                             'Paid Social CPL')
                                THEN NULL
                            ELSE c.touch_id
                            END AS nullify_touch_id,
                        CASE
                            --don't nullify if first touch
                            WHEN LAG(c.touch_mkt_channel)
                                     OVER (PARTITION BY c.attributed_user_id ORDER BY b.touch_start_tstamp) IS NULL
                                THEN b.touch_start_tstamp
                            --nullify if is a not a paid channel
                            WHEN c.touch_mkt_channel NOT IN
                                 ('PPC - Brand', 'PPC - Non Brand CPA', 'PPC - Non Brand CPL', 'Display', 'Paid Social')
                                THEN NULL
                            ELSE b.touch_start_tstamp
                            END AS nullify_touch_start_tstamp
                 FROM data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel c
                          INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes b
                                     ON c.touch_id = b.touch_id
                      -- get all touches from users who have had a new touch
             ),
             last_value AS (
                 --use proxy touch id and touch tstamp to back fill nulls
                 SELECT touch_id,
                        touch_start_tstamp,
                        touch_mkt_channel,
                        attributed_user_id,
                        LAST_VALUE(nullify_touch_id) IGNORE NULLS OVER
                            (PARTITION BY attributed_user_id ORDER BY touch_start_tstamp
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS persisted_touch_id,
                        LAST_VALUE(nullify_touch_start_tstamp) IGNORE NULLS OVER
                            (PARTITION BY attributed_user_id ORDER BY touch_start_tstamp
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS persisted_touch_start_tstamp
                 FROM all_touches_from_users
             )
             --check that the back fills don't persist longer than 30 days
        SELECT touch_id,
               --        touch_start_tstamp,
               --        touch_mkt_channel,
               --        attributed_user_id,
               --        persisted_touch_id,
               --        persisted_touch_start_tstamp,
               CASE
                   WHEN touch_id != persisted_touch_id AND
                       -- if a different paid touch id exists AND its within 30 days then use it
                        DATEDIFF(DAY, persisted_touch_start_tstamp, touch_start_tstamp) <= 30
                       THEN persisted_touch_id
                   ELSE touch_id END AS attributed_touch_id,
               'last paid'           AS attribution_model,
               1                     AS attributed_weight
        FROM last_value
    ) AS batch ON target.touch_id = batch.touch_id
        AND target.attributed_touch_id = batch.attributed_touch_id
        AND target.attribution_model = batch.attribution_model
    WHEN NOT MATCHED
        THEN INSERT (
                     schedule_tstamp,
                     run_tstamp,
                     operation_id,
                     created_at,
                     updated_at,
                     touch_id,
                     attributed_touch_id,
                     attribution_model,
                     attributed_weight
        )
        VALUES ('2021-02-08 03:00:00',
                '2021-02-10 10:32:33',
                'refactor_lookback_window_repopulate',
                CURRENT_TIMESTAMP()::TIMESTAMP,
                CURRENT_TIMESTAMP()::TIMESTAMP,
                batch.touch_id,
                batch.attributed_touch_id,
                batch.attribution_model,
                batch.attributed_weight)
    WHEN MATCHED THEN UPDATE SET
        target.schedule_tstamp = '2021-02-08 03:00:00',
        target.run_tstamp = '2021-02-10 10:32:33',
        target.operation_id =
                'refactor_lookback_window_repopulate',
        target.updated_at = CURRENT_TIMESTAMP()::TIMESTAMP,

        target.touch_id = batch.touch_id,
        target.attributed_touch_id = batch.attributed_touch_id,
        target.attribution_model = batch.attribution_model,
        target.attributed_weight = batch.attributed_weight;