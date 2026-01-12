--https://docs.google.com/spreadsheets/d/18Wmo15Zin35pYZ-kcr9BqZT0Ezkvj0psKLnkmqFPV3o/edit#gid=873892155

--create a task to persist aggregated numbers
CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.weighted_spvs_input AS (

    WITH spvs AS (
        SELECT sts.touch_id,
               COUNT(*) AS spvs
        FROM se.data.scv_touched_spvs sts
            INNER JOIN se.data.scv_touch_basic_attributes a ON sts.touch_id = a.touch_id
        WHERE sts.event_tstamp >= '2018-01-01'
          AND a.stitched_identity_type = 'se_user_id' -- member only spvs
        GROUP BY 1
    ),
         bookings AS (
             SELECT stt.touch_id,
                    COUNT(*) AS bookings
             FROM se.data.scv_touched_transactions stt
                 --only complete bookings
                 INNER JOIN se.data.fact_complete_booking fcb ON stt.booking_id = fcb.booking_id
             WHERE stt.event_tstamp >= '2018-01-01'
             GROUP BY 1
         )
    SELECT stba.touch_start_tstamp::DATE AS date,
           stmc.touch_mkt_channel,
           stba.touch_experience,
           stmc.touch_affiliate_territory,
           SUM(s.spvs)                   AS se_user_spvs,
           SUM(b.bookings)               AS bookings

    FROM se.data.scv_touch_basic_attributes stba
        INNER JOIN se.data.scv_touch_attribution sta
                   ON stba.touch_id = sta.touch_id AND sta.attribution_model = 'last non direct'
        INNER JOIN se.data.scv_touch_marketing_channel stmc ON sta.attributed_touch_id = stmc.touch_id
        LEFT JOIN  spvs s ON stba.touch_id = s.touch_id
        LEFT JOIN  bookings b ON stba.touch_id = b.touch_id
    WHERE stba.touch_start_tstamp >= '2018-01-01'
      AND stba.stitched_identity_type = 'se_user_id'
    GROUP BY 1, 2, 3
)
;


--create a task that works out rolling numbers
WITH daily_totals AS (
    SELECT w.date,
           SUM(w.se_user_spvs) AS total_spvs,
           SUM(w.bookings)     AS total_bookings
    FROM scratch.robinpatel.weighted_spvs_input w
    WHERE w.date >= CURRENT_DATE - 100
    GROUP BY 1
)
SELECT wsi.date,
       wsi.touch_mkt_channel,
       wsi.touch_experience,
       COALESCE(wsi.se_user_spvs, 0)                                       AS spvs,
       COALESCE(wsi.bookings, 0)                                           AS bookings,
       COALESCE(t.total_spvs, 0)                                           AS total_spvs,
       COALESCE(t.total_bookings, 0)                                       AS total_bookings,
       COALESCE(SUM(wsi.se_user_spvs)
                    OVER (PARTITION BY wsi.touch_mkt_channel, wsi.touch_experience ORDER BY wsi.date ROWS BETWEEN 39 PRECEDING AND CURRENT ROW ),
                0)                                                         AS rolling_4_week_spvs,
       COALESCE(SUM(wsi.bookings)
                    OVER (PARTITION BY wsi.touch_mkt_channel, wsi.touch_experience ORDER BY wsi.date ROWS BETWEEN 39 PRECEDING AND CURRENT ROW ),
                0)                                                         AS rolling_4_week_bookings,

       rolling_4_week_bookings / NULLIF(rolling_4_week_spvs, 0)            AS rolling_4_week_spv_conversion,
       COALESCE(SUM(t.total_spvs)
                    OVER (PARTITION BY wsi.touch_mkt_channel, wsi.touch_experience ORDER BY wsi.date ROWS BETWEEN 39 PRECEDING AND CURRENT ROW ),
                0)                                                         AS rolling_4_week_total_spvs,
       COALESCE(SUM(t.total_bookings)
                    OVER (PARTITION BY wsi.touch_mkt_channel, wsi.touch_experience ORDER BY wsi.date ROWS BETWEEN 39 PRECEDING AND CURRENT ROW ),
                0)                                                         AS rolling_4_week_total_bookings,
       rolling_4_week_total_bookings / rolling_4_week_total_spvs           AS rolling_4_week_total_spv_conversion,
       rolling_4_week_spv_conversion / rolling_4_week_total_spv_conversion AS weight

FROM scratch.robinpatel.weighted_spvs_input wsi
    LEFT JOIN daily_totals t ON wsi.date = t.date
WHERE wsi.date >= CURRENT_DATE - 100;


SELECT *
FROM data_vault_mvp.finance.svb_manual_refund smr

SELECT GET_DDL('table', 'scratch.robinpatel.weighted_spvs_input');


CREATE OR REPLACE TRANSIENT TABLE weighted_spvs_input
(
    date              DATE,
    touch_mkt_channel VARCHAR(16777216),
    touch_experience  VARCHAR(16777216),
    se_user_spvs      NUMBER(30, 0),
    bookings          NUMBER(30, 0)
);

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions CLONE data_vault_mvp.single_customer_view_stg.module_touched_transactions;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution CLONE data_vault_mvp.single_customer_view_stg.module_touch_attribution;

self_describing_task --include 'bi/weighted_spvs.py'  --method 'run' --start '2021-06-16 00:00:00' --end '2021-06-16 00:00:00'

SELECT GET_DDL('table', 'se_dev_robin.data.fact_complete_booking');

SELECT *
FROM data_vault_mvp_dev_robin.bi.scv_weighted_spvs;

self_describing_task --include '/bi/daily_spvs_bookings.py'  --method 'run' --start '2021-06-22 00:00:00' --end '2021-06-22 00:00:00'


DROP TABLE data_vault_mvp_dev_robin.bi.daily_spvs_bookings;

SELECT *
FROM data_vault_mvp_dev_robin.bi.daily_spvs_bookings__step03__aggregate_data;

SELECT *
FROM data_vault_mvp_dev_robin.bi.daily_spvs_bookings
    QUALIFY COUNT(*) OVER (PARTITION BY date, lnd_touch_mkt_channel,touch_experience, member_status) > 1
    self_describing_task --include 'bi/daily_spv_weight.py'  --method 'run' --start '2021-06-22 00:00:00' --end '2021-06-22 00:00:00'

SELECT *
FROM data_vault_mvp_dev_robin.bi.daily_spv_weight
WHERE date = CURRENT_DATE - 1
;

SELECT *
FROM data_vault_mvp_dev_robin.bi.daily_spvs_bookings;

DROP TABLE data_vault_mvp_dev_robin.bi.daily_spv_weight;

SELECT *
FROM data_vault_mvp_dev_robin.bi.daily_spv_weight__step01__daily_totals;

SELECT *
FROM data_vault_mvp_dev_robin.bi.daily_spv_weight
WHERE daily_spv_weight.date = CURRENT_DATE - 1;

CREATE SCHEMA collab.bi;
GRANT USAGE ON SCHEMA collab.bi TO ROLE personal_role__roseyin;
GRANT SELECT ON ALL TABLES IN SCHEMA data_vault_mvp_dev_robin.bi TO ROLE personal_role__roseyin;

CREATE OR REPLACE TRANSIENT TABLE collab.bi.daily_spv_weight AS
SELECT daily_spv_weight.date,
       daily_spv_weight.lnd_touch_mkt_channel,
       daily_spv_weight.touch_experience,
       daily_spv_weight.spvs,
       daily_spv_weight.bookings,
       daily_spv_weight.total_spvs,
       daily_spv_weight.total_bookings,
       daily_spv_weight.rolling_4_week_spvs,
       daily_spv_weight.rolling_4_week_bookings,
       daily_spv_weight.rolling_4_week_spv_conversion,
       daily_spv_weight.rolling_4_week_total_spvs,
       daily_spv_weight.rolling_4_week_total_bookings,
       daily_spv_weight.rolling_4_week_total_spv_conversion,
       daily_spv_weight.weight
FROM data_vault_mvp_dev_robin.bi.daily_spv_weight;

SELECT *
FROM data_vault_mvp_dev_robin.bi.daily_spv_weight;

self_describing_task --include 'bi/daily_spvs_bookings.py'  --method 'run' --start '2018-01-01 00:00:00' --end '2018-01-01 00:00:00'

SELECT *
FROM collab.bi.daily_spv_weight dsw
WHERE dsw.date = CURRENT_DATE - 1
  AND spvs < bookings;

GRANT SELECT ON TABLE collab.bi.daily_spv_weight TO ROLE personal_role__roseyin;


USE WAREHOUSE pipe_xlarge;
SELECT sts.*,
       stba.*,
       stmc.touch_mkt_channel
FROM se.data.scv_touched_spvs sts
    INNER JOIN se.data.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
    INNER JOIN se.data.scv_touch_attribution sta ON sts.touch_id = sta.touch_id AND sta.attribution_model = 'last non direct'
    INNER JOIN se.data.scv_touch_marketing_channel stmc ON sta.attributed_touch_id = stmc.touch_id
WHERE sts.event_tstamp::DATE = '2021-06-22'
  AND stmc.touch_mkt_channel = 'Email - Triggers'
  AND stba.touch_experience = 'native app android';


SELECT *
FROM se.data.scv_touched_transactions stt
WHERE stt.touch_id IN ('0d3ea1e2b0f7d4d6e292522bbe03f2180976912ee789cbd883f57896ba95ef85',
                       '756bc2d3000143cade21fc323d4d589b965b5cdfc96cec5c38de2780cf9b8a16'
    )


SELECT stt.*,
       stba.*,
       stmc.touch_mkt_channel
FROM se.data.scv_touched_transactions stt
    INNER JOIN se.data.scv_touch_basic_attributes stba ON stt.touch_id = stba.touch_id
    INNER JOIN se.data.scv_touch_attribution sta ON stt.touch_id = sta.touch_id AND sta.attribution_model = 'last non direct'
    INNER JOIN se.data.scv_touch_marketing_channel stmc ON sta.attributed_touch_id = stmc.touch_id
WHERE stt.event_tstamp::DATE = '2021-06-22'
  AND stmc.touch_mkt_channel = 'Email - Triggers'
  AND stba.touch_experience = 'native app android';

SELECT *
FROM se.data_pii.scv_session_events_link ssel
    INNER JOIN se.data_pii.scv_event_stream ses ON ssel.event_hash = ses.event_hash
WHERE ssel.touch_id IN ('c2c8a660b54b684013bf8ff4abc32356721f2367ce4e71550aa6074043f63dff',
                        '8d4833c54a4f1438e43d92459b903cafef16a1d519e10a863a0d5fcf9c3fc51a',
                        '46823f690210ca46224b15a582d16ac0d3828b80161eb8d6d9ac00e3213ac7c4',
                        'edb82e8ef002631ff3f4466790bf10f93498a7ec018959c3c4e77bc96c630557',
                        'cbe9ba8c9d49bd6d8ddc80845033e87dd14ca7c1e9f7bc10a571907671a263df',
                        '9082fd9ac18e7c3b1e218eb3f1c7caa9baabcdae1ba4f91d54bd6176ad019289',
                        'd683951925cbd6c186b343c64d1f41333d2bddb5955b6cbcc1b77bcaae639341',
                        '433627448de152eec4eb8f4481e69a6000dfaafbbff335632166675b27a97382'
    );


SELECT booking_id,
       REPLACE(refund_element, ' ', '')             AS refund_element_trimmed,
       SPLIT_PART(refund_element_trimmed, '-', '2') AS booking_ref
FROM se.finance.svb_manual_refund;

CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.svb.svb_statement CLONE hygiene_snapshot_vault_mvp.svb.svb_statement;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_booking CLONE data_vault_mvp.dwh.se_booking;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tb_booking CLONE data_vault_mvp.dwh.tb_booking;

self_describing_task --include 'data_vault_mvp/finance/svb/manual_refund.py'  --method 'run' --start '2021-06-22 00:00:00' --end '2021-06-22 00:00:00';

SELECT *
FROM data_vault_mvp_dev_robin.finance.svb_manual_refund smr
SELECT *
FROM data_vault_mvp.finance.svb_manual_refund smr;


SELECT *
FROM se.bi.daily_spv_weight;

SELECT *
FROM se.bi.daily_spvs_bookings;


------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions CLONE data_vault_mvp.single_customer_view_stg.module_touched_transactions;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution CLONE data_vault_mvp.single_customer_view_stg.module_touch_attribution;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.fact_booking CLONE data_vault_mvp.dwh.fact_booking;

SELECT *
FROM data_vault_mvp.dwh.fact_booking fb;



self_describing_task --include 'bi/daily_spvs_bookings.py'  --method 'run' --start '2021-07-07 00:00:00' --end '2021-07-07 00:00:00'



airflow backfill --start_date '2021-07-07 00:00:00' --end_date '2021-07-08 00:00:00' --task_regex '.*' bi__daily_spv_weight__daily_at_04h00
DROP TABLE data_vault_mvp.bi.daily_spvs_bookings;
DROP TABLE data_vault_mvp.bi.daily_spv_weight;

self_describing_task --include 'bi/daily_spv_weight.py'  --method 'run' --start '2021-07-07 00:00:00' --end '2021-07-07 00:00:00'

airflow backfill --start_date '2018-01-01 00:00:00' --end_date '2018-01-02 00:00:00' --reset_dagruns --task_regex '.*' bi__daily_spv_weight__daily_at_04h00


SELECT *
FROM data_vault_mvp_dev_robin.bi.daily_spvs_bookings;

SELECT *
FROM data_vault_mvp_dev_robin.bi.daily_spv_weight;