self_describing_task --include 'staging/hygiene/sfmc/events_bounces.py'  --method 'run' --start '2020-03-24 00:00:00' --end '2020-03-24 00:00:00'

CREATE OR REPLACE TRANSIENT TABLE raw_vault_mvp_dev_robin.sfmc.events_bounces CLONE raw_vault_mvp.sfmc.events_bounces;

SELECT MIN(loaded_at)
FROM raw_vault_mvp.sfmc.events_bounces eb; --2020-03-24 18:34:10.960405000

SELECT *
FROM raw_vault_mvp_dev_robin.sfmc.events_bounces;

WITH ua_shiro_user_id AS (
        -- found instances where an email address lives in the subscriber_key
        -- field rather than a shiro user id. Searching the shiro user table
        -- to return matches on the email address to a shiro user id.
            SELECT
                eb.email_address,
                ua.shiro_user_id

            FROM hygiene_vault_mvp_dev_robin.sfmc.events_bounces__step01__get_source_batch ec
            INNER JOIN data_vault_mvp_dev_robin.dwh.user_attributes ua ON eb.email_address = ua.email
            -- rows where there's no user id in the subscriber key
            WHERE TRY_TO_NUMBER(eb.subscriber_key) IS NULL
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY ua.shiro_user_id
                ORDER BY ua.created_at DESC
            ) = 1
        )
;

SELECT * FROM hygiene_vault_mvp_dev_robin.sfmc.events_bounces__step01__get_source_batch


self_describing_task --include 'staging/hygiene_snapshots/sfmc/events_bounces.py'  --method 'run' --start '2021-10-21 00:00:00' --end '2021-10-21 00:00:00'

SELECT * FROM hygiene_snapshot_vault_mvp_dev_robin.sfmc.events_bounces;

airflow backfill --start_date '2020-03-24 00:00:00' --end_date '2020-03-25 00:00:00' --task_regex '.*' hygiene_snapshots__sfmc__events_bounces__daily_at_03h00
airflow backfill --start_date '2021-10-21 00:00:00' --end_date '2021-10-22 00:00:00' --task_regex '.*' --m hygiene_snapshots__sfmc__events_bounces__daily_at_03h00

select * from hygiene_snapshot_vault_mvp.sfmc.events_bounces ub
where ub.shiro_user_id is null;

SELECT * FROm data_vault_mvp.dwh.user_attributes ua
QUALIFY COUNT(*) OVER (PARTITION BY ua.email) >1;

self_describing_task --include 'staging/outgoing/iterable/user_profile_historical/modelling.py'  --method 'run' --start '2021-10-21 00:00:00' --end '2021-10-21 00:00:00'
CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.iterable__user_profile AS SELECT * FROM data_vault_mvp.dwh.iterable__user_profile;

SELECT * FROM data_vault_mvp.dwh.user_attributes ua;

self_describing_task --include 'dv/dwh/iterable/user_profile.py'  --method 'run' --start '2021-10-21 00:00:00' --end '2021-10-21 00:00:00';


SELECT * FROM hygiene_snapshot_vault_mvp.cms_mysql.profile p WHERE p.region IS NOT NULL;

SELECT * FROm unload_vault_mvp_dev_robin.iterable.user_profile_historical__20211020T030000__daily_at_03h00
WHERE record['dataFields']['referredBy']::VARCHAR IS NOT NULL;

DROP TABLe unload_vault_mvp_dev_robin.iterable.user_profile_historical__20211020T030000__daily_at_03h00;
