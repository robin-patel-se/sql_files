USE WAREHOUSE pipe_2xlarge;

WITH row_per AS (
    SELECT *,
           ROW_NUMBER() OVER (ORDER BY iupa.shiro_user_id)                    AS row_number,
           ROW_NUMBER() OVER (ORDER BY iupa.shiro_user_id) / COUNT(*) OVER () AS row_percentile
    FROM data_vault_mvp.dwh.iterable__user_profile_activity iupa
    WHERE iupa.updated_at >= CURRENT_DATE - 1
)
SELECT *
FROM row_per
WHERE row_per.row_percentile > 0.25
;


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity CLONE data_vault_mvp.dwh.iterable__user_profile_activity;


SELECT *
FROM data_vault_mvp.dwh.iterable__user_profile_activity iupa
WHERE iupa.updated_at >= CURRENT_DATE - 1
    QUALIFY ROW_NUMBER() OVER (ORDER BY iupa.shiro_user_id) / COUNT(*) OVER () BETWEEN 0.25 AND 0.5
    self_describing_task --include 'biapp/task_catalogue/staging/outgoing/iterable/user_profile_activity/modelling_first_quartile.py'  --method 'run' --start '2022-08-18 00:00:00' --end '2022-08-18 00:00:00'


WITH compute_quartile AS (
    SELECT *,
           ROW_NUMBER() OVER (ORDER BY iv.shiro_user_id)                    AS row_number,
           ROW_NUMBER() OVER (ORDER BY iv.shiro_user_id) / COUNT(*) OVER () AS row_percentile
    FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity sv
    WHERE sv.updated_at > '2022-08-17 03:00:00'::TIMESTAMP
)
SELECT *
FROM compute_quartile
WHERE row_percentile < 0.25

SELECT *
FROM unload_vault_mvp_dev_robin.iterable.user_profile_activity_first_quartile__20220817t030000__daily_at_03h00;