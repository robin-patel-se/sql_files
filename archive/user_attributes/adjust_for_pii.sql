SELECT *
FROM data_vault_mvp.cms_mysql_snapshots.shiro_user_snapshot sus
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.profile_snapshot ps ON sus.profile_id = ps.id;

DROP TABLE data_vault_mvp_dev_robin.dwh.user_attributes;

--include 'dv/dwh/user_attributes/user_attributes'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.dwh.user_first_activities CLONE data_vault_mvp.dwh.user_first_activities;

ALTER TABLE data_vault_mvp.dwh.user_first_activities
    ADD COLUMN app_cohort_id INT, app_cohort_year_month VARCHAR;

UPDATE data_vault_mvp.dwh.user_first_activities ufa
SET ufa.app_cohort_id         = DATEDIFF('month', '2018-12-31', ufa.first_app_activity_tstamp)::INT,
    ufa.app_cohort_year_month = TO_VARCHAR(ufa.first_app_activity_tstamp, 'YYYY-MM');

ALTER TABLE data_vault_mvp.dwh.user_first_activities
    ADD PRIMARY KEY (shiro_user_id);

SELECT *
FROM data_vault_mvp_dev_robin.dwh.user_attributes ua;

self_describing_task --include 'se/data_pii/se_user_attributes'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
self_describing_task --include 'se/data/se_user_attributes'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'


CREATE SCHEMA se_dev_robin.data_pii;

SELECT *
FROM se_dev_robin.data.se_user_attributes;
SELECT *
FROM se_dev_robin.data.se_user_attributes;
