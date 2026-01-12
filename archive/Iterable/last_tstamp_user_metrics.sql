SELECT fb.shiro_user_id,
       MAX(IFF(fb.booking_status_type = 'abandoned', fb.booking_created_date, NULL))                   AS last_abandoned_booking_tstamp,
       MAX(IFF(fb.booking_status_type IN ('live', 'cancelled'), fb.booking_completed_timestamp, NULL)) AS last_purchase_booking_tstamp,
       MAX(IFF(fb.booking_status_type = 'live', fb.booking_completed_timestamp, NULL))                 AS last_complete_booking_tstamp
FROM data_vault_mvp.dwh.fact_booking fb
GROUP BY 1
HAVING last_abandoned_booking_tstamp IS NOT NULL
    OR last_purchase_booking_tstamp IS NOT NULL
    OR last_complete_booking_tstamp IS NOT NULL
;


CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.shiro_user CLONE hygiene_snapshot_vault_mvp.cms_mysql.shiro_user;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.sfmc.events_clicks CLONE hygiene_snapshot_vault_mvp.sfmc.events_clicks;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.sfmc.events_opens_plus_inferred CLONE hygiene_snapshot_vault_mvp.sfmc.events_opens_plus_inferred;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.fact_booking CLONE data_vault_mvp.dwh.fact_booking;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_last_pageview CLONE data_vault_mvp.dwh.user_last_pageview;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_last_spv CLONE data_vault_mvp.dwh.user_last_spv;

self_describing_task --include 'dv/dwh/user_attributes/user_recent_activities.py'  --method 'run' --start '2021-10-06 00:00:00' --end '2021-10-06 00:00:00'

SELECT *
FROM data_vault_mvp_dev_robin.dwh.user_recent_activities;

inspect_dependencies biapp/
task_catalogue/
dv/
dwh/
user_attributes/
user_recent_activities.py --downstream

/
USERS/
robin/
myrepos/
one-DATA-pipeline/
biapp/
task_catalogue/
dv/
dwh/
user_attributes/
user_recent_activities.py

DROP TABLE data_vault_mvp_dev_robin.dwh.user_recent_activities;


SELECT iup.shiro_user_id,
       iup.reference
FROM data_vault_mvp.dwh.iterable__user_profile iup
WHERE iup.shiro_user_id IN (
                            '11202545',
                            '20922280',
                            '47311303',
                            '69203857',
                            '59480018',
                            '57033713',
                            '25952443',
                            '57328696',
                            '66218664',
                            '38953955'
    )

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.dwh.user_recent_activities_20211008 CLONE data_vault_mvp.dwh.user_recent_activities;

SELECT COUNT(*)
FROM data_vault_mvp.dwh.user_recent_activities ura;
SELECT COUNT(*)
FROM data_vault_mvp.dwh.user_recent_activities_20211008 ura;

SELECT COUNT(*)
FROM data_vault_mvp.dwh.user_recent_activities ura
WHERE last_sale_pageview_tstamp IS NOT NULL;
SELECT COUNT(*)
FROM data_vault_mvp.dwh.user_recent_activities_20211008 ura
WHERE last_sale_pageview_tstamp IS NOT NULL;


SELECT ura.last_sale_pageview_tstamp::DATE,
       COUNT(*)
FROM data_vault_mvp.dwh.user_recent_activities ura
WHERE last_sale_pageview_tstamp >= current_date - 7
GROUP BY 1
ORDER BY 1;

SELECT ura.last_sale_pageview_tstamp::DATE,
       COUNT(*)
FROM data_vault_mvp_dev_robin.dwh.user_recent_activities ura
WHERE last_sale_pageview_tstamp >= current_date - 7
GROUP BY 1
ORDER BY 1;

SELECT COUNT(*) FROM data_vault_mvp.dwh.user_recent_activities;
SELECT COUNT(*) FROM data_vault_mvp.dwh.user_recent_activities_20211008;