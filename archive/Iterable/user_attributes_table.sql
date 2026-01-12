CREATE OR REPLACE TRANSIENT TABLE raw_vault_mvp_dev_robin.chiasma_sql_server.affiliate_classification CLONE raw_vault_mvp.chiasma_sql_server.affiliate_classification;
CREATE OR REPLACE TRANSIENT TABLE raw_vault_mvp_dev_robin.chiasma_sql_server.user_acquisition_platform CLONE raw_vault_mvp.chiasma_sql_server.user_acquisition_platform;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.affiliate CLONE hygiene_snapshot_vault_mvp.cms_mysql.affiliate;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.membership CLONE hygiene_snapshot_vault_mvp.cms_mysql.membership;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.profile CLONE hygiene_snapshot_vault_mvp.cms_mysql.profile;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.shiro_user CLONE hygiene_snapshot_vault_mvp.cms_mysql.shiro_user;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.territory CLONE hygiene_snapshot_vault_mvp.cms_mysql.territory;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.theme CLONE hygiene_snapshot_vault_mvp.cms_mysql.theme;

self_describing_task --include 'dv/dwh/user_attributes/user_attributes.py'  --method 'run' --start '2021-09-13 00:00:00' --end '2021-09-13 00:00:00'

self_describing_task --include 'se/data_pii/dwh/se_user_attributes.py'  --method 'run' --start '2021-09-08 00:00:00' --end '2021-09-08 00:00:00'
self_describing_task --include 'se/data/dwh/se_user_attributes.py'  --method 'run' --start '2021-09-08 00:00:00' --end '2021-09-08 00:00:00'

SELECT *
FROM data_vault_mvp_dev_robin.dwh.user_attributes ua;

DESC TABLE data_vault_mvp_dev_robin.dwh.user_attributes;
SHOW TABLES LIKE 'user_attributes' IN DATABASE data_vault_mvp_dev_robin;
SHOW VIEWS LIKE 'se_user_attributes' IN DATABASE se_dev_robin;

SELECT ubr.shiro_user_id,
       COUNT(*)                                                                         AS number_of_reviews,
       COUNT_IF(ubr.customer_score >= 9)                                                AS promoter_reviews,
       COUNT_IF(ubr.customer_score BETWEEN 7 AND 8)                                     AS passive_reviews,
       COUNT_IF(ubr.customer_score <= 6)                                                AS detractor_reviews,
       AVG(customer_score)                                                              AS avg_review_score,
       --https://www.qualtrics.com/uk/experience-management/customer/measure-nps/?rid=ip&prevsite=en&newsite=uk&geo=GB&geomatch=uk
       (promoter_reviews / number_of_reviews) - (detractor_reviews / number_of_reviews) AS nps_score
FROM data_vault_mvp.dwh.user_booking_review ubr
GROUP BY 1;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_booking_review CLONE data_vault_mvp.dwh.user_booking_review;

SELECT s.user_id,
       s.activation_date,
       s.processed,
       s.receive_hand_picked_offers,
       s.receive_sales_reminders,
       s.receive_weekly_offers,
       s.date_created,
       s.last_updated
FROM hygiene_snapshot_vault_mvp.cms_mysql.subscription s
    QUALIFY ROW_NUMBER() OVER (PARTITION BY s.user_id ORDER BY s.last_updated DESC, s.date_created DESC) = 1;

self_describing_task --include 'se/data/dwh/se_user_attributes.py'  --method 'run' --start '2021-09-13 00:00:00' --end '2021-09-13 00:00:00'

inspect_dependencies biapp/task_catalogue/dv/dwh/user_attributes/user_attributes.py --downstream


SELECT * FROM data_vault_mvp.dwh.iterable__user_profile iup
WHERE iup.email_address = 'robin.patel@secretescapes.com';