/*
 FYI

In a strive to detect glitches on the Service Escapes website caused by changes deployed to our systems as early as
 possible, the Tech team has started an initiative around running a set of automated tests mimicking the behaviour of
 travelers on the Website going through our core journeys related to search, visiting sale pages, visiting booking
 forms, etc. The tests are performed on both desktop and web browsers running on Android devices.

Why is this important to me?

The automated tests often create users on the website. In case metrics that you use are somehow dependent on the number
 of new users created over some period of time, you may want to inspect them. Is the data skewed in any way for the
 last 30 days?

If you find any discrepancies, you could manually filter out test users and exclude them from further data analysis.
 Their email address would always look like "test-e2e-..@secretescapes.com". Some teams - Data and CRM - are aware of our
 initiative and take care to filter the activities of such users out.

If you find discrepancies caused by excessive test user creation and you don't know how to resolve the issue, please
 reach out to me immediately.
 */


SELECT *
FROM se.data_pii.se_user_attributes sua
WHERE LOWER(sua.email) LIKE 'test-e2e-%@secretescapes.com'


------------------------------------------------------------------------------------------------------------------------
USE ROLE PERSONAL_ROLE__ROBINPATEL;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.cms_mysql;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.affiliate
CLONE latest_vault.cms_mysql.affiliate;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.membership
CLONE latest_vault.cms_mysql.membership;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.profile
CLONE latest_vault.cms_mysql.profile;

CREATE SCHEMA IF NOT EXISTS hygiene_vault_dev_robin.cms_mysql;

CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_dev_robin.cms_mysql.shiro_user
CLONE hygiene_vault.cms_mysql.shiro_user;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.shiro_user
CLONE latest_vault.cms_mysql.shiro_user;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.staff_discount_profile
CLONE latest_vault.cms_mysql.staff_discount_profile;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.subscription
CLONE latest_vault.cms_mysql.subscription;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.territory
CLONE latest_vault.cms_mysql.territory;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.theme
CLONE latest_vault.cms_mysql.theme;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.iterable;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.iterable.app_users
CLONE latest_vault.iterable.app_users;

CREATE SCHEMA IF NOT EXISTS raw_vault_mvp_dev_robin.chiasma_sql_server;

CREATE OR REPLACE TRANSIENT TABLE raw_vault_mvp_dev_robin.chiasma_sql_server.user_acquisition_platform
CLONE raw_vault_mvp.chiasma_sql_server.user_acquisition_platform;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.cms_mongodb;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mongodb.users
CLONE latest_vault.cms_mongodb.users;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_booking_review
CLONE data_vault_mvp.dwh.user_booking_review;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_attributes
CLONE data_vault_mvp.dwh.user_attributes;

self_describing_task \
    --include 'biapp.task_catalogue.dv.dwh.user_attributes.user_attributes.py' \
    --method 'run' \
    --start '2025-01-23 00:00:00' \


SELECT * FROM se.data.se_user_attributes sua;



