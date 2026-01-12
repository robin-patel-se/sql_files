USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh
;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.fact_booking
AS
SELECT *
FROM data_vault_mvp.dwh.fact_booking
;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.cms_mysql
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.profile
	CLONE latest_vault.cms_mysql.profile
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_credit
	CLONE data_vault_mvp.dwh.se_credit
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_voucher
	CLONE data_vault_mvp.dwh.se_voucher
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.shiro_user
	CLONE latest_vault.cms_mysql.shiro_user
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_attributes
	CLONE data_vault_mvp.dwh.user_attributes
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_recent_activities
	CLONE data_vault_mvp.dwh.user_recent_activities
;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.inactive_users
	CLONE data_vault_mvp.dwh.inactive_users
;

/*
self_describing_task \
    --include 'biapp.task_catalogue.dv.dwh.camilla.inactive_users.py' \
    --method 'run' \
    --start '2024-12-19 00:00:00' \
    --end '2024-12-19 00:00:00'


self_describing_task \
	--include 'biapp/task_catalogue/staging/outgoing/camilla/inactive_users/modelling.py'  \
	--method 'run' \
	--start '2024-12-18 00:00:00' \
	--end '2024-12-18 00:00:00'

*/
SELECT *
FROM unload_vault_mvp_dev_robin.camilla.inactive_users__20241217t043000__daily_at_04h30
;

/*
dataset_task \
    --include 'outgoing.camilla.inactive_users' \
    --operation UnloadOperation \
    --method 'run' \
    --start '2024-12-18 00:00:00' \
    --end '2024-12-18 00:00:00'


dataset_task
\
    --include 'outgoing.camilla.inactive_users' \
    --operation DistributeOperation \
    --method 'run' \
    --start '2024-12-18 00:00:00' \
    --end '2024-12-18 00:00:00'

*/

DROP TABLE data_vault_mvp_dev_robin.dwh.inactive_users
;


------------------------------------------------------------------------------------------------------------------------
-- sense checking data

SELECT
	COUNT(*)
FROM data_vault_mvp_dev_robin.dwh.inactive_users iu
;

SELECT
	COUNT(*)
FROM customer_insight.temp.ah_users_for_deletion_20241218 ud
;


-- check how many are the overlap of both lists
SELECT
	COUNT(*)
FROM customer_insight.temp.ah_users_for_deletion_20241218 ud
	INNER JOIN data_vault_mvp_dev_robin.dwh.inactive_users iu ON ud.shiro_user_id = iu.shiro_user_id

-- to check users that are on CI's delete list but not in active user list
WITH
	users_not_on_delete_list AS (
		SELECT
			ud.*
		FROM customer_insight.temp.ah_users_for_deletion_20241218 ud
			LEFT JOIN data_vault_mvp_dev_robin.dwh.inactive_users iu ON ud.shiro_user_id = iu.shiro_user_id
		WHERE iu.shiro_user_id IS NULL
	)
SELECT
	sua.*
FROM users_not_on_delete_list dl
	INNER JOIN se.data_pii.se_user_attributes sua ON sua.shiro_user_id = dl.shiro_user_id
;

------------------------------------------------------------------------------------------------------------------------

-- with removing logic of email opens and extending the non booker window to 7 years the current production job outputs
-- 1.1M users to be deleted today on the 19th December 2024

-- cross-referencing this with Alex H's list of 103K users has a cross over of 100,413 users that are in both lists.
-- going to prune the inactive users dataset to only users that are in both lists and run outgoing job

-- backup table
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.inactive_users_20241219 CLONE data_vault_mvp_dev_robin.dwh.inactive_users
;

-- update local table to only be the crossover of both lists
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.inactive_users AS (
	SELECT
		iu.*
	FROM data_vault_mvp_dev_robin.dwh.inactive_users_20241219 iu
		INNER JOIN customer_insight.temp.ah_users_for_deletion_20241218 ud ON ud.shiro_user_id = iu.shiro_user_id
)

-- sense check the output
SELECT *
FROM data_vault_mvp_dev_robin.dwh.inactive_users
;

-- list is now 100,413 users.

-- rerun outgoing jobs

/*
self_describing_task \
	--include 'biapp/task_catalogue/staging/outgoing/camilla/inactive_users/modelling.py'  \
	--method 'run' \
	--start '2024-12-19 00:00:00' \
	--end '2024-12-19 00:00:00'


dataset_task \
    --include 'outgoing.camilla.inactive_users' \
    --operation UnloadOperation \
    --method 'run' \
    --start '2024-12-19 00:00:00' \
    --end '2024-12-19 00:00:00'


dataset_task \
    --include 'outgoing.camilla.inactive_users' \
    --operation DistributeOperation \
    --method 'run' \
    --start '2024-12-19 00:00:00' \
    --end '2024-12-19 00:00:00'
 */

-- 5 files were placed in the s3 bucket

SELECT *
FROM data_vault_mvp_dev_robin.dwh.inactive_users
WHERE inactive_users.shiro_user_id = 9738708
;



CREATE OR REPLACE TABLE customer_insight.temp.rp_users_passed_to_tech_to_delete_20241219 AS
SELECT *
FROM data_vault_mvp_dev_robin.dwh.inactive_users
;

USE ROLE personal_role__alexhenshaw
;

USE WAREHOUSE pipe_default

SELECT *
FROM customer_insight.temp.rp_users_passed_to_tech_to_delete_20241219


SELECT *
FROM customer_insight.temp.rp_users_passed_to_tech_to_delete_20241219 del
	LEFT JOIN se.data.se_user_attributes sua ON del.shiro_user_id = sua.shiro_user_id
WHERE sua.membership_account_status IS DISTINCT FROM 'DELETED'
;

------------------------------------------------------------------------------------------------------------------------


SELECT *
FROM latest_vault.cms_mysql.profile p
WHERE p.last_updated IS NULL
;


-- 99K profiles that have a null last updated
SELECT *
FROM hygiene_vault.cms_mysql.profile p
WHERE id = 43348
;

SELECT *
FROM latest_vault.cms_mysql.profile p
WHERE id = 43348
;


WITH
	ids AS (
		SELECT
			id,
			p.mobile_phone,
			p.home_phone,
			p.telegraph_phone
		FROM latest_vault.cms_mysql.profile p
		WHERE p.last_updated IS NULL
	),
	bookings AS (
		SELECT *
		FROM se.data.fact_booking fb
		WHERE fb.booking_status_type IN ('live', 'cancelled')
		  AND fb.booking_completed_timestamp >= '2024-10-14'
		  AND fb.se_brand = 'SE Brand'
		  AND fb.tech_platform = 'SECRET_ESCAPES'
	)
SELECT
	b.booking_id,
	sua.mobile_phone
FROM bookings b
	INNER JOIN se.data_pii.se_user_attributes sua ON b.shiro_user_id = sua.shiro_user_id
	INNER JOIN ids ON sua.profile_id = ids.id
;

SELECT
	sua.profile_id
FROM se.data_pii.se_user_attributes sua
WHERE sua.shiro_user_id = '184325'
;



SELECT *
FROM hygiene_vault.cms_mysql.profile p
WHERE id = 20870
;


SELECT *
FROM latest_vault.cms_mysql.reservation r
WHERE r.last_updated IS NULL


-- voucher
SELECT
	COUNT(*)
FROM latest_vault.cms_mysql.voucher
WHERE last_updated IS NULL
;
-- refunded_amount
SELECT
	COUNT(*)
FROM latest_vault.cms_mysql.refunded_amount
WHERE last_updated IS NULL
;
-- subscription
SELECT
	COUNT(*)
FROM latest_vault.cms_mysql.subscription
WHERE last_updated IS NULL
;
-- base_cancellation_policy
SELECT
	COUNT(*)
FROM latest_vault.cms_mysql.base_cancellation_policy
WHERE last_updated IS NULL
;
-- membership
SELECT
	COUNT(*)
FROM latest_vault.cms_mysql.membership
WHERE last_updated IS NULL
;
-- allocation_item
SELECT
	COUNT(*)
FROM latest_vault.cms_mysql.allocation_item
WHERE last_updated IS NULL
;
-- sale
SELECT
	COUNT(*)
FROM latest_vault.cms_mysql.sale
WHERE last_updated IS NULL
;
-- base_sale
SELECT
	COUNT(*)
FROM latest_vault.cms_mysql.base_sale
WHERE last_updated IS NULL
;
-- removed_favorite
SELECT
	COUNT(*)
FROM latest_vault.cms_mysql.removed_favorite
WHERE last_updated IS NULL
;
-- promo_code_redemption
SELECT
	COUNT(*)
FROM latest_vault.cms_mysql.promo_code_redemption
WHERE last_updated IS NULL
;
-- promo_code
SELECT
	COUNT(*)
FROM latest_vault.cms_mysql.promo_code
WHERE last_updated IS NULL
;
-- activation_request
SELECT
	COUNT(*)
FROM latest_vault.cms_mysql.activation_request
WHERE last_updated IS NULL
;
-- city
SELECT
	COUNT(*)
FROM latest_vault.cms_mysql.city
WHERE last_updated IS NULL
;
-- reservation_exchange_rate
SELECT
	COUNT(*)
FROM latest_vault.cms_mysql.reservation_exchange_rate
WHERE last_updated IS NULL
;
-- product_reservation
SELECT
	COUNT(*)
FROM latest_vault.cms_mysql.product_reservation
WHERE last_updated IS NULL
;
-- sale_translation
SELECT
	COUNT(*)
FROM latest_vault.cms_mysql.sale_translation
WHERE last_updated IS NULL
;
-- credit_version
SELECT
	COUNT(*)
FROM latest_vault.cms_mysql.credit_version
WHERE last_updated IS NULL
;
-- affiliate_user
SELECT
	COUNT(*)
FROM latest_vault.cms_mysql.affiliate_user
WHERE last_updated IS NULL
;
-- external_booking
SELECT
	COUNT(*)
FROM latest_vault.cms_mysql.external_booking
WHERE last_updated IS NULL
;
-- wish_list
SELECT
	COUNT(*)
FROM latest_vault.cms_mysql.wish_list
WHERE last_updated IS NULL
;
-- affiliate
SELECT
	COUNT(*)
FROM latest_vault.cms_mysql.affiliate
WHERE last_updated IS NULL
;
-- triggered_email
SELECT
	COUNT(*)
FROM latest_vault.cms_mysql.triggered_email
WHERE last_updated IS NULL
;
-- sale_details
SELECT
	COUNT(*)
FROM latest_vault.cms_mysql.sale_details
WHERE last_updated IS NULL
;
-- booking_cancellation
SELECT
	COUNT(*)
FROM latest_vault.cms_mysql.booking_cancellation
WHERE last_updated IS NULL
;
-- offer_details
SELECT
	COUNT(*)
FROM latest_vault.cms_mysql.offer_details
WHERE last_updated IS NULL
;
-- wish_list_item
SELECT
	COUNT(*)
FROM latest_vault.cms_mysql.wish_list_item
WHERE last_updated IS NULL
;
-- exchange_rate
SELECT
	COUNT(*)
FROM latest_vault.cms_mysql.exchange_rate
WHERE last_updated IS NULL
;
-- allocation
SELECT
	COUNT(*)
FROM latest_vault.cms_mysql.allocation
WHERE last_updated IS NULL
;
-- feature_toggle
SELECT
	COUNT(*)
FROM latest_vault.cms_mysql.feature_toggle
WHERE last_updated IS NULL
;
-- promotion
SELECT
	COUNT(*)
FROM latest_vault.cms_mysql.promotion
WHERE last_updated IS NULL
;
-- payment
SELECT
	COUNT(*)
FROM latest_vault.cms_mysql.payment
WHERE last_updated IS NULL
;
-- billing
SELECT
	COUNT(*)
FROM latest_vault.cms_mysql.billing
WHERE last_updated IS NULL
;
-- offer_translation
SELECT
	COUNT(*)
FROM latest_vault.cms_mysql.offer_translation
WHERE last_updated IS NULL
;
-- staff_discount_profile
SELECT
	COUNT(*)
FROM latest_vault.cms_mysql.staff_discount_profile
WHERE last_updated IS NULL
;
-- booking
SELECT
	COUNT(*)
FROM latest_vault.cms_mysql.booking
WHERE last_updated IS NULL
;
-- reservation
SELECT
	COUNT(*)
FROM latest_vault.cms_mysql.reservation
WHERE last_updated IS NULL
;
-- stripe_transaction
SELECT
	COUNT(*)
FROM latest_vault.cms_mysql.stripe_transaction
WHERE last_updated IS NULL
;
-- rebooking
SELECT
	COUNT(*)
FROM latest_vault.cms_mysql.rebooking
WHERE last_updated IS NULL
;
-- favorite
SELECT
	COUNT(*)
FROM latest_vault.cms_mysql.favorite
WHERE last_updated IS NULL
;
-- location_info
SELECT
	COUNT(*)
FROM latest_vault.cms_mysql.location_info
WHERE last_updated IS NULL
;
-- shiro_user
SELECT
	COUNT(*)
FROM latest_vault.cms_mysql.shiro_user
WHERE last_updated IS NULL
;
-- imported_user_data
SELECT
	COUNT(*)
FROM latest_vault.cms_mysql.imported_user_data
WHERE last_updated IS NULL
;
-- credit
SELECT
	COUNT(*)
FROM latest_vault.cms_mysql.credit
WHERE last_updated IS NULL
;
-- booking_note
SELECT
	COUNT(*)
FROM latest_vault.cms_mysql.booking_note
WHERE last_updated IS NULL
;
-- offer
SELECT
	COUNT(*)
FROM latest_vault.cms_mysql.offer
WHERE last_updated IS NULL
;
-- profile
SELECT
	COUNT(*)
FROM latest_vault.cms_mysql.profile
WHERE profile.last_updated IS NULL
;

USE WAREHOUSE pipe_xlarge
;
------------------------------------------------------------------------------------------------------------------------
WITH
	affected_memberships AS (
		SELECT *
		FROM latest_vault.cms_mysql.membership_20241220 m
		WHERE m.last_updated IS NULL
	),
	affected_profiles AS (
		SELECT *
		FROM latest_vault.cms_mysql.profile p
		WHERE last_updated IS NULL
	),
	members_potentially_affected AS (
		SELECT *
		FROM se.data_pii.se_user_attributes sua
			LEFT JOIN affected_memberships am ON sua.membership_id = am.id
			LEFT JOIN affected_profiles ap ON sua.profile_id = ap.id
		WHERE (am.id IS NOT NULL OR ap.id IS NOT NULL)
	)
SELECT
	mpa.membership_account_status,
	mpa.email_opt_in_status,
	MIN(mpa.signup_tstamp)            AS first_sign_up,
	MAX(mpa.signup_tstamp)            AS last_sign_up,
	COUNT(DISTINCT mpa.shiro_user_id) AS members
FROM members_potentially_affected mpa
GROUP BY 1, 2
;
-- 191,094 users potentially affected by either incorrect membership type or


SELECT *
FROM hygiene_vault.cms_mysql.profile p
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY p.last_updated DESC) = 1



SELECT * FROM unload_vault_mvp_dev_robin.camilla.inactive_users__20250122T043000__daily_at_04h30;


SELECT * FROM data_vault_mvp.dwh.iterable__user_profile_activity iupa;

------------------------------------------------------------------------------------------------------------------------

