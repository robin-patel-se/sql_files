-- Remove any users that signed up and was then deleted in 16 days that haven't made a booking and also BLOCKED users
WITH users_with_bookings AS (
    -- users that have ever made a booking
    SELECT DISTINCT
        fb.shiro_user_id
    FROM se.data.fact_booking fb
    WHERE fb.booking_status_type IN ('live', 'cancelled')
)
SELECT
    DATE_TRUNC(MONTH, ua.signup_tstamp) AS month,
    COUNT(*)
FROM data_vault_mvp.dwh.user_attributes ua
    LEFT JOIN users_with_bookings uwb ON ua.shiro_user_id = uwb.shiro_user_id
WHERE ua.membership_account_status NOT IN ('DELETED', 'BLOCKED')
   OR (
    -- add back in some deleted accounts
            ua.membership_account_status = 'DELETED'
        AND (
                -- when their deletion diff is more than 16 days
                        DATEDIFF(DAY, ua.signup_tstamp, ua.membership_last_updated) > 16
                    -- or they've made a booking
                    OR uwb.shiro_user_id IS NOT NULL
                )
    )
GROUP BY 1;


SELECT
    DATE_TRUNC(MONTH, ua.signup_tstamp) AS month,
    COUNT(*)
FROM data_vault_mvp.dwh.user_attributes ua
GROUP BY 1;

ALTER SESSION SET USE_CACHED_RESULT = FALSE;


SELECT
    DATE_TRUNC(MONTH, su.signup_date) AS month,
    SUM(su.members)
FROM dbt_dev.dbt_robinpatel.cohort_v4_member_signups su
GROUP BY 1;


WITH users_with_bookings AS (
    -- users that have ever made a booking
    SELECT DISTINCT
        fb.shiro_user_id
    FROM se.data.fact_booking fb
    WHERE fb.booking_status_type IN ('live', 'cancelled')
)
SELECT
    *
FROM data_vault_mvp.dwh.user_attributes ua
    LEFT JOIN users_with_bookings uwb ON ua.shiro_user_id = uwb.shiro_user_id
WHERE ua.membership_account_status NOT IN ('DELETED', 'BLOCKED')
   OR (
    -- add back in some deleted accounts
            ua.membership_account_status = 'DELETED'
        AND (
                -- when their deletion diff is more than 16 days
                        DATEDIFF(DAY, ua.signup_tstamp, ua.membership_last_updated) > 16
                    -- or they've made a booking
                    OR uwb.shiro_user_id IS NOT NULL
                )
    );