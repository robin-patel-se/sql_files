
--daily version
--bulk all unsubs
--shiro user id

--ideally

SELECT u.event_date as unsubscribe_date,
       user_id as shiro_user_id,
       u.subscription_type as email_opt_in
FROM data_vault_mvp.dwh.user_subscription_event u
WHERE u.subscription_type = 0
  AND u.event_date = current_date - 1;

SELECT sua.shiro_user_id,
       sua.email_opt_in
FROM se.data.se_user_attributes sua
WHERE sua.email_opt_in = 0;