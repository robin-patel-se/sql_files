SELECT ua.*,
       sa.main_for_domain,
       sa.domain,
       sa.active
FROM data_vault_mvp.dwh.user_attributes ua
    LEFT JOIN se.data.se_affiliate sa ON ua.current_affiliate_id = sa.id
WHERE ua.current_affiliate_id = 147


DROP TABLE collab.iterable_data.main_affiliate_users_to_update;
CREATE TRANSIENT TABLE collab.iterable_data.main_affiliate_users_to_update AS (
    SELECT ua.shiro_user_id,
           ua.main_affiliate_id
    FROM data_vault_mvp_dev_robin.dwh.user_attributes ua
    WHERE ua.membership_account_status IS DISTINCT FROM 'DELETED' --not include any deleted user information
      AND ua.current_affiliate_territory IS DISTINCT FROM 'US'    -- CRM team have instructed us that NO US members should exist in iterable
        EXCEPT
    SELECT ua.shiro_user_id,
           ua.main_affiliate_id
    FROM data_vault_mvp.dwh.user_attributes ua
    WHERE ua.membership_account_status IS DISTINCT FROM 'DELETED' --not include any deleted user information
      AND ua.current_affiliate_territory IS DISTINCT FROM 'US' -- CRM team have instructed us that NO US members should exist in iterable
);