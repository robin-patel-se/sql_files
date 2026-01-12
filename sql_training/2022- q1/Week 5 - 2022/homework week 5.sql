SELECT sua.member_original_affiliate_classification,
       COUNT(DISTINCT sc.credit_id) AS credits,
       SUM(sc.credit_amount_gbp) AS active_credit
FROM se.data.se_user_attributes sua
    INNER JOIN se.data.se_credit sc ON sua.shiro_user_id = sc.shiro_user_id
WHERE sc.credit_status = 'ACTIVE'
AND sc.credit_type IN ('REFUND', 'VOUCHER_CREDIT')
AND sua.membership_account_status = 'FULL_ACCOUNT'
GROUP BY 1;
