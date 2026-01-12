SELECT c.id,
       COUNT(DISTINCT c.currency)
FROM raw_vault_mvp.cms_mysql.credit c
GROUP BY 1
ORDER BY 2 DESC;


SELECT COUNT(*)
FROM raw_vault_mvp.cms_mysql.credit c;
SELECT *
FROM hygiene_snapshot_vault_mvp.cms_mysql.credit c;


SELECT *
FROM se.data.se_credit sc;

SELECT *
FROM data_vault_mvp.dwh.object_relationships o;

SELECT *,
       REGEXP_SUBSTR(sc.credit_reason, '.*\\[Actioned by: (.*)\\]', 1, 1, 'e') AS actioned_by
FROM se.data_pii.se_credit sc
WHERE sc.credit_reason LIKE 'Converted after territory change:%'
  AND sc.credit_status = 'ACTIVE';


SELECT sc.shiro_user_id,
       COUNT(*)                                                                                                                      AS total_credits,
       SUM(IFF(sc.credit_status = 'ACTIVE', 1, 0))                                                                                   AS active_credits,
       SUM(IFF(sc.credit_status = 'ACTIVE', sc.credit_amount_gbp, 0))                                                                AS active_credit_amount_gbp,
       SUM(IFF(sc.credit_reason LIKE 'Converted after territory change:%', 1, 0))                                                    AS converted_credits,
       SUM(IFF(sc.credit_reason LIKE 'Converted after territory change:%' AND sc.credit_status = 'ACTIVE', sc.credit_amount_gbp, 0)) AS active_converted_credit_amount_gbp,
       COUNT(DISTINCT sc.credit_currency)                                                                                            AS credit_currencies
FROM se.data_pii.se_credit sc
    INNER JOIN se.data.se_user_attributes sua ON sc.shiro_user_id = sua.shiro_user_id
WHERE sua.membership_account_status = 'FULL_ACCOUNT'
GROUP BY 1
HAVING active_credits > 1
ORDER BY 6 DESC;

SELECT *
FROM se.data.se_user_attributes sua
WHERE sua.shiro_user_id = 76736572;



SELECT *
FROM se.finance.se_credit sc
    INNER JOIN se.data.se_user_attributes sua ON sc.shiro_user_id = sua.shiro_user_id
WHERE sua.membership_account_status = 'FULL_ACCOUNT'
  AND sc.credit_id = 13417980


SELECT *
FROM se.finance.se_credit sc
    INNER JOIN se.data.se_user_attributes sua ON sc.shiro_user_id = sua.shiro_user_id
WHERE sua.membership_account_status = 'FULL_ACCOUNT'
  AND sc.credit_id = 13417980