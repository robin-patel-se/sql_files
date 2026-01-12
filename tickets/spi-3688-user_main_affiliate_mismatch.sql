WITH input_data AS (
    SELECT
        sus.view_date,
        sus.shiro_user_id,
        sus.original_affiliate_id,
        sus.affiliate_id,
        sus.territory_name,
        LAG(sus.territory_name) OVER (PARTITION BY sus.shiro_user_id ORDER BY sus.view_date) AS last_territory_name
    FROM data_vault_mvp.dwh.shiro_user_snapshot sus
    WHERE DATE_TRUNC(MONTH, sus.view_date) = '2023-02-01'
       OR sus.view_date = '2023-01-31' -- to get the entry before beginning of month
),
     changes AS (
         SELECT *
         FROM input_data ind
         WHERE ind.territory_name != ind.last_territory_name
           AND DATE_TRUNC(MONTH, ind.view_date) = '2023-02-01'
     )
SELECT *
-- COUNT(DISTINCT c.shiro_user_id)
FROM changes c
;
USE WAREHOUSE pipe_xlarge;


SELECT
    COUNT(DISTINCT shiro_user_id)
FROM se.data.se_user_attributes sua
WHERE sua.current_affiliate_territory != original_affiliate_territory;

1,313,260