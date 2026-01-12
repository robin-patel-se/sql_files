SELECT sa.se_sale_id,
       MIN(sa.sale_start_date) AS min_start,
       MIN(sa.view_date)       AS min_view
FROM data_vault_mvp.dwh.sale_active sa
WHERE sa.tech_platform = 'SECRET_ESCAPES'
  AND sa.sale_start_date >= '2020-06-18'
GROUP BY 1
HAVING min_start != min_view
;


SELECT sa.schedule_tstamp,
       sa.run_tstamp,
       sa.operation_id,
       sa.created_at,
       sa.updated_at,
       sa.sale_start_date AS view_date,
       sa.se_sale_id,
       sa.sale_active,
       sa.sale_id,
       sa.base_sale_id,
       sa.tb_offer_id,
       sa.sale_start_date,
       sa.sale_end_date,
       sa.active,
       sa.tech_platform
FROM data_vault_mvp.dwh.sale_active sa
WHERE sa.tech_platform = 'SECRET_ESCAPES'
  AND sa.sale_start_date >= '2020-06-18'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY sa.se_sale_id ORDER BY sa.sale_start_date) = 1;

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.dwh.sale_active CLONE data_vault_mvp.dwh.sale_active;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.dwh.sale_active_bkup CLONE data_vault_mvp.dwh.sale_active;

INSERT INTO data_vault_mvp_dev_robin.dwh.sale_active
SELECT CURRENT_TIMESTAMP,
       CURRENT_TIMESTAMP,
       'hotfix bug for missing start date',
       CURRENT_TIMESTAMP,
       CURRENT_TIMESTAMP,
       sa.sale_start_date AS view_date,
       sa.se_sale_id,
       sa.sale_active,
       sa.sale_id,
       sa.base_sale_id,
       sa.tb_offer_id,
       sa.sale_start_date,
       sa.sale_end_date,
       sa.active,
       sa.tech_platform
FROM data_vault_mvp.dwh.sale_active sa
WHERE sa.tech_platform = 'SECRET_ESCAPES'
  AND sa.sale_start_date >= '2020-06-18'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY sa.se_sale_id ORDER BY sa.sale_start_date) = 1;


SELECT sa.se_sale_id,
       MIN(sa.sale_start_date) AS min_start,
       MIN(sa.view_date)       AS min_view
FROM data_vault_mvp_dev_robin.dwh.sale_active sa
WHERE sa.tech_platform = 'SECRET_ESCAPES'
  AND sa.sale_start_date >= '2020-06-18'
GROUP BY 1
HAVING min_start != min_view
;

SELECT sa.se_sale_id,
       sa.view_date,
       count(*)
FROM data_vault_mvp.dwh.sale_active sa
GROUP BY 1, 2
HAVING count(*) > 1;


