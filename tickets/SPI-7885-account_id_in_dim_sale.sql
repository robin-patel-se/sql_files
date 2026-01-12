WITH
	data AS (
		SELECT
			uc.company_id,
			c.account_id,
			c.name AS company_name,
			sao.channel_manager_used__c,
			sao.connected_to_se__c,
			u.username,
			ur.role_id,
			r.name AS role_name
		FROM se.data_pii.partner_portal_user u
		LEFT JOIN se.data_pii.partner_portal_user_companies uc
			ON u.id = uc.user_id
		LEFT JOIN se.data_pii.partner_portal_company c
			ON uc.company_id = c.id
		LEFT JOIN se.data_pii.partner_portal_user_roles ur
			ON u.id = ur.user_id
		LEFT JOIN se.data_pii.partner_portal_role r
			ON ur.role_id = r.id
		LEFT JOIN se.data_pii.sfsc_account_object sao
			ON c.account_id = LEFT(sao.account_id_18_digit__c, 15)
		WHERE u.active = FALSE
		  AND company_id IS NOT NULL
	),
	active_users AS (
		SELECT
			uc.company_id,
			SUM(CASE WHEN u.active = TRUE THEN 1 ELSE 0 END) AS active_users
		FROM se.data_pii.partner_portal_user u
		LEFT JOIN se.data_pii.partner_portal_user_companies uc
			ON u.id = uc.user_id
		WHERE company_id IS NOT NULL
		GROUP BY 1
	),
	account_info AS
		(
		SELECT
			ds.salesforce_account_id,
			IFF(CONTAINS(LISTAGG(DISTINCT ds.sale_active, '-'), 'true'), 'TRUE', 'FALSE') AS sale_active,
			LISTAGG(DISTINCT ds.current_contractor_name, '-')                             AS current_contractor_name,
			LISTAGG(DISTINCT ds.posu_cluster, '-')                                        AS posu_cluster,
			LISTAGG(DISTINCT ds.posu_cluster_region, '-')                                 AS posu_cluster_region,
			LISTAGG(DISTINCT ds.posu_cluster_sub_region, '-')                             AS posu_cluster_sub_region
		FROM se.data.dim_sale ds
		GROUP BY 1
	)
SELECT
	d.company_id,
	d.account_id,
	d.company_name,
	d.channel_manager_used__c,
	d.connected_to_se__c,
	d.username,
	d.role_id,
	d.role_name,
	ai.sale_active,
	ai.current_contractor_name,
	ai.posu_cluster,
	ai.posu_cluster_region,
	ai.posu_cluster_sub_region
FROM data d
LEFT JOIN active_users au
	ON d.company_id = au.company_id
LEFT JOIN account_info ai
	ON d.account_id = LEFT(ai.salesforce_account_id, 15)
WHERE au.active_users = 0
;


------------------------------------------------------------------------------------------------------------------------
-- module=/biapp/task_catalogue/dv/dwh/transactional/dim_sale.py make clones

USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.chiasma_sale
	CLONE data_vault_mvp.dwh.chiasma_sale
;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.fpa_gsheets
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.fpa_gsheets.posu_categorisation
	CLONE latest_vault.fpa_gsheets.posu_categorisation
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.reactivated_sale_active
	CLONE data_vault_mvp.dwh.reactivated_sale_active
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_sale
	CLONE data_vault_mvp.dwh.se_sale
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tb_offer
	CLONE data_vault_mvp.dwh.tb_offer
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tvl_sale
	CLONE data_vault_mvp.dwh.tvl_sale
;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.dim_sale
	CLONE data_vault_mvp.dwh.dim_sale
;

self_describing_task
\
    --include 'biapp.task_catalogue.dv.dwh.transactional.dim_sale.py' \
    --method 'run' \
    --start '2025-10-14 00:00:00' \
    --end '2025-10-14 00:00:00'

                     ;

SELECT
	ds.tech_platform,
	COUNT(*)
FROM data_vault_mvp_dev_robin.dwh.dim_sale ds
GROUP BY 1
;

SELECT
	ds.tech_platform,
	COUNT(*)
FROM data_vault_mvp.dwh.dim_sale ds
GROUP BY 1
;

SELECT *
FROM data_vault_mvp.dwh.sfsc__opportunity
;

0061r00000zgg72AAA
006Tg00000HKWWb
006travellist95
006w000000cCmrY

001w000001VDtVaAAL
001w000001RWw6fAAD

SELECT *
FROM se.data.dim_sale ds
;


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.sfsc__opportunity CLONE data_vault_mvp.dwh.sfsc__opportunity
;



WITH
	data AS (
		SELECT
			uc.company_id,
			c.account_id,
			c.name AS company_name,
			sao.channel_manager_used__c,
			sao.connected_to_se__c,
			u.username,
			ur.role_id,
			r.name AS role_name
		FROM latest_vault.partner_portal.user u
		LEFT JOIN latest_vault.partner_portal.user_companies uc
			ON u.id = uc.user_id
		LEFT JOIN latest_vault.partner_portal.company c
			ON uc.company_id = c.id
		LEFT JOIN latest_vault.partner_portal.user_roles ur
			ON u.id = ur.user_id
		LEFT JOIN latest_vault.partner_portal.role r
			ON ur.role_id = r.id
		LEFT JOIN se.data_pii.sfsc_account_object sao
			ON c.account_id = LEFT(sao.account_id_18_digit__c, 15)
		WHERE u.active = FALSE
		  AND company_id IS NOT NULL
	),
	active_users AS (
		SELECT
			uc.company_id,
			SUM(CASE WHEN u.active = TRUE THEN 1 ELSE 0 END) AS active_users
		FROM latest_vault.partner_portal.user u
		LEFT JOIN latest_vault.partner_portal.user_companies uc
			ON u.id = uc.user_id
		WHERE company_id IS NOT NULL
		GROUP BY 1
	),
	account_info AS
		(
		SELECT
			ssa.salesforce_account_id,
			IFF(CONTAINS(LISTAGG(DISTINCT ssa.sale_active, '-'), 'true'), 'TRUE', 'FALSE') AS sale_active,
			LISTAGG(DISTINCT ssa.current_contractor_name, '-')                             AS current_contractor_name,
			LISTAGG(DISTINCT ds.posu_cluster, '-')                                         AS posu_cluster,
			LISTAGG(DISTINCT ds.posu_cluster_region, '-')                                  AS posu_cluster_region,
			LISTAGG(DISTINCT ds.posu_cluster_sub_region, '-')                              AS posu_cluster_sub_region
		FROM data_vault_mvp.dwh.se_sale ssa
		LEFT JOIN se.data.dim_sale ds
			ON ds.se_sale_id = ssa.se_sale_id
		GROUP BY 1
	)
SELECT
	d.company_id,
	d.account_id,
	d.company_name,
	d.channel_manager_used__c,
	d.connected_to_se__c,
	d.username,
	d.role_id,
	d.role_name,
	ai.sale_active,
	ai.current_contractor_name,
	ai.posu_cluster,
	ai.posu_cluster_region,
	ai.posu_cluster_sub_region
FROM data d
LEFT JOIN active_users au
	ON d.company_id = au.company_id
LEFT JOIN account_info ai
	ON d.account_id = LEFT(ai.salesforce_account_id, 15)
WHERE au.active_users = 0
;



USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.chiasma_sale
	CLONE data_vault_mvp.dwh.chiasma_sale
;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.fpa_gsheets
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.fpa_gsheets.posu_categorisation
	CLONE latest_vault.fpa_gsheets.posu_categorisation
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.reactivated_sale_active
	CLONE data_vault_mvp.dwh.reactivated_sale_active
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_sale
	CLONE data_vault_mvp.dwh.se_sale
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tb_offer
	CLONE data_vault_mvp.dwh.tb_offer
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tvl_sale
	CLONE data_vault_mvp.dwh.tvl_sale
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.sfsc__opportunity
	CLONE data_vault_mvp.dwh.sfsc__opportunity
;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.dim_sale
	CLONE data_vault_mvp.dwh.dim_sale
;

self_describing_task
\
    --include 'biapp.task_catalogue.dv.dwh.transactional.dim_sale.py' \
    --method 'run' \
    --start '2025-10-14 00:00:00' \
    --end '2025-10-14 00:00:00'



SELECT
	u.id   AS user_id,
	uc.company_id,
	c.account_id,
	c.name AS company_name,
	sao.channel_manager_used__c,
	sao.connected_to_se__c,
	u.username,
	ur.role_id,
	r.name AS role_name
FROM se.data_pii.partner_portal_user u
LEFT JOIN se.data_pii.partner_portal_user_companies uc
	ON u.id = uc.user_id
LEFT JOIN se.data_pii.partner_portal_company c
	ON uc.company_id = c.id
LEFT JOIN se.data_pii.partner_portal_user_roles ur
	ON u.id = ur.user_id
LEFT JOIN se.data_pii.partner_portal_role r
	ON ur.role_id = r.id
LEFT JOIN se.data_pii.sfsc_account_object sao
	ON c.account_id = LEFT(sao.account_id_18_digit__c, 15)
WHERE u.active = FALSE
  AND uc.company_id IS NOT NULL
  AND u.id = 12349;


SELECT * FROM latest_vault.partner_portal.company;