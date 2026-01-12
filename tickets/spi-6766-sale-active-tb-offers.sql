SELECT
	tos.view_date,
	tos.se_sale_id,
	tos.sale_active
FROM data_vault_mvp.dwh.tb_offer_snapshot tos
WHERE tos.se_sale_id IS NOT NULL
;

SELECT * FROm se.data.sale_active sa



