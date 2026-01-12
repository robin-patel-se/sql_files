CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.travelbird_mysql.allocations_allocation CLONE latest_vault.travelbird_mysql.allocations_allocation
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.travelbird_mysql.allocations_allocationboard CLONE latest_vault.travelbird_mysql.allocations_allocationboard
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.travelbird_mysql.allocations_allocationunit CLONE latest_vault.travelbird_mysql.allocations_allocationunit
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.travelbird_mysql.allocations_itemtranslation CLONE latest_vault.travelbird_mysql.allocations_itemtranslation
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.travelbird_mysql.allocations_boardtranslation CLONE latest_vault.travelbird_mysql.allocations_boardtranslation
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.travelbird_mysql.common_sitesettings CLONE latest_vault.travelbird_mysql.common_sitesettings
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.travelbird_mysql.offers_offer CLONE latest_vault.travelbird_mysql.offers_offer
;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.travelbird_mysql_snapshots.packaging_accommodationpackageconfiguration_snapshot AS
SELECT *
FROM data_vault_mvp.travelbird_mysql_snapshots.packaging_accommodationpackageconfiguration_snapshot
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.travelbird_mysql.packaging_accommodationpackageconfiguration_allocation_boards CLONE latest_vault.travelbird_mysql.packaging_accommodationpackageconfiguration_allocation_boards
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.travelbird_mysql.packaging_accommodationpackageconfiguration_allocation_units CLONE latest_vault.travelbird_mysql.packaging_accommodationpackageconfiguration_allocation_units
;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.travelbird_mysql_snapshots.packaging_packageconfiguration_snapshot AS
SELECT *
FROM data_vault_mvp.travelbird_mysql_snapshots.packaging_packageconfiguration_snapshot
;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.travelbird_mysql_snapshots.packaging_accommodationpackageinventoryconfiguration_snapshot AS
SELECT *
FROM data_vault_mvp.travelbird_mysql_snapshots.packaging_accommodationpackageinventoryconfiguration_snapshot
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.travelbird_mysql_snapshots.products_hotelproduct_snapshot CLONE data_vault_mvp.travelbird_mysql_snapshots.products_hotelproduct_snapshot
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.travelbird_mysql_snapshots.products_hotelproducttype_snapshot CLONE data_vault_mvp.travelbird_mysql_snapshots.products_hotelproducttype_snapshot
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.travelbird_mysql.products_hotelproperty CLONE latest_vault.travelbird_mysql.products_hotelproperty
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.travelbird_mysql.contracts_producttermlink CLONE latest_vault.travelbird_mysql.contracts_producttermlink
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.travelbird_mysql.contracts_productterms CLONE latest_vault.travelbird_mysql.contracts_productterms
;

self_describing_task --include 'biapp/task_catalogue/dv/dwh/sale/packages/package_component_accommodation.py'  --method 'run' --start '2024-02-11 00:00:00' --end '2024-02-11 00:00:00'



SELECT DISTINCT
	concept.internal_name AS concept_name,
	offer.id              AS offer_id,
	t.se_sale_id,
	offer.internal_name   AS offer_name,
	allocation.name       AS allocation_name,
	offer.product_line,
-- 	block.package_name,
	partner.company,
	terms.partner_cancellation_terms
FROM latest_vault.travelbird_mysql.offers_offer offer
	LEFT JOIN hygiene_snapshot_vault_mvp.travelbird_mysql.offers_offerconcept concept ON concept.id = offer.concept_id
	LEFT JOIN hygiene_snapshot_vault_mvp.travelbird_mysql.offers_hotelproductlink hpl ON hpl.offer_id = offer.id
	LEFT JOIN latest_vault.travelbird_mysql.allocations_allocation allocation ON allocation.id = hpl.allocation_id
	LEFT JOIN latest_vault.travelbird_mysql.allocations_chargeblock block ON block.allocation_id = allocation.id
	LEFT JOIN latest_vault.travelbird_mysql.contracts_producttermlink link ON link.product_id = block.id
	AND link.product_type_id = 67 -- allocations_chargeblock type id
	LEFT JOIN latest_vault.travelbird_mysql.contracts_productterms terms ON terms.id = link.product_terms_id
	LEFT JOIN hygiene_snapshot_vault_mvp.travelbird_mysql.partners_partner partner ON partner.id = allocation.partner_id
	LEFT JOIN se.data.tb_offer t ON offer.id = t.tb_offer_id
WHERE offer.active = 1
  AND offer.pub_date < CURRENT_DATE
  AND offer.site_id != 46-- # EXCLUDE travelist
  AND offer.product_line = 'catalogue'
;


SELECT *
FROM latest_vault.travelbird_mysql.allocations_chargeblock ac
;

SELECT *
FROM hygiene_snapshot_vault_mvp.travelbird_mysql.offers_hotelproductlink
;


WITH input_data AS (
	SELECT DISTINCT
		concept.internal_name AS concept_name,
		offer.id              AS offer_id,
		t.se_sale_id,
		offer.internal_name   AS offer_name,
		allocation.id,
		allocation.name       AS allocation_name,
		offer.product_line,
		block.id,
		block.allocation_id,
-- 	block.package_name,
		partner.company,
		terms.partner_cancellation_terms
	FROM latest_vault.travelbird_mysql.offers_offer offer
		LEFT JOIN hygiene_snapshot_vault_mvp.travelbird_mysql.offers_offerconcept concept
				  ON concept.id = offer.concept_id
		LEFT JOIN hygiene_snapshot_vault_mvp.travelbird_mysql.offers_hotelproductlink hpl ON hpl.offer_id = offer.id
		LEFT JOIN latest_vault.travelbird_mysql.allocations_allocation allocation ON allocation.id = hpl.allocation_id
		LEFT JOIN latest_vault.travelbird_mysql.allocations_chargeblock block ON block.allocation_id = allocation.id
		LEFT JOIN latest_vault.travelbird_mysql.contracts_producttermlink link ON link.product_id = block.id
		AND link.product_type_id = 67 -- allocations_chargeblock type id
		LEFT JOIN latest_vault.travelbird_mysql.contracts_productterms terms ON terms.id = link.product_terms_id
		LEFT JOIN hygiene_snapshot_vault_mvp.travelbird_mysql.partners_partner partner
				  ON partner.id = allocation.partner_id
		LEFT JOIN se.data.tb_offer t ON offer.id = t.tb_offer_id
	WHERE offer.active = 1
	  AND offer.pub_date < CURRENT_DATE
	  AND offer.site_id != 46-- # EXCLUDE travelist
	  AND offer.product_line = 'catalogue'
)
SELECT * FROM input_data WHERE input_data.offer_id = 118736;

SELECT * FROM latest_vault.travelbird_mysql.allocations_chargeblock WHERE allocation_id = 61;

SELECT * FROM latest_vault.travelbird_mysql.allocations_chargeblock WHERE id = 78;


./scripts/mwaa-cli production "dags backfill --start-date '2020-01-10 00:00:00' --end-date '2020-01-02 00:00:00' incoming__travelbird_mysql__allocations_chargeblock__daily_at_00h30”
./scripts/mwaa-cli production 'dags backfill --mark-success --start-date "2018-01-01 03:00:00" --end-date "2018-01-01 03:00:00" incoming__travelbird_mysql__allocations_chargeblock__daily_at_00h30'

