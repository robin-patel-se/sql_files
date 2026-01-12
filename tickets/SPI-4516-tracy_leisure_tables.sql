python3 biapp/
bau/
manifests/
generate_manifest_from_sql_table.py
    --connector 'travelbird_mysql' /
    --table_names 'leisures_leisureunittranslation' 'leisures_leisuredescription' /
    --mode 'regenerative' /
    --start_date '2024-01-07 00:00:00'


PYTHON biapp/
bau/
manifests/
generate_manifest_from_sql_table.py
\
    --connector 'travelbird_mysql' \
    --table_names 'leisures_leisureunittranslation' 'leisures_leisuredescription' \
    --mode 'regenerative' \
    --start_date '2024-01-07 00:00:00'

CREATE TABLE travelbird.leisures_leisuredescription
(
	id                     int auto_increment
		LANGUAGE VARCHAR (7) NOT NULL,
	leisure_name           varchar(255) NOT NULL,
	description            longtext     NOT NULL,
	leisure_id             int          NOT NULL,
	instructions           longtext     NOT NULL,
	additional_information longtext     NOT NULL,
	CONSTRAINT leisures_leisuredescription_leisure_id_d19002a9_uniq
		UNIQUE (leisure_id, language),
	CONSTRAINT leisures_leisuredescr_leisure_id_cc704b59_fk_leisures_leisure_id
		FOREIGN KEY (leisure_id) REFERENCES travelbird.leisures_leisure (id)
)


CREATE TABLE travelbird.leisures_leisureunittranslation
(
	id              int auto_increment
		LANGUAGE VARCHAR (7) NOT NULL,
	translation     varchar(255) NOT NULL,
	leisure_id      int          NOT NULL,
	leisure_unit_id int          NOT NULL,
	CONSTRAINT leisures_leisureunittranslation_leisure_unit_id_3d8f16fd_uniq
		UNIQUE (leisure_unit_id, language),
	CONSTRAINT leisures_lei_leisure_unit_id_c9f3279b_fk_leisures_leisureunit_id
		FOREIGN KEY (leisure_unit_id) REFERENCES travelbird.leisures_leisureunit (id),
	CONSTRAINT leisures_leisureunitt_leisure_id_b428becf_fk_leisures_leisure_id
		FOREIGN KEY (leisure_id) REFERENCES travelbird.leisures_leisure (id)
) dataset_task --include 'travelbird_mysql.leisures_leisuredescription' --operation LatestRecordsOperation --method 'run' --upstream --start '2024-01-07 00:30:00' --end '2024-01-07 00:30:00'
dataset_task --include 'travelbird_mysql.leisures_leisureunittranslation' --operation LatestRecordsOperation --method 'run' --upstream --start '2024-01-07 00:30:00' --end '2024-01-07 00:30:00'

SELECT *
FROM latest_vault_dev_robin.travelbird_mysql.leisures_leisuredescription
;

SELECT *
FROM latest_vault_dev_robin.travelbird_mysql.leisures_leisureunittranslation
;



SELECT
	offer.id                           AS offer_id,
	offer.internal_name                AS internal_name,
	trans.language                     AS language,
	leisure.name                       AS leisure,
	unit.name                          AS unit,
	trans.translation                  AS translation,
	description.additional_information AS additional_information,
	description.instructions           AS instructions,
	description.description            AS short_description,
	t.current_contractor_name
FROM latest_vault.travelbird_mysql.offers_offer offer
	LEFT JOIN latest_vault.travelbird_mysql.offers_leisureproductlink lpl ON lpl.offer_id = offer.id
	LEFT JOIN latest_vault.travelbird_mysql.leisures_leisure leisure ON leisure.id = lpl.leisure_id
	LEFT JOIN latest_vault.travelbird_mysql.leisures_leisureunit unit ON unit.leisure_id = leisure.id
	LEFT JOIN latest_vault.travelbird_mysql.leisures_leisureunittranslation trans
			  ON trans.leisure_unit_id = unit.id
	LEFT JOIN latest_vault.travelbird_mysql.leisures_leisuredescription description
			  ON description.leisure_id = leisure.id
	LEFT JOIN se.data.tb_offer t ON offer.id = t.tb_offer_id
WHERE offer.site_id != 46
  AND offer.active = 1
  AND lpl.id IS NOT NULL
;

USE ROLE pipelinerunner
;

CREATE SCHEMA collab.risk
;

GRANT USAGE ON SCHEMA collab.risk TO ROLE data_team_basic
;

GRANT USAGE ON SCHEMA collab.risk TO ROLE personal_role__shiannestannard
;

GRANT SELECT ON ALL TABLES IN SCHEMA collab.risk TO ROLE personal_role__shiannestannard
;

GRANT SELECT ON ALL VIEWS IN SCHEMA collab.risk TO ROLE personal_role__shiannestannard
;

CREATE OR REPLACE VIEW collab.risk.package_add_on_details COPY GRANTS AS
SELECT
	offer.id                           AS offer_id,
	offer.internal_name                AS internal_name,
	trans.language                     AS language,
	leisure.name                       AS leisure,
	unit.name                          AS unit,
	trans.translation                  AS translation,
	description.additional_information AS additional_information,
	description.instructions           AS instructions,
	description.description            AS short_description,
	t.current_contractor_name
FROM latest_vault.travelbird_mysql.offers_offer offer
	LEFT JOIN latest_vault.travelbird_mysql.offers_leisureproductlink lpl ON lpl.offer_id = offer.id
	LEFT JOIN latest_vault.travelbird_mysql.leisures_leisure leisure ON leisure.id = lpl.leisure_id
	LEFT JOIN latest_vault.travelbird_mysql.leisures_leisureunit unit ON unit.leisure_id = leisure.id
	LEFT JOIN latest_vault.travelbird_mysql.leisures_leisureunittranslation trans
			  ON trans.leisure_unit_id = unit.id
	LEFT JOIN latest_vault.travelbird_mysql.leisures_leisuredescription description
			  ON description.leisure_id = leisure.id
	LEFT JOIN se.data.tb_offer t ON offer.id = t.tb_offer_id
WHERE offer.site_id != 46
  AND offer.active = 1
  AND lpl.id IS NOT NULL
;

GRANT SELECT ON TABLE collab.risk.package_add_on_details TO ROLE data_team_basic;


SELECT * FROM collab.risk.package_add_on_details;