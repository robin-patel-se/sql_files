CREATE OR REPLACE SCHEMA data_vault_mvp_dev_robin.cms_mysql_snapshots CLONE data_vault_mvp.cms_mysql_snapshots;
GRANT OWNERSHIP ON ALL TABLES IN SCHEMA data_vault_mvp_dev_robin.cms_mysql_snapshots TO ROLE personal_role__robinpatel; --run in prod

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.base_sale_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.base_sale_snapshot;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.hotel_sale_offer_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.hotel_sale_offer_snapshot;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.base_offer_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.base_offer_snapshot;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.base_offer_product_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.base_offer_product_snapshot;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.product_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.product_snapshot;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.hotel_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.hotel_snapshot;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.product_provider_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.product_provider_snapshot;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.hotel_rate_plan_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.hotel_rate_plan_snapshot;

DROP SCHEMA data_vault_mvp_dev_robin.cms_mysql_snapshots;
CREATE SCHEMA data_vault_mvp_dev_robin.cms_mysql_snapshots;



ALTER TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.base_sale_snapshot
    ADD CONSTRAINT pk_1 PRIMARY KEY (id);

ALTER TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.base_offer_snapshot
    ADD CONSTRAINT pk_1 PRIMARY KEY (id);

ALTER TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.base_offer_product_snapshot
    ADD CONSTRAINT pk_1 PRIMARY KEY (base_offer_products_id);

ALTER TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.product_snapshot
    ADD CONSTRAINT pk_1 PRIMARY KEY (id);

ALTER TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.hotel_snapshot
    ADD CONSTRAINT pk_1 PRIMARY KEY (id);

ALTER TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.product_provider_snapshot
    ADD CONSTRAINT pk_1 PRIMARY KEY (id);

ALTER TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.hotel_rate_plan_snapshot
    ADD CONSTRAINT pk_1 PRIMARY KEY (id);

ALTER TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.hotel_sale_offer_snapshot
    ADD CONSTRAINT pk_1 PRIMARY KEY (id);
ALTER TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.hotel_sale_offer_snapshot
    ADD CONSTRAINT fk_1 FOREIGN KEY (hotel_sale_id) REFERENCES data_vault_mvp_dev_robin.cms_mysql_snapshots.base_sale_snapshot (id);
ALTER TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.hotel_sale_offer_snapshot
    ADD CONSTRAINT fk_2 FOREIGN KEY (hotel_offer_id) REFERENCES data_vault_mvp_dev_robin.cms_mysql_snapshots.base_offer_snapshot (id);
ALTER TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.hotel_sale_offer_snapshot
    ADD CONSTRAINT fk_3 FOREIGN KEY (hotel_offer_id) REFERENCES data_vault_mvp_dev_robin.cms_mysql_snapshots.base_offer_product_snapshot (base_offer_products_id);

ALTER TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.base_offer_product_snapshot
    ADD CONSTRAINT fk_1 FOREIGN KEY (product_id) REFERENCES data_vault_mvp_dev_robin.cms_mysql_snapshots.product_snapshot (id);

ALTER TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.product_snapshot
    ADD CONSTRAINT fk_1 FOREIGN KEY (hotel_id) REFERENCES data_vault_mvp_dev_robin.cms_mysql_snapshots.hotel_snapshot (id);
ALTER TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.product_snapshot
    ADD CONSTRAINT fk_2 FOREIGN KEY (product_provider_id) REFERENCES data_vault_mvp_dev_robin.cms_mysql_snapshots.product_provider_snapshot (id);


ALTER TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.hotel_rate_plan_snapshot
    ADD CONSTRAINT fk_1 FOREIGN KEY (hotel_product_id) REFERENCES data_vault_mvp_dev_robin.cms_mysql_snapshots.product_snapshot (id);