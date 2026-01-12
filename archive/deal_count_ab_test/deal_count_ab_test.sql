/*
Download csv list from the global_id_exclusion_list tab in https://docs.google.com/spreadsheets/d/1nmGrzRgdKT9XtduzZNCadKIYUZmPNUQvQ19jcAl77gA/edit#gid=1372099804
Create table
Stage the file
Ingest stage
Clone to Collab
*/
USE ROLE personal_role__robinpatel;

CREATE OR REPLACE TABLE scratch.robinpatel.deal_count_ab_test_exlusion_global_sale_ids
(
    global_sale_id VARCHAR
)
;



USE SCHEMA scratch.robinpatel;

PUT 'file:///Users/robin/myrepos/sql_files/deal_count_ab_test/Bottom 50% A_B 05.10 - global_id_exclusion_list.csv' @%deal_count_ab_test_exlusion_global_sale_ids;

COPY INTO scratch.robinpatel.deal_count_ab_test_exlusion_global_sale_ids
    FILE_FORMAT = (
        TYPE = CSV
            FIELD_DELIMITER = ','
            SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
            RECORD_DELIMITER = '\\n'
        );

SELECT *
FROM scratch.robinpatel.deal_count_ab_test_exlusion_global_sale_ids;


CREATE OR REPLACE TABLE collab.ab_testing.deal_count_ab_test_exlusion_global_sale_ids COPY GRANTS CLONE scratch.robinpatel.deal_count_ab_test_exlusion_global_sale_ids;


SELECT gs.global_sale_id,
       ds.se_sale_id,
       ds.posa_territory_id
FROM collab.ab_testing.deal_count_ab_test_exlusion_global_sale_ids gs
    INNER JOIN data_vault_mvp.dwh.dim_sale ds ON gs.global_sale_id = ds.salesforce_opportunity_id AND ds.sale_active
;

USE ROLE personal_role__robinpatel;
USE WAREHOUSE pipe_default;
SELECT *
FROM dbt.bi_data_science__intermediate.ds_ab_dc_excluded_sale_ids;

USE ROLE personal_role__dbt_prod;
SELECT * FROM collab.ab_testing.deal_count_ab_test_exlusion_global_sale_ids dcategsi;

SELECT * FROM dbt.bi_staging.base_collab;


SHOW TABLES IN SCHEMA data_vault_mvp.bi;