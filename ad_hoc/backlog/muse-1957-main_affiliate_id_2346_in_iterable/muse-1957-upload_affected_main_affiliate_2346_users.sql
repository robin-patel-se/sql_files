--upload cube export from chiasma sql server to collab space
USE WAREHOUSE pipe_large;
USE SCHEMA collab.muse_data_modelling;


CREATE OR REPLACE TABLE archive.iterable.affiliate_user_id_2346_users
(
    email             VARCHAR,
    userid_null       VARCHAR,
    userid            VARCHAR,
    affiliateid       NUMBER,
    main_affiliate_id NUMBER,
    user_id           VARCHAR
);


USE SCHEMA archive.iterable;

PUT file:///Users/robin/myrepos/sql_files/backlog/muse-1957-main_affiliate_id_2346_in_iterable/users_with_main_affiliate_id_2346.csv @%affiliate_user_id_2346_users;

COPY INTO archive.iterable.affiliate_user_id_2346_users
    FILE_FORMAT = (
        TYPE = CSV
            FIELD_DELIMITER = ','
            SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
            RECORD_DELIMITER = '\\n'
        );

GRANT SELECT ON TABLE archive.iterable.affiliate_user_id_2346_users TO ROLE data_team_basic;


SELECT *
FROM archive.iterable.affiliate_user_id_2346_users;

ALTER TABLE archive.iterable.affiliate_user_id_2346_users
    DROP COLUMN user_id;

ALTER TABLE archive.iterable.affiliate_user_id_2346_users
    DROP COLUMN userid_null;

SELECT u.userid,
       24 AS mainaffiliateid
FROM archive.iterable.affiliate_user_id_2346_users u;



SELECT *
FROM unload_vault_mvp_dev_robin.iterable.user_profile_activity__20220331t030000__daily_at_03h00__step01__get_source_batch;
SELECT *
FROM unload_vault_mvp_dev_robin.iterable.user_profile_activity__20220331t030000__daily_at_03h00__step02__construct_json;

SELECT *
FROM unload_vault_mvp_dev_robin.iterable.user_profile_activity__20220331t030000__daily_at_03h00;

SELECT *
FROM archive.iterable.affiliate_user_id_2346_users
WHERE userid = 53778390;

SELECT * FROM unload_vault_mvp_dev_robin.iterable.user_profile_activity__20220403T030000__daily_at_03h00;

