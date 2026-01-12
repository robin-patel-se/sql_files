------------------------------------------------------------------------------------------------------------------------
--see what views have been created,
SELECT *
FROM snowflake.account_usage.views
-- WHERE view_definition LIKE ('%WORLDPAY_TRANSACTION_SUMMARY_V%');


SELECT
    REGEXP_REPLACE(qh.query_text, '\\\'', ''),
    *
FROM snowflake.account_usage.query_history qh
WHERE qh.user_name NOT IN ('PIPELINERUNNER', 'DATASCIENCERUNNER', 'TABLEAU', 'SNOWPLOW')
  AND qh.user_name = 'ROBINPATEL'
  AND qh.query_type = 'SELECT'
  AND qh.start_time >= '2020-03-01'
  AND REGEXP_REPLACE(qh.query_text, '\\\'', '') NOT IN
      ('SELECT CAST(test unicode returns AS VARCHAR(60)) AS anon_1',
       'SELECT CAST(test plain returns AS VARCHAR(60)) AS anon_1',
       'select current_database() as a, current_schema() as b',
       'SELECT keep alive',
       'SELECT ?',
       'SELECT SYSTEM$BOOTSTRAP_DATA_REQUEST(?) AS BOOTSTRAP_DATA;',
       'SELECT CURRENT_AVAILABLE_ROLES() AS "ROLES";',
       '"SELECT SYSTEM$CURRENT_ACCOUNT_HAS_GRANTS(?, ?, ?, ?) AS AVAILABLEGRANTS;"',
       '"SELECT SYSTEM$BOOTSTRAP_DATA_REQUEST(?, ?) AS BOOTSTRAP_DATA;"',
       'select system$list_outbound_shares_details();',
       '"SELECT system$GET_NPS_FEEDBACK_TIMESTAMP() as feedback_timestamp, system$GET_NPS_DISMISS_TIMESTAMP() as dismiss_timestamp;"',
       '"SELECT ALL_USER_NAMES('') AS USERS;"',
       '"SELECT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS ""COL"""'
          )



SELECT
    DATE_TRUNC(MONTH, qh.start_time) AS month,
    qh.user_name,
    COUNT(1)                         AS select_queries
FROM snowflake.account_usage.query_history qh
WHERE qh.user_name NOT IN ('PIPELINERUNNER', 'DATASCIENCERUNNER', 'TABLEAU', 'SNOWPLOW')
  AND qh.query_type = 'SELECT'
  AND qh.start_time >= '2020-03-01'
  AND REGEXP_REPLACE(qh.query_text, '\\\'', '') NOT IN
      ('SELECT CAST(test unicode returns AS VARCHAR(60)) AS anon_1',
       'SELECT CAST(test plain returns AS VARCHAR(60)) AS anon_1',
       'select current_database() as a, current_schema() as b',
       'SELECT keep alive',
       'SELECT ?',
       'SELECT SYSTEM$BOOTSTRAP_DATA_REQUEST(?) AS BOOTSTRAP_DATA;',
       'SELECT CURRENT_AVAILABLE_ROLES() AS "ROLES";',
       '"SELECT SYSTEM$CURRENT_ACCOUNT_HAS_GRANTS(?, ?, ?, ?) AS AVAILABLEGRANTS;"',
       '"SELECT SYSTEM$BOOTSTRAP_DATA_REQUEST(?, ?) AS BOOTSTRAP_DATA;"',
       'select system$list_outbound_shares_details();',
       '"SELECT system$GET_NPS_FEEDBACK_TIMESTAMP() as feedback_timestamp, system$GET_NPS_DISMISS_TIMESTAMP() as dismiss_timestamp;"',
       '"SELECT ALL_USER_NAMES('') AS USERS;"',
       '"SELECT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS ""COL"""'
          )
GROUP BY 1, 2;



SELECT
    qh.query_text,
    COUNT(1) AS select_queries

FROM snowflake.account_usage.query_history qh
WHERE qh.user_name = 'ROBINPATEL'
  AND qh.query_type = 'SELECT'
  AND qh.start_time >= '2020-03-01'
  AND REGEXP_REPLACE(qh.query_text, '\\\'', '') NOT IN
      ('SELECT CAST(test unicode returns AS VARCHAR(60)) AS anon_1',
       'SELECT CAST(test plain returns AS VARCHAR(60)) AS anon_1',
       'select current_database() as a, current_schema() as b',
       'SELECT keep alive',
       'SELECT ?',
       'SELECT SYSTEM$BOOTSTRAP_DATA_REQUEST(?) AS BOOTSTRAP_DATA;',
       'SELECT CURRENT_AVAILABLE_ROLES() AS "ROLES";'
          )
GROUP BY 1
ORDER BY 2 DESC;

USE WAREHOUSE pipe_xlarge;

SELECT *
FROM snowflake.account_usage.query_history qh
WHERE LOWER(qh.query_text) LIKE '%se.data.se_country%'
  AND qh.user_name NOT IN ('PIPELINERUNNER', 'DATASCIENCERUNNER', 'TABLEAU', 'SNOWPLOW')
  AND qh.start_time >= '2020-07-01';

SELECT *
FROM se.data.se_sale_attributes ssa
WHERE ssa.se_sale_id = '108340'


SELECT *
FROM data_vault_mvp.snowflake_uac.user_roles;


SELECT *
FROM snowflake.information_schema.tables t;


SELECT *
FROM snowflake.account_usage.database_storage_usage_history dsuh
WHERE dsuh.usage_date = CURRENT_DATE - 1
ORDER BY average_database_bytes DESC;

SELECT
    dsuh.usage_date,
    dsuh.database_id,
    dsuh.database_name,
    dsuh.deleted,
    dsuh.average_database_bytes,
    dsuh.average_failsafe_bytes,
    dsuh.average_database_bytes / POW(1024, 4) AS average_database_tb
FROM snowflake.account_usage.database_storage_usage_history dsuh
WHERE dsuh.usage_date >= CURRENT_DATE - 30
  AND dsuh.database_name = 'DATA_VAULT_MVP'
ORDER BY usage_date DESC;


SELECT *
FROM snowflake.account_usage.table_storage_metrics tsm
ORDER BY active_bytes + time_travel_bytes + retained_for_clone_bytes + failsafe_bytes DESC;


SELECT *
FROM snowflake.account_usage.query_history qh
------------------------------------------------------------------------------------------------------------------------
SET table_name = 'raw_vault.sfsc.account__a_l';

--how many times the table was used
SELECT
    role_name,
    COUNT(*) AS table_queries
FROM snowflake.account_usage.query_history
WHERE LOWER(query_text) LIKE ('% ' || $table_name || ' %')
  AND role_name IS DISTINCT FROM 'PIPELINERUNNER'
  AND start_time::DATE >= CURRENT_DATE() - 90
GROUP BY 1;

-- who used the table
WITH data AS (
    SELECT *
    FROM snowflake.account_usage.query_history
    WHERE LOWER(query_text) LIKE ('% ' || $table_name || ' %')
      AND role_name IS DISTINCT FROM 'PIPELINERUNNER'
      AND start_time::DATE >= CURRENT_DATE() - 90
)
SELECT
    start_time::date,
    COUNT(*)                      AS queries,
    LISTAGG(DISTINCT (role_name)) AS who
FROM data
GROUP BY 1
ORDER BY 1 DESC;


------------------------------------------------------------------------------------------------------------------------
--check usage on a group of tables

;

USE ROLE pipelinerunner;

CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.table_usage
(
    table_source  VARCHAR,
    role_name     VARCHAR,
    table_queries NUMBER
)
;


arrayOfTables = [
    'raw_vault.sfsc.account__a_l',
    'raw_vault.sfsc.account__m_z',
    'raw_vault.sfsc.inclusion',
    'raw_vault.sfsc.offers',
    'raw_vault.sfsc.opportunity__a_l',
    'raw_vault.sfsc.opportunity__m_z',
    'raw_vault.sfsc.rebooking_request_cases',
    'raw_vault.sfsc.salesforce_cases',
    'hygiene_vault_mvp.sfsc.account',
    'hygiene_vault_mvp.sfsc.inclusion',
    'hygiene_vault_mvp.sfsc.offers',
    'hygiene_vault_mvp.sfsc.opportunity',
    'hygiene_vault_mvp.sfsc.rebooking_request_cases',
    'hygiene_vault_mvp.sfsc.salesforce_cases',
    'hygiene_snapshot_vault_mvp.sfsc.account',
    'hygiene_snapshot_vault_mvp.sfsc.inclusion',
    'hygiene_snapshot_vault_mvp.sfsc.offers',
    'hygiene_snapshot_vault_mvp.sfsc.opportunity',
    'hygiene_snapshot_vault_mvp.sfsc.rebooking_request_cases'
    ];
/*
var arrayOfTables = [
    'raw_vault.sfsc.account__a_l',    'raw_vault.sfsc.account__m_z',    'raw_vault.sfsc.inclusion',    'raw_vault.sfsc.offers',    'raw_vault.sfsc.opportunity__a_l',    'raw_vault.sfsc.opportunity__m_z',    'raw_vault.sfsc.rebooking_request_cases',    'raw_vault.sfsc.salesforce_cases',    'hygiene_vault_mvp.sfsc.account',    'hygiene_vault_mvp.sfsc.inclusion',    'hygiene_vault_mvp.sfsc.offers',    'hygiene_vault_mvp.sfsc.opportunity',    'hygiene_vault_mvp.sfsc.rebooking_request_cases',    'hygiene_vault_mvp.sfsc.salesforce_cases',    'hygiene_snapshot_vault_mvp.sfsc.account',    'hygiene_snapshot_vault_mvp.sfsc.inclusion',    'hygiene_snapshot_vault_mvp.sfsc.offers',    'hygiene_snapshot_vault_mvp.sfsc.opportunity',    'hygiene_snapshot_vault_mvp.sfsc.rebooking_request_cases'
    ];
 */

--working version
CREATE OR REPLACE PROCEDURE scratch.robinpatel.table_users(
                                                          )
    RETURNS VARCHAR
    LANGUAGE JAVASCRIPT
    RETURNS NULL ON NULL INPUT
AS
$$
var arrayOfTables = [
    'raw_vault.sfsc.account__a_l',
    'raw_vault.sfsc.account__m_z',
    'raw_vault.sfsc.inclusion',
    'raw_vault.sfsc.offers',
    'raw_vault.sfsc.opportunity__a_l',
    'raw_vault.sfsc.opportunity__m_z',
    'raw_vault.sfsc.rebooking_request_cases',
    'raw_vault.sfsc.salesforce_cases',
    'hygiene_vault_mvp.sfsc.account',
    'hygiene_vault_mvp.sfsc.inclusion',
    'hygiene_vault_mvp.sfsc.offers',
    'hygiene_vault_mvp.sfsc.opportunity',
    'hygiene_vault_mvp.sfsc.rebooking_request_cases',
    'hygiene_vault_mvp.sfsc.salesforce_cases',
    'hygiene_snapshot_vault_mvp.sfsc.account',
    'hygiene_snapshot_vault_mvp.sfsc.inclusion',
    'hygiene_snapshot_vault_mvp.sfsc.offers',
    'hygiene_snapshot_vault_mvp.sfsc.opportunity',
    'hygiene_snapshot_vault_mvp.sfsc.rebooking_request_cases'
    ];
var arrayLength = arrayOfTables.length;
for (i = 0; i < arrayLength; i++) {
    var sql_command = `
        INSERT INTO scratch.robinpatel.table_usage
        SELECT
            '${arrayOfTables[i]}' AS table_source,
            role_name,
            COUNT(*) AS table_queries
        FROM snowflake.account_usage.query_history
        WHERE LOWER(query_text) LIKE ('% ' || '${arrayOfTables[i]}' || ' %')
          AND role_name IS DISTINCT FROM 'PIPELINERUNNER'
          AND start_time::DATE >= CURRENT_DATE() - 90
        GROUP BY 1,2;
    `
    var stmt = snowflake.createStatement( {sqlText: sql_command} );
    var res = stmt.execute();
};
$$;

CALL scratch.robinpatel.table_users();


TRUNCATE scratch.robinpatel.table_usage;
SELECT *
FROM scratch.robinpatel.table_usage;


------------------------------------------------------------------------------------------------------------------------

USE ROLE pipelinerunner;
USE WAREHOUSE pipe_large;

CALL scratch.robinpatel.table_users('scratch.robinpatel.table_usage',
                                    'raw_vault.sfsc.account__a_l, raw_vault.sfsc.account__m_z, raw_vault.sfsc.inclusion, raw_vault.sfsc.offers, raw_vault.sfsc.opportunity__a_l, raw_vault.sfsc.opportunity__m_z, raw_vault.sfsc.rebooking_request_cases, raw_vault.sfsc.salesforce_cases, hygiene_vault_mvp.sfsc.account, hygiene_vault_mvp.sfsc.inclusion, hygiene_vault_mvp.sfsc.offers, hygiene_vault_mvp.sfsc.opportunity, hygiene_vault_mvp.sfsc.rebooking_request_cases, hygiene_vault_mvp.sfsc.salesforce_cases, hygiene_snapshot_vault_mvp.sfsc.account, hygiene_snapshot_vault_mvp.sfsc.inclusion, hygiene_snapshot_vault_mvp.sfsc.offers, hygiene_snapshot_vault_mvp.sfsc.opportunity, hygiene_snapshot_vault_mvp.sfsc.rebooking_request_cases');

--attempt with array argument
CREATE OR REPLACE PROCEDURE scratch.robinpatel.table_users(outputtable VARCHAR, tablelist VARCHAR
                                                          )
    RETURNS BOOLEAN
    LANGUAGE JAVASCRIPT
AS
$$
var outputTableReference = OUTPUTTABLE;
var tableListString = TABLELIST.replace(/\s/g, ""); // to accommodate for spaces in list
var arrayOfTables = tableListString.split(",");
var arrayLength = arrayOfTables.length;

var sql_command = `
CREATE OR REPLACE TRANSIENT TABLE ${outputTableReference}
(
    table_source  VARCHAR,
    role_name     VARCHAR,
    last_query_tstamp TIMESTAMP,
    table_queries NUMBER
)
;
`
var stmt = snowflake.createStatement( {sqlText: sql_command} );
var res = stmt.execute();

for (i = 0; i < arrayLength; i++) {
    var sql_command = `
        INSERT INTO ${outputTableReference}
        SELECT
            '${arrayOfTables[i]}' AS table_source,
            role_name,
            MAX(start_time) AS last_query_tstamp,
            COUNT(*) AS table_queries
        FROM snowflake.account_usage.query_history
        WHERE LOWER(query_text) LIKE ('% ' || '${arrayOfTables[i]}' || ' %')
          AND role_name IS DISTINCT FROM 'PIPELINERUNNER'
          AND start_time::DATE >= CURRENT_DATE() - 90
        GROUP BY 1,2;
    `
    var stmt = snowflake.createStatement( {sqlText: sql_command} );
    var res = stmt.execute();
};
return 'TRUE';
$$;



USE ROLE pipelinerunner;
USE WAREHOUSE pipe_large;

CALL data_vault_mvp.dwh.table_users('scratch.robinpatel.table_usage',
                                    'raw_vault.sfsc.account__a_l, raw_vault.sfsc.account__m_z, raw_vault.sfsc.inclusion, raw_vault.sfsc.offers, raw_vault.sfsc.opportunity__a_l, raw_vault.sfsc.opportunity__m_z, raw_vault.sfsc.rebooking_request_cases, raw_vault.sfsc.salesforce_cases, hygiene_vault_mvp.sfsc.account, hygiene_vault_mvp.sfsc.inclusion, hygiene_vault_mvp.sfsc.offers, hygiene_vault_mvp.sfsc.opportunity, hygiene_vault_mvp.sfsc.rebooking_request_cases, hygiene_vault_mvp.sfsc.salesforce_cases, hygiene_snapshot_vault_mvp.sfsc.account, hygiene_snapshot_vault_mvp.sfsc.inclusion, hygiene_snapshot_vault_mvp.sfsc.offers, hygiene_snapshot_vault_mvp.sfsc.opportunity, hygiene_snapshot_vault_mvp.sfsc.rebooking_request_cases');

SELECT *
FROM scratch.robinpatel.table_usage;

------------------------------------------------------------------------------------------------------------------------
SET table_name = 'hygiene_snapshot_vault_mvp.sfsc.opportunity';

SELECT
    $table_name                                                                        AS referenced_table,
    views.created,
    views.last_altered,
    views.table_owner                                                                  AS view_owner,
    table_name                                                                         AS view,
    LOWER(views.table_catalog || '.' || views.table_schema || '.' || views.table_name) AS view_location,
    LOWER(view_definition)                                                             AS view_definition
FROM collab.information_schema.views
WHERE LOWER(views.view_definition) LIKE ('% ' || $table_name || ' %');


------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.table_referenced_in_view
(
    referenced_table VARCHAR,
    database         VARCHAR,
    created          TIMESTAMP,
    last_altered     TIMESTAMP,
    view_owner       VARCHAR,
    view_name        VARCHAR,
    view_location    VARCHAR,
    view_definition  VARCHAR
)
;

USE ROLE pipelinerunner;

CALL scratch.robinpatel.table_referenced_in_view('scratch.robinpatel.table_referenced_in_view',
                                                 'raw_vault.sfsc.account__a_l, raw_vault.sfsc.account__m_z, hygiene_snapshot_vault_mvp.sfsc.opportunity', 'collab, data_vault_mvp, se');

CALL data_vault_mvp.dwh.table_reference_in_view('scratch.robinpatel.table_referenced_in_view',
                                                'raw_vault.sfsc.account__a_l, raw_vault.sfsc.account__m_z, hygiene_snapshot_vault_mvp.sfsc.opportunity', 'collab, data_vault_mvp, se');


SELECT *
FROM scratch.robinpatel.table_referenced_in_view;


--attempt with array argument
CREATE OR REPLACE PROCEDURE scratch.robinpatel.table_referenced_in_view(outputtable VARCHAR, tablelist VARCHAR, databaselist VARCHAR
                                                                       )
    RETURNS BOOLEAN
    LANGUAGE JAVASCRIPT
AS
$$
var outputTableReference = OUTPUTTABLE;

var tableListString = TABLELIST.replace(/\s/g, '');
var arrayOfTables = tableListString.split(",");
var tableArrayLength = arrayOfTables.length;

var databaseListString = DATABASELIST.replace(/\s/g, '');
var arrayOfDatabases = databaseListString.split(",")
var databaseArrayLength = arrayOfDatabases.length;;

var create_table_command = `
CREATE OR REPLACE TRANSIENT TABLE ${outputTableReference}
(
    referenced_table  VARCHAR,
    database VARCHAR,
    created  TIMESTAMP,
    last_altered  TIMESTAMP,
    view_owner  VARCHAR,
    view_name VARCHAR,
    view_location VARCHAR,
    view_definition VARCHAR
)
;
`
var stmt = snowflake.createStatement( {sqlText: create_table_command} );
var res = stmt.execute();

for (x = 0; x < databaseArrayLength; x++) {
    for (i = 0; i < tableArrayLength; i++) {
        var sql_command = `
            INSERT INTO ${outputTableReference}
            SELECT
                '${arrayOfTables[i]}' AS referenced_table,
                '${arrayOfDatabases[x]}' AS database,
                views.created,
                VIEWS.last_altered,
                VIEWS.table_owner AS view_owner,
                table_name AS view_name,
                LOWER(views.table_catalog || '.' || views.table_schema || '.' || views.table_name) AS view_location,
                LOWER(view_definition) AS view_definition
            FROM ${arrayOfDatabases[x]}.information_schema.views
            WHERE LOWER(views.view_definition) LIKE ('% ' || '${arrayOfTables[i]}' || ' %')
        `
        var stmt = snowflake.createStatement( {sqlText: sql_command} );
        var res = stmt.execute();
    }
};
return 'TRUE';
$$;


------------------------------------------------------------------------------------------------------------------------


/*SELECT DISTINCT
    u.role
FROM snowflake.account_usage.grants_to_users u
    INNER JOIN snowflake.account_usage.grants_to_roles r
               ON u.role = r.grantee_name
WHERE u.granted_to = 'USER'
  AND r.table_catalog = $database_name
  AND r.table_schema = $schema_name
  AND r.name = $object_name
  AND u.deleted_on IS NULL
  AND r.deleted_on IS NULL;*/


SELECT
    u.created_on,
    u.deleted_on,
    u.role,
    u.grantee_name,
    u.granted_by,
    r.created_on,
    r.modified_on,
    r.privilege,
    r.granted_on,
    r.name,
    r.table_schema,
    r.granted_to,
    r.grant_option,
    r.granted_by,
    r.deleted_on
FROM snowflake.account_usage.grants_to_users u
    INNER JOIN snowflake.account_usage.grants_to_roles r
               ON u.role = r.grantee_name
WHERE u.granted_to = 'USER'
  AND u.deleted_on IS NULL
  AND r.deleted_on IS NULL;


CREATE OR REPLACE PROCEDURE scratch.robinpatel.object_premissions(outputtable VARCHAR, tablelist VARCHAR
                                                                 )
    RETURNS BOOLEAN
    LANGUAGE JAVASCRIPT
AS
$$
var outputTableReference = OUTPUTTABLE;
var tableListString = TABLELIST.replace(/\s/g, ""); // to accommodate for spaces in list
var arrayOfTables = tableListString.split(",");
var arrayLength = arrayOfTables.length;

var sql_command = `
CREATE OR REPLACE TRANSIENT TABLE ${outputTableReference}
(
    table_source VARCHAR,
    role_name VARCHAR,
    user_name VARCHAR,
    created_on TIMESTAMP,
    modified_on TIMESTAMP,
    granted_name VARCHAR,
    granted_by VARCHAR,
    priviledge VARCHAR,
    database VARCHAR,
    schema VARCHAR

)
;
`
var stmt = snowflake.createStatement( {sqlText: sql_command} );
var res = stmt.execute();

for (i = 0; i < arrayLength; i++) {
    var sql_command = `
        INSERT INTO ${outputTableReference}

        SELECT DISTINCT
            '${arrayOfTables[i]}' AS table_source,
            u.role AS role_name
        FROM snowflake.account_usage.grants_to_users u
            INNER JOIN snowflake.account_usage.grants_to_roles r
                       ON u.role = r.grantee_name
        WHERE u.granted_to = 'USER'
          AND r.name = ${arrayOfTables[i]}
          AND u.deleted_on IS NULL
          AND r.deleted_on IS NULL
    `
    var stmt = snowflake.createStatement( {sqlText: sql_command} );
    var res = stmt.execute();
};
return 'TRUE';
$$;


------------------------------------------------------------------------------------------------------------------------
USE ROLE pipelinerunner;

CALL data_vault_mvp.dwh.table_reference_in_view('scratch.robinpatel.table_reference_in_view', 'data_vault_mvp.bi.chrt_fact_cohort_metrics, se.bi.chrt_fact_cohort_metrics', 'collab, data_vault_mvp, se, scratch');

SELECT *
FROM scratch.robinpatel.table_reference_in_view;