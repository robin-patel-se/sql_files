--query to check who has access to a certain data object so these permissions can be reapplied to another data object
SET database_name = 'DATA_SCIENCE';
SET schema_name = 'PREDICTIVE_MODELING';
SET object_name = 'USER_DEAL_EVENTS';

SELECT DISTINCT
    u.grantee_name AS user_name
FROM snowflake.account_usage.grants_to_users u
    INNER JOIN snowflake.account_usage.grants_to_roles r
               ON u.role = r.grantee_name
WHERE u.granted_to = 'USER'
  AND r.table_catalog = $database_name
  AND r.table_schema = $schema_name
  AND r.name = $object_name
  AND u.deleted_on IS NULL
  AND r.deleted_on IS NULL;


SET database_name = 'DATA_SCIENCE';
SET schema_name = 'MART_ANALYTICS';
SET object_name = 'VW_DEAL_TAGS';

SELECT DISTINCT
--        u.grantee_name AS user_name
u.role
FROM snowflake.account_usage.grants_to_users u
    INNER JOIN snowflake.account_usage.grants_to_roles r
               ON u.role = r.grantee_name
WHERE u.granted_to = 'USER'
  AND r.table_catalog = $database_name
  AND r.table_schema = $schema_name
  AND r.name = $object_name
  AND u.deleted_on IS NULL
  AND r.deleted_on IS NULL;

data_science.operational_output.vw_recommended_deals_augmented

SET database_name = 'COLLAB';
SET schema_name = 'MUSE';
SET object_name = 'SNOWFLAKE_QUERY_HISTORY';

SELECT DISTINCT
    u.role
FROM snowflake.account_usage.grants_to_users u
    INNER JOIN snowflake.account_usage.grants_to_roles r
               ON u.role = r.grantee_name
WHERE u.granted_to = 'USER'
  AND r.table_catalog = $database_name
  AND r.table_schema = $schema_name
  AND r.name = $object_name
  AND u.deleted_on IS NULL
  AND r.deleted_on IS NULL;
