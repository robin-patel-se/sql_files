SHOW GRANTS TO ROLE personal_role__robinpatel
;


SHOW GRANTS ON WAREHOUSE pipe_xlarge
;

SHOW GRANTS TO ROLE data_compute_pipe_xlarge
;

SHOW GRANTS TO USER robinpatel
;

SHOW GRANTS ON ROLE data_compute_pipe_xlarge
;

SHOW GRANTS ON ROLE data_compute_pipe_large
;

SHOW GRANTS ON ROLE data_compute_pipe_medium
;

SHOW GRANTS ON ROLE data_team_basic
;

SHOW GRANTS ON ROLE data_team_extended
;


SHOW GRANTS ON ROLE default_compute;
------------------------------------------------------------------------------------------------------------------------

SHOW GRANTS ON ROLE marketing_compute_pipe_xlarge
;

-- grantee_name
-- ACCOUNTADMIN
-- DATA_TEAM_BASIC
-- FPA_TEAM_EXTENDED
-- PERSONAL_ROLE__ALEXHENSHAW
-- PERSONAL_ROLE__GEORGIEAGNEW
-- PERSONAL_ROLE__KRYSTYNAJOHNSON


SHOW GRANTS ON ROLE marketing_compute_pipe_large
;

-- grantee_name
-- DATA_TEAM_BASIC
-- FPA_TEAM_EXTENDED
-- PERSONAL_ROLE__ALEXHENSHAW
-- PERSONAL_ROLE__APOORVAKAPAVARAPU
-- PERSONAL_ROLE__DARSHANASRIDHAR
-- PERSONAL_ROLE__GARETHBOOTH
-- PERSONAL_ROLE__GEORGIEAGNEW
-- PERSONAL_ROLE__JACKBACKLER
-- PERSONAL_ROLE__KRYSTYNAJOHNSON
-- PERSONAL_ROLE__SAIFWIDYATMOKO
-- SECURITYADMIN


SHOW GRANTS ON ROLE fpa_team_extended
;

-- grantee_name
-- PERSONAL_ROLE__DHARMITABHANDERI
-- PERSONAL_ROLE__JAMESCARTER
-- PIPELINERUNNER
-- SECURITYADMIN

SHOW GRANTS TO ROLE fpa_team_extended
;

-- name
-- CUSTOMER_INSIGHT_COMPUTE_2XLARGE
-- CUSTOMER_INSIGHT_COMPUTE_LARGE
-- CUSTOMER_INSIGHT_COMPUTE_MEDIUM
-- CUSTOMER_INSIGHT_COMPUTE_SMALL
-- CUSTOMER_INSIGHT_COMPUTE_XLARGE
-- FPA_TEAM_BASIC
-- MARKETING_COMPUTE_PIPE_LARGE
-- MARKETING_COMPUTE_PIPE_XLARGE

SELECT DISTINCT
	wmh.warehouse_name
FROM snowflake.account_usage.warehouse_metering_history wmh
WHERE wmh.start_time >= CURRENT_DATE - 30
;


USE ROLE pipelinerunner;
CALL data_vault_mvp.dwh.table_reference_in_view('scratch.robinpatel.table_reference_in_view', 'latest_vault.cms_mysql.booking_cancellation', 'collab, data_vault_mvp, se');
SELECT * FROM scratch.robinpatel.table_reference_in_view;

SELECT get_ddl('table', 'collab.travel_trust.booking_cancellation');
SELECT get_ddl('table', 'collab.finance.booking_cancellation');