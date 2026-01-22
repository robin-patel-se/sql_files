USE ROLE securityadmin
;

GRANT OWNERSHIP ON ALL semantic views IN SCHEMA se.data
TO ROLE ai_admin;

GRANT OWNERSHIP ON ALL semantic views IN SCHEMA se.bi
TO ROLE ai_admin COPY current GRANTS
;

GRANT SELECT ON ALL SEMANTIC VIEWS IN SCHEMA se.data TO ROLE data_team_basic;