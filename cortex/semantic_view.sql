USE ROLE securityadmin
;

GRANT OWNERSHIP ON ALL semantic views IN SCHEMA se.data
TO ROLE ai_admin;

GRANT OWNERSHIP ON ALL semantic views IN SCHEMA se.bi
TO ROLE ai_admin COPY current GRANTS
;

GRANT SELECT ON ALL SEMANTIC VIEWS IN SCHEMA se.data TO ROLE data_team_basic;

USE ROLE securityadmin;
GRANT OWNERSHIP ON semantic view snowmind.semantic_views.TARGETS_SEMANTIC_VIEW TO ROLE ai_developer COPY current GRANTS;


USE ROLE ai_developer;
ALTER ROLE ai_developer SET DEFAULT_WAREHOUSE = ANALYST_MEDIUM

GRANT OWNERSHIP ON agent snowmind.agents.search_agent TO ROLE ai_developer COPY current GRANTS;