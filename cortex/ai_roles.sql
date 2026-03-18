USE ROLE securityadmin
;


-- 1. Create Functional Roles for the SnowMind Ecosystem
CREATE ROLE IF NOT EXISTS ai_admin
; -- The Architect (e.g., Donald)
CREATE ROLE IF NOT EXISTS ai_developer
; -- The Playbook Creator (e.g., Alex H)
CREATE ROLE IF NOT EXISTS ai_user
;

-- The Senior Partner (End User)

-- 2. Grant Core AI Permissions to the "Brain" and "Manager"
-- Cortex User allows general LLM functions (The Brain)
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE AI_USER
;

GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE AI_DEVELOPER
;

-- 3. Specialized Permissions for the "Data Expert" (Analyst)
-- Allows the Developer to build and test Semantic Views
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_ANALYST_USER TO ROLE AI_DEVELOPER
;

-- 4. Specialized Permissions for the "Manager" (Agent)
-- Allows the Manager to orchestrate Search, Analyst, and Intelligence
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_AGENT_USER TO ROLE AI_ADMIN
;

GRANT DATABASE ROLE SNOWFLAKE.CORTEX_AGENT_USER TO ROLE AI_DEVELOPER
;

-- 5. Data Access Governance (The Librarian & Data Expert)
-- Only Developers should have access to the underlying Search Services and Semantic Logic
GRANT USAGE ON SCHEMA snowmind_db.semantic_layers TO ROLE ai_developer
;

GRANT USAGE ON SERVICE SNOWMIND_DB.SEARCH.TRADING_DECKS_SEARCH TO ROLE AI_DEVELOPER
;

-- 6. Hierarchy: Admins inherit Developer rights
GRANT ROLE ai_developer TO ROLE ai_admin
;

GRANT ROLE ai_user TO ROLE ai_developer
;

------------------------------------------------------------------------------------------------------------------------
-- setting up the SnowMind Database and Schemas for our AI Ecosystem

-- 1. Create the dedicated Intelligence Database
CREATE DATABASE IF NOT EXISTS snowmind
	COMMENT = 'Dedicated database for SnowMind AI Ecosystem and Trading Agent'
;

-- 2. Create Functional Schemas for our Specialists
-- The Playbook Schema: For Snowflake Semantic Views
CREATE SCHEMA IF NOT EXISTS snowmind.semantic_views
	COMMENT = 'Storage for the Analyst Playbook (YAML business logic)'
;

-- The Library Schema: For Cortex Search Services
CREATE SCHEMA IF NOT EXISTS snowmind.search_services
	COMMENT = 'Specialist: The Librarian - Houses search indexes for unstructured text'
;

-- The Staging Schema: For raw documents and logs
CREATE SCHEMA IF NOT EXISTS snowmind.staging_unstructured
	COMMENT = 'Landing zone for PDFs, calendars, and release notes'
;

-- The Monitoring Schema: For the Feedback Loop & Cost Tracking
CREATE SCHEMA IF NOT EXISTS snowmind.monitoring
	COMMENT = 'Governance: Feedback logs and Cortex AI consumption tracking'
;

-- 3. Set up the Internal Library Stage (for the Librarian)
CREATE OR REPLACE STAGE snowmind.staging_unstructured.doc_library DIRECTORY = (ENABLE = TRUE)
	ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')

COMMENT = 'Stage for trading decks and event logs to be indexed by Cortex Search'
;

CREATE SCHEMA IF NOT EXISTS snowmind.agents
	COMMENT = 'Storage for the Manager - Cortex Agents'
;


------------------------------------------------------------------------------------------------------------------------

-- 1. DATABASE ACCESS HIERARCHY
-- ==========================================
GRANT USAGE ON DATABASE SNOWMIND TO ROLE AI_ADMIN
;

GRANT USAGE ON DATABASE SNOWMIND TO ROLE AI_DEVELOPER
;

GRANT USAGE ON DATABASE SNOWMIND TO ROLE AI_USER
;

-- A. THE PLAYBOOK (The renamed Semantic Views schema)
GRANT ALL PRIVILEGES ON SCHEMA snowmind.semantic_views TO ROLE ai_admin
;

GRANT ALL PRIVILEGES ON SCHEMA snowmind.semantic_views TO ROLE ai_developer
;

GRANT USAGE ON SCHEMA snowmind.semantic_views TO ROLE ai_user
;

-- B. THE LIBRARY (Search & Staging)
GRANT ALL PRIVILEGES ON SCHEMA snowmind.search_services TO ROLE ai_developer
;

GRANT ALL PRIVILEGES ON SCHEMA snowmind.staging_unstructured TO ROLE ai_developer
;

GRANT READ, WRITE ON STAGE snowmind.staging_unstructured.doc_library TO ROLE ai_developer
;

GRANT USAGE ON SCHEMA snowmind.search_services TO ROLE ai_user
;

-- C. THE MONITORING
GRANT ALL PRIVILEGES ON SCHEMA snowmind.monitoring TO ROLE ai_admin
;

-- D. THE PLAYBOOK (The renamed Semantic Views schema)
GRANT ALL PRIVILEGES ON SCHEMA snowmind.agents TO ROLE ai_admin
;

GRANT ALL PRIVILEGES ON SCHEMA snowmind.agents TO ROLE ai_developer
;

GRANT USAGE ON SCHEMA snowmind.agents TO ROLE ai_user
;

-- 2. CORTEX SERVICE BINDINGS
-- ==========================================
-- Admin: Orchestration & Cost Governance (SPI-8130)
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE AI_ADMIN
;

-- Developer: Building the Data Expert & Librarian tools
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE AI_DEVELOPER
;

GRANT DATABASE ROLE SNOWFLAKE.CORTEX_ANALYST_USER TO ROLE AI_DEVELOPER
;

-- User: Narrative generation and Agent interaction
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE AI_USER
;

-- 3. ROLE INHERITANCE
-- ==========================================
GRANT ROLE ai_user TO ROLE ai_developer
;

GRANT ROLE ai_developer TO ROLE ai_admin
;



------------------------------------------------------------------------------------------------------------------------
-- for snowmind to work seamlessly a person's default role and warehouse needs to be set.


ALTER USER jonathandownes SET
	DEFAULT_ROLE = personal_role__jonathandownes,
    DEFAULT_WAREHOUSE = 'PIPE_DEFAULT'
;

ALTER USER janhitzke SET
	DEFAULT_ROLE = personal_role__janhitzke,
    DEFAULT_WAREHOUSE = 'PIPE_DEFAULT'
;

ALTER USER konstantineberle SET
	DEFAULT_ROLE = personal_role__konstantineberle,
    DEFAULT_WAREHOUSE = 'PIPE_DEFAULT'
;

ALTER USER joshuamiranda SET
	DEFAULT_ROLE = personal_role__joshuamiranda,
    DEFAULT_WAREHOUSE = 'PIPE_DEFAULT'
;

ALTER USER guidoferreira SET
	DEFAULT_ROLE = personal_role__guidoferreira,
    DEFAULT_WAREHOUSE = 'PIPE_DEFAULT'
;

------------------------------------------------------------------------------------------------------------------------
USE ROLE securityadmin
;

GRANT SELECT ON SCHEMA snowmind.semantic_views
        TO ROLE ai_user
;

USE ROLE pipelinerunner
;

SHOW TABLES IN SCHEMA data_vault_mvp.travelbird


SELECT *
FROM raw_vault_mvp.
;

USE ROLE ai_admin
;

GRANT OWNERSHIP ON agent dbt.bi_product_analytics__intermediate.ces TO ROLE ai_developer COPY CURRENT GRANTS
;

SELECT GET_DDL('CORTEX_AGENT', 'se.data.trading')
;


SELECT '0.00005'::DECIMAL(13, 6)
;


SELECT
	ds.se_sale_id,
    ds.tb_cms_url
FROM se.data.tb_offer ds
WHERE ds.se_sale_id = 'A80695'
;

GRANT OWNERSHIP ON semantic view snowmind.semantic_views.crm_analysis TO ROLE ai_developer COPY CURRENT GRANTS
;


GRANT OWNERSHIP ON AGENT se.data.crm TO ROLE ai_developer COPY CURRENT GRANTS
;

SHOW CORTEX SEARCH SERVICES IN ACCOUNT;

USE ROLE ai_admin
DROP semantic view se.data.JUNIPER_RATES_COMPARISON;
DROP semantic view se.data.CRM_ANALYSIS;
DROP semantic view se.data.BOOKING_ANALYSIS;
DROP semantic view se.bi.SESSION_ANALYSIS;
DROP semantic view se.bi.availability_analysis;




USE ROLE securityadmin;
GRANT SELECT ON TABLE dbt.bi_commercial_insights_planning.cip_search_model_agg_snowmind TO ROLE se_basic;
