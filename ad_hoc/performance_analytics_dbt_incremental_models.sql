SELECT COUNT(*) FROM dbt_dev.dbt_robinpatel_performance_analytics.pa_margins_last_paid;
-- 1,600,335

SELECT COUNT(*) FROM dbt.bi_performance_analytics.pa_margins_last_paid;
-- 570,201

USE ROLE personal_role__dbt_prod;
CREATE OR REPLACE TRANSIENT TABLE dbt.bi_performance_analytics.pa_margins_last_paid_20250820 CLONE dbt.bi_performance_analytics.pa_margins_last_paid;
CREATE OR REPLACE TRANSIENT TABLE dbt.bi_performance_analytics.pa_margins_last_paid CLONE dbt_dev.dbt_robinpatel_performance_analytics.pa_margins_last_paid;
CREATE OR REPLACE TRANSIENT TABLE dbt.bi_performance_analytics.pa_margins_last_paid COPY GRANTS CLONE dbt_dev.dbt_robinpatel_performance_analytics.pa_margins_last_paid;

SELECT * FROM latest_vault.kingfisher.sales_kingfisher;

SELECT COUNT(*) FROM dbt.bi_performance_analytics.pa_margins_last_click; --84,580
SELECT COUNT(*) FROM dbt_dev.dbt_robinpatel_performance_analytics.pa_margins_last_click --238,991

CREATE OR REPLACE TRANSIENT TABLE dbt.bi_performance_analytics.pa_margins_last_click_20250820 CLONE dbt.bi_performance_analytics.pa_margins_last_click;
CREATE OR REPLACE TRANSIENT TABLE dbt.bi_performance_analytics.pa_margins_last_click COPY GRANTS CLONE dbt_dev.dbt_robinpatel_performance_analytics.pa_margins_last_click;

GRANT SELECT ON TABLE dbt_dev.dbt_robinpatel_performance_analytics.pa_margins_last_click TO ROLE personal_role__krystynajohnson;


SELECT COUNT(*) FROM dbt_dev.dbt_robinpatel_performance_analytics.pa_margins_last_non_direct; -- 1,275,877
SELECT COUNT(*) FROM dbt.bi_performance_analytics.pa_margins_last_non_direct; -- 419,765

USE role personal_role__dbt_prod
CREATE OR REPLACE TRANSIENT TABLE dbt.bi_performance_analytics.pa_margins_last_non_direct_20250820 CLONE dbt.bi_performance_analytics.pa_margins_last_non_direct;
CREATE OR REPLACE TRANSIENT TABLE dbt.bi_performance_analytics.pa_margins_last_non_direct COPY GRANTS CLONE dbt_dev.dbt_robinpatel_performance_analytics.pa_margins_last_non_direct;
