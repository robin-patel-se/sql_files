"name":"Cluster_Hierarchy",
"name":"Cluster_Region_Hierarchy",
"name":"Cluster_Sub_Region_Hierarchy",
"name":"Target_Month",
"name":"Target_Year",
"name":"Shared_Region",
"name":"Target_Split",
"name":"Product_Types",
"name":"Target_Split_Check",
"name":"Junior_Contracting_Manager",
"name":"Contracting_Executive",
"name":"Contracting_Manager",
"name":"Team_Lead",
"name":"Head_of_Contracting",
"name":"Contracting_Director",
"name":"Start_Date",
"name":"End_Date",



CLUSTER
Cluster_Region
Cluster_Sub_Region
Target_Month
Target_Year
Shared_Region
Target_Split
Product_Types
Target_Split_Check
Junior_Contracting_Manager
Contracting_Executive
Contracting_Manager
Team_Lead
Head_of_Contracting
Contracting_Director
Start_Date
End_Date

dataset_task --include 'cro_gsheets.sub_region_target_splits' --operation LatestRecordsOperation --method 'run' --upstream --start '2020-07-15 00:30:00' --end '2020-07-15 00:30:00'


SELECT *
FROM hygiene_vault_dev_robin.sub_region_target_splits.sub_region_target_splits
;

SELECT *
FROM latest_vault_dev_robin.sub_region_target_splits.sub_region_target_splits
;

SELECT *
FROM latest_vault_dev_robin.cro_gsheets.sub_region_target_splits
;

SELECT TO_DATE('2024-01-02', 'yyyy-dd-MM')


SELECT *
FROM dbt_dev.dbt_robinpatel_staging.base_cro_gsheets__sub_region_target_splits
;

USE ROLE personal_role__robinpatel;
SELECT *
FROM dbt.bi_staging.base_cro_gsheets__sub_region_target_splits
;


