CREATE OR REPLACE TABLE scratch.robinpatel.attrition_data
(
    employee_number          VARCHAR,
    employee_or_contractor   VARCHAR,
    location                 VARCHAR,
    manager                  VARCHAR,
    head_of_department       VARCHAR,
    job_role__division__post VARCHAR,
    job_role__function__post VARCHAR,
    leaving_date             VARCHAR,
    start_date               VARCHAR,
    reason_for_leaving       VARCHAR
);


USE SCHEMA scratch.robinpatel;

PUT file:///Users/robin/myrepos/sql_files/attrition_wig/AttritionDataV2-DataSheet.csv @%attrition_data;

COPY INTO scratch.robinpatel.attrition_data
    FILE_FORMAT = (
        TYPE = CSV
            FIELD_DELIMITER = ','
            SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
            RECORD_DELIMITER = '\\n'
        );

SELECT MIN(TO_DATE(ad.start_date, 'dd/MM/yyyy')) AS start_date
FROM scratch.robinpatel.attrition_data ad -- 2012-06-25

CREATE OR REPLACE VIEW scratch.robinpatel.attrition_user_data AS
(
WITH model_user_data AS (
    SELECT ad.employee_number,
           ad.employee_or_contractor,
           ad.location,
           ad.manager,
           ad.head_of_department,
           ad.job_role__division__post,
           ad.job_role__function__post,
           TO_DATE(ad.start_date, 'dd/MM/yyyy')                                         AS start_date,
           TO_DATE(ad.leaving_date, 'dd/MM/yyyy')                                       AS leaving_date,
           ad.reason_for_leaving,
           IFF(ad.leaving_date IS NOT NULL, 'No longer employed', 'Currently employed') AS employment_status
    FROM scratch.robinpatel.attrition_data ad
),
     grain AS (
         SELECT sc.date_value,
                sc.month,
                sc.month_name,
                sc.year,
                mud.employee_number,
                mud.employee_or_contractor,
                mud.location,
                mud.manager,
                mud.head_of_department,
                mud.job_role__division__post,
                mud.job_role__function__post,
                mud.start_date,
                mud.leaving_date,
                mud.employment_status
         FROM se.data.se_calendar sc
             --join to get a currently employed count
             LEFT JOIN model_user_data mud ON sc.date_value >= mud.start_date
             AND sc.date_value <= COALESCE(mud.leaving_date, CURRENT_DATE)
         WHERE sc.date_value >= ( -- earliest start date
             SELECT MIN(TO_DATE(ad.start_date, 'dd/MM/yyyy')) AS start_date
             FROM scratch.robinpatel.attrition_data ad
         )
           AND sc.date_value <= CURRENT_DATE
     )

SELECT g.date_value,
       g.month,
       g.month_name,
       g.year,
       g.employee_number,
       g.employee_or_contractor,
       g.location,
       g.manager,
       g.head_of_department,
       g.job_role__division__post,
       g.job_role__function__post,
       g.start_date,
       g.leaving_date,
       g.employment_status,
       IFF(j.employee_number IS NOT NULL, TRUE, FALSE) AS employee_joined,
       IFF(l.employee_number IS NOT NULL, TRUE, FALSE) AS employee_left,
       l.reason_for_leaving
FROM grain g
    --new starters join
    LEFT JOIN model_user_data j ON g.date_value = j.start_date AND g.employee_number = j.employee_number
                  --leavers join
    LEFT JOIN model_user_data l ON g.date_value = l.leaving_date AND g.employee_number = l.employee_number
    )
;

SELECT *
FROM scratch.robinpatel.attrition_user_data;

SELECT aud.year,
       aud.month,
       aud.job_role__division__post        AS division,
       aud.job_role__function__post        AS function,
       COUNT(DISTINCT aud.employee_number) AS employed,
       SUM(IFF(employee_joined, 1, 0))     AS joiners,
       SUM(IFF(employee_left, 1, 0))       AS leavers
FROM scratch.robinpatel.attrition_user_data aud
GROUP BY 1, 2, 3, 4;


--whole tech summary
WITH agg AS (
    SELECT aud.year,
           aud.month,
           SUM(IFF(employee_joined, 1, 0)) AS joiners,
           SUM(IFF(employee_left, 1, 0))   AS leavers
    FROM scratch.robinpatel.attrition_user_data aud
    GROUP BY 1, 2
)
SELECT a.year,
       a.month,
       a.joiners,
       a.leavers,
       SUM(a.joiners) OVER (ORDER BY a.year, a.month) - a.joiners          AS opening_month_cumulative_joiners,
       SUM(a.leavers) OVER (ORDER BY a.year, a.month) - a.leavers          AS opening_month_cumulative_leavers,
       SUM(a.joiners) OVER (ORDER BY a.year, a.month)                      AS closing_month_cumulative_joiners,
       SUM(a.leavers) OVER (ORDER BY a.year, a.month)                      AS closing_month_cumulative_leavers,
       opening_month_cumulative_joiners - opening_month_cumulative_leavers AS opening_month_employed,
       closing_month_cumulative_joiners - closing_month_cumulative_leavers AS closing_month_employed
FROM agg a
ORDER BY 1, 2;

WITH agg AS (
    SELECT aud.year,
           aud.month,
           SUM(IFF(employee_joined, 1, 0)) AS joiners,
           SUM(IFF(employee_left, 1, 0))   AS leavers
    FROM scratch.robinpatel.attrition_user_data aud
    GROUP BY 1, 2
),
     cumulative_sums AS (
         SELECT a.year,
                a.month,
                a.joiners,
                a.leavers,
                SUM(a.joiners) OVER (ORDER BY a.year, a.month) - a.joiners          AS opening_month_cumulative_joiners,
                SUM(a.leavers) OVER (ORDER BY a.year, a.month) - a.leavers          AS opening_month_cumulative_leavers,
                SUM(a.joiners) OVER (ORDER BY a.year, a.month)                      AS closing_month_cumulative_joiners,
                SUM(a.leavers) OVER (ORDER BY a.year, a.month)                      AS closing_month_cumulative_leavers,
                opening_month_cumulative_joiners - opening_month_cumulative_leavers AS opening_month_employed,
                closing_month_cumulative_joiners - closing_month_cumulative_leavers AS closing_month_employed
         FROM agg a
         ORDER BY 1, 2
     )
SELECT cs.year,
       cs.month,
       cs.joiners,
       cs.leavers,
       cs.opening_month_employed,
       cs.closing_month_employed,
       SUM(cs.leavers) OVER (PARTITION BY cs.year)                                                                AS yearly_leavers,
       AVG(cs.opening_month_employed) OVER (PARTITION BY cs.year)                                                 AS yearly_avg_no_employees,
       (yearly_leavers / yearly_avg_no_employees)                                                         AS attrition,
       SUM(cs.leavers) OVER (ORDER BY cs.year, cs.month ROWS BETWEEN 12 PRECEDING AND CURRENT ROW)                AS leavers_12m,
       AVG(cs.opening_month_employed) OVER (ORDER BY cs.year, cs.month ROWS BETWEEN 12 PRECEDING AND CURRENT ROW) AS avg_employees_12m,
       (leavers_12m / NULLIF(avg_employees_12m, 0))                                                        AS attrition
FROM cumulative_sums cs;

------------------------------------------------------------------------------------------------------------------------

--department function
WITH agg AS (
    SELECT aud.year,
           aud.month,
           aud.job_role__division__post    AS division,
           aud.job_role__function__post    AS function,
           SUM(IFF(employee_joined, 1, 0)) AS joiners,
           SUM(IFF(employee_left, 1, 0))   AS leavers
    FROM scratch.robinpatel.attrition_user_data aud
    GROUP BY 1, 2, 3, 4
)
SELECT a.year,
       a.month,
       a.division,
       a.function,
       a.joiners,
       a.leavers,
       SUM(a.joiners) OVER (PARTITION BY a.division, a.function ORDER BY a.year, a.month) - a.joiners AS opening_month_cumulative_joiners,
       SUM(a.leavers) OVER (PARTITION BY a.division, a.function ORDER BY a.year, a.month) - a.leavers AS opening_month_cumulative_leavers,
       SUM(a.joiners) OVER (PARTITION BY a.division, a.function ORDER BY a.year, a.month)             AS closing_month_cumulative_joiners,
       SUM(a.leavers) OVER (PARTITION BY a.division, a.function ORDER BY a.year, a.month)             AS closing_month_cumulative_leavers,
       opening_month_cumulative_joiners - opening_month_cumulative_leavers                            AS opening_month_employed,
       closing_month_cumulative_joiners - closing_month_cumulative_leavers                            AS closing_month_employed
FROM agg a
ORDER BY 1, 2;


SELECT *
FROM scratch.robinpatel.attrition_data ad
WHERE TO_DATE(ad.start_date, 'dd/MM/yyyy') < '2013-01-01'