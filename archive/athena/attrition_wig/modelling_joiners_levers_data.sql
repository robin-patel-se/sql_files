SELECT *
FROM latest_vault.hr_gsheets.leavers l;
SELECT *
FROM latest_vault.hr_gsheets.joiners j;

SELECT *
FROM latest_vault.hr_gsheets.joiners;

-- create a user dataset
CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.hr_employee_list AS (
    WITH stack_data AS (
        SELECT j.employee_number,
               j.start_date AS event_date,
               j.start_date,
               NULL         AS leaving_date,
               j.job_role_department,
               j.job_role_sub_department,
               j.job_role_job_title,
               j.job_role_role_code,
               j.company_role,
               NULL         AS reason_for_leaving,
               'employed'   AS employment_status
        FROM latest_vault.hr_gsheets.joiners j
        UNION ALL
        SELECT l.employee_number,
               l.leaving_date AS event_date,
               NULL           AS start_date,
               l.leaving_date,
               l.job_role_department,
               l.job_role_sub_department,
               l.job_role_job_title,
               l.job_role_role_code,
               l.company_role,
               l.reason_for_leaving,
               'leaver'       AS employment_status
        FROM latest_vault.hr_gsheets.leavers l
    )
    SELECT DISTINCT
           sd.employee_number,
           IFF(LEFT(sd.employee_number, 3) = 'CON', TRUE, FALSE)                                                AS is_contractor,
           FIRST_VALUE(sd.start_date) OVER (PARTITION BY sd.employee_number ORDER BY sd.event_date)             AS start_date,
           LAST_VALUE(sd.leaving_date) OVER (PARTITION BY sd.employee_number ORDER BY sd.event_date)            AS leaving_date,
           LAST_VALUE(sd.job_role_department) OVER (PARTITION BY sd.employee_number ORDER BY sd.event_date)     AS department,
           LAST_VALUE(sd.job_role_sub_department) OVER (PARTITION BY sd.employee_number ORDER BY sd.event_date) AS sub_department,
           LAST_VALUE(sd.job_role_job_title) OVER (PARTITION BY sd.employee_number ORDER BY sd.event_date)      AS job_title,
           LAST_VALUE(sd.job_role_role_code) OVER (PARTITION BY sd.employee_number ORDER BY sd.event_date)      AS role_code,
           LAST_VALUE(sd.company_role) OVER (PARTITION BY sd.employee_number ORDER BY sd.event_date)            AS company_role,
           LAST_VALUE(sd.reason_for_leaving) OVER (PARTITION BY sd.employee_number ORDER BY sd.event_date)      AS reason_for_leaving,
           LAST_VALUE(sd.employment_status) OVER (PARTITION BY sd.employee_number ORDER BY sd.event_date)       AS employment_status
    FROM stack_data sd
);

SELECT GET_DDL('table', 'scratch.robinpatel.hr_employee_list');



SELECT *
FROM scratch.robinpatel.hr_employee_list;
SELECT DATE_TRUNC(MONTH, hr.start_date),
       COUNT(*) AS starters
FROM scratch.robinpatel.hr_employee_list hr
GROUP BY 1;

--need to create a user level grain of tennure that we enrich with user data

-- CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.hr_modelled_employee_movement AS (
--     WITH deps_and_sub_deps AS (
--         -- list used to explode calendar to make a grain table
--         SELECT DISTINCT hel.department, hel.sub_department, hel.company_role
--         FROM scratch.robinpatel.hr_employee_list hel
--     ),
--          grain AS (
--              SELECT DISTINCT
--                     DATE_TRUNC('month', sc.date_value) AS month,
--                     dp.department
--              FROM se.data.se_calendar sc
--                  LEFT JOIN deps_and_sub_deps dp
--              WHERE sc.date_value BETWEEN '2018-01-01' AND CURRENT_DATE
--          )
--             ,
--          joiners AS (
--              SELECT DATE_TRUNC('month', j.start_date) AS month,
--                     j.job_role_department             AS department,
--                     COUNT(*)                          AS joiners
--              FROM latest_vault.hr_gsheets.joiners j
--              WHERE LEFT(j.employee_number, 3) IS DISTINCT FROM 'CON' -- remove contractors
--              GROUP BY 1, 2
--          ),
--          leavers AS (
--
--              SELECT DATE_TRUNC('month', l.leaving_date) AS month,
--                     l.job_role_department               AS department,
--                     COUNT(*)                            AS leavers
--              FROM latest_vault.hr_gsheets.leavers l
--              WHERE LEFT(l.employee_number, 3) IS DISTINCT FROM 'CON' -- remove contractors
--              GROUP BY 1, 2
--          )
--
--     SELECT g.month,
--            g.department,
--            j.joiners,
--            l.leavers
--     FROM grain g
--         LEFT JOIN joiners j ON g.month = j.month AND g.department = j.department
--         LEFT JOIN leavers l ON g.month = l.month AND g.department = l.department
-- );


SELECT GET_DDL('table', 'scratch.robinpatel.hr_employees_pit');
CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.hr_employees_pit AS (
    SELECT sc.date_value,
           sc.year,
           sc.month,
           sc.week_start,
           hel.employee_number,
           hel.start_date,
           hel.leaving_date,
           sc.date_value = hel.start_date                           AS joiner,
           sc.date_value = COALESCE(hel.leaving_date, '1979-01-01') AS leaver
    FROM scratch.robinpatel.hr_employee_list hel
        LEFT JOIN data_vault_mvp.dwh.se_calendar sc ON sc.date_value BETWEEN hel.start_date AND COALESCE(hel.leaving_date, CURRENT_DATE)
);

WITH aggregate_numbers AS (
    SELECT DATE_TRUNC(MONTH, hep.date_value) AS month,
           hel.department,
           SUM(IFF(hep.joiner, 1, 0))        AS joiners,
           SUM(IFF(hep.leaver, 1, 0))        AS leavers
    FROM scratch.robinpatel.hr_employees_pit hep
        LEFT JOIN scratch.robinpatel.hr_employee_list hel ON hep.employee_number = hel.employee_number
    GROUP BY 1, 2
)
SELECT an.month,
       an.department,
       an.joiners,
       an.leavers,
       SUM(an.joiners) OVER (PARTITION BY an.department ORDER BY an.month) AS cumulative_joiners,
       SUM(an.leavers) OVER (PARTITION BY an.department ORDER BY an.month) AS cumulative_leavers,
       cumulative_joiners - cumulative_leavers                             AS headcount
FROM aggregate_numbers an
WHERE an.month IS NOT NULL
;


SELECT *
FROM scratch.robinpatel.hr_employee_list hel
WHERE hel.sub_department = 'Data';

SELECT *
FROM scratch.robinpatel.hr_employees_pit hep;

SELECT *
FROM scratch.robinpatel.hr_employees_pit;

SELECT hel.department,
       COUNT(*)
FROM scratch.robinpatel.hr_employee_list hel
WHERE hel.department IN ('Tech', 'Product', 'Data')
  AND hel.employment_status = 'employed'
GROUP BY 1;

-- calc for attrition is a (12 monthly cumulative leavers) / avg 12 monthly head count

SELECT *
FROM se.data.se_offers_inclusions_rates soir
WHERE soir.hotel_code = '001w000001KKhn3'

SELECT *
FROM data_vault_mvp.finance.stripe_cash_on_booking scob;

DESC TABLE data_vault_mvp_dev_robin.dwh.se_booking;


CREATE OR REPLACE TRANSIENT TABLE hr_employee_list
(
    employee_number    VARCHAR,
    is_contractor      BOOLEAN,
    start_date         DATE,
    leaving_date       DATE,
    department         VARCHAR,
    sub_department     VARCHAR,
    job_title          VARCHAR,
    role_code          VARCHAR,
    company_role       VARCHAR,
    reason_for_leaving VARCHAR,
    employment_status  VARCHAR
);


self_describing_task --include 'dv/dwh/hr/employee_list.py'  --method 'run' --start '2022-02-10 00:00:00' --end '2022-02-10 00:00:00'

CREATE SCHEMA latest_vault_dev_robin.hr_gsheets;
CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.hr_gsheets.leavers CLONE latest_vault.hr_gsheets.leavers;
CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.hr_gsheets.joiners CLONE latest_vault.hr_gsheets.joiners;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.employee_list;

CREATE OR REPLACE TRANSIENT TABLE hr_employees_pit
(
    date_value      DATE,
    year            NUMBER,
    month           NUMBER,
    week_start      DATE,
    employee_number VARCHAR,
    start_date      DATE,
    leaving_date    DATE,
    joiner          BOOLEAN,
    leaver          BOOLEAN
);

self_describing_task --include 'dv/dwh/hr/employees_pit.py'  --method 'run' --start '2022-02-10 00:00:00' --end '2022-02-10 00:00:00'

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.se_calendar AS SELECT * FROM data_vault_mvp.dwh.se_calendar;

SELECT * FROM data_vault_mvp_dev_robin.dwh.employees_pit;