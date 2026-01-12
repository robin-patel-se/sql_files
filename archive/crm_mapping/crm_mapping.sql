USE WAREHOUSE pipe_large;

SELECT j.send_id,
       j.subject,
       j.email_name,
       s.list_id,
       count(*) AS sends
FROM raw_vault_mvp.sfmc.jobs_list j
         LEFT JOIN raw_vault_mvp.sfmc.events_sends s ON j.send_id = s.send_id
WHERE j.send_id = 1135092
GROUP BY 1, 2, 3, 4;

USE WAREHOUSE pipe_xlarge;

SELECT s.list_id, count(*)
FROM raw_vault_mvp.sfmc.events_sends s
GROUP BY 1;

SELECT datasources.source_name,
       count(*)
FROM raw_vault_mvp.sfmc.datasources
GROUP BY 1
HAVING COUNT(*) > 1;


SELECT send_id,
       count(*)
FROM raw_vault_mvp.sfmc.jobs_list
GROUP BY 1
HAVING COUNT(*) > 1;

SELECT jl.email_name,
       d.source_name,
       count(*)
FROM raw_vault_mvp.sfmc.jobs_list jl
         LEFT JOIN raw_vault_mvp.sfmc.datasources d ON jl.send_id = d.send_id
WHERE jl.send_id = 1135092
GROUP BY 1, 2;

SELECT *
FROM raw_vault_mvp.sfmc.events_sends es
         LEFT JOIN raw_vault_mvp.sfmc.jobs_list jl ON es.send_id = jl.send_id
WHERE jl.send_id = 1135092

-- sendid + subscriber key is unique for each email send
-- opens and clicks can be linked back to a source via the send_id and subscriber key

--ollie will provide a send id to compare send event numbers to check that aggregations up to list id match what we expect for source name


SELECT j.send_id,
       j.subject,
       j.email_name,
       s.list_id,
       count(*) AS sends
FROM raw_vault_mvp.sfmc.jobs_list j
         LEFT JOIN raw_vault_mvp.sfmc.events_sends s ON j.send_id = s.send_id
WHERE j.send_id = 1147191
GROUP BY 1, 2, 3, 4;

------------------------------------------------------------------------------------------------------------------------
--provide ollie out of:
--email name, job id, list id, sends

--name has created a file

GRANT SELECT ON TABLE collab.my_schema.my_table TO ROLE personal_role__gianniraftis;

USE WAREHOUSE pipe_xlarge;
SELECT j.send_id,
       j.subject,
       j.email_name,
       s.list_id,
       count(*) AS sends
FROM raw_vault_mvp.sfmc.jobs_list j
         LEFT JOIN raw_vault_mvp.sfmc.events_sends s ON j.send_id = s.send_id
-- WHERE j.send_id = 1135092
WHERE j.sched_time >= '2019-01-01'
GROUP BY 1, 2, 3, 4
ORDER BY 1, 3, 4;

--check new job sources feed combined with sends to see if we can get data source name
SELECT j.send_id,
       j.subject,
       j.email_name,
       s.list_id,
--        js.datasourcename,
       count(*) AS sends
FROM raw_vault_mvp.sfmc.jobs_list j
         LEFT JOIN raw_vault_mvp.sfmc.events_sends s ON j.send_id = s.send_id
--          LEFT JOIN raw_vault_mvp.sfmc.jobs_sources js ON s.send_id = js.jobid AND s.subscriber_key = js.subscriberkey
WHERE j.send_id = 1160337 --searched job list for a send that was recent
GROUP BY 1, 2, 3, 4;

USE WAREHOUSE pipe_xlarge;


SELECT min(j.sched_time)
FROM raw_vault_mvp.sfmc.jobs_sources js
         INNER JOIN raw_vault_mvp.sfmc.events_sends s ON s.send_id = js.jobid AND s.subscriber_key = js.subscriberkey
         INNER JOIN raw_vault_mvp.sfmc.jobs_list j ON j.send_id = s.send_id