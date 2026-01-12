SELECT es.event_tstamp::DATE as date,
       count(*) AS reset_pw_sends
FROM se.data_pii.crm_events_sends es
         INNER JOIN se.data_pii.crm_jobs_list jl ON es.send_id = jl.send_id
WHERE LOWER(jl.email_name) LIKE '%password%'
AND es.event_tstamp >= '2020-01-01'
GROUP BY 1;