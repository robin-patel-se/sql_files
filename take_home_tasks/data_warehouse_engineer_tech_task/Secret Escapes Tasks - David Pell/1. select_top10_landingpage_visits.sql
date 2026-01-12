select
	page_urlpath,
	count(*) landingpage_visits
from (
SELECT user_id,
       occurred_at,
		last_event,
       SUM(is_new_session) OVER (ORDER BY user_id, occurred_at) AS global_session_id,
       SUM(is_new_session) OVER (PARTITION BY user_id ORDER BY occurred_at) AS user_session_id,
	   is_new_session,
	   page_urlpath
      FROM (
        SELECT *,
              CASE WHEN EXTRACT('EPOCH' FROM occurred_at) - EXTRACT('EPOCH' FROM last_event) >= (60 * 30) 
                     OR last_event IS NULL 
                   THEN 1 ELSE 0 END AS is_new_session
         FROM (
              SELECT member_id as user_id,
                     event_tstamp as occurred_at,
                     LAG(event_tstamp,1) OVER (PARTITION BY member_id ORDER BY event_tstamp) AS last_event,
			 		page_urlpath
                FROM david_pell.events
              ) last
       ) final
	where is_new_session = 1
--     LIMIT 1000
) as sessions
group by page_urlpath
order by landingpage_visits desc
limit 10;
