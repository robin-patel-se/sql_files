
select
	MAX(global_session_id) total sessions
from (
	SELECT
			*,
		   SUM(is_new_session) OVER (ORDER BY user_id, occurred_at) AS global_session_id,
		   SUM(is_new_session) OVER (PARTITION BY user_id ORDER BY occurred_at) AS user_session_id
		  FROM (
			SELECT *,
				  CASE WHEN EXTRACT('EPOCH' FROM occurred_at) - EXTRACT('EPOCH' FROM last_event) >= (60 * 30) 
						 OR last_event IS NULL 
					   THEN 1 ELSE 0 END AS is_new_session,
					--minutes_past column isnt needed but its useful to show the duration since the last event
				  (EXTRACT('EPOCH' FROM occurred_at) - EXTRACT('EPOCH' FROM last_event))/60 as minutes_past
			 FROM (
				  SELECT member_id as user_id,
						 event_tstamp as occurred_at,
						 LAG(event_tstamp,1) OVER (PARTITION BY member_id ORDER BY event_tstamp) AS last_event,
						 booking_id
					FROM events
				  ) last
		   ) final
	--     LIMIT 1000
		) as sessions