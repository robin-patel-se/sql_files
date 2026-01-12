select sum(booking_exists) complete_bookings,
	count(*) total_sessions,
	CAST(100*sum(booking_exists) AS FLOAT)/ CAST(count(*) AS FLOAT) conv_rate_percentage
	from (
select
	global_session_id,
	count(booking_status) booking_count,
	CASE WHEN count(booking_status) > 1
					   THEN 1 ELSE 0 END AS booking_exists
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
					FROM david_pell.events
				  ) last
		   ) final
		) as sessions
left join (
	SELECT
			bookings.booking_id,
			bookings.booking_status
		FROM david_pell.bookings
		inner join(
			SELECT
			 BOOKING_ID,
			 MAX(LAST_UPDATED) AS most_recent_update
			FROM david_pell.bookings
			GROUP BY BOOKING_ID
		) updated_bookings ON bookings.BOOKING_ID = updated_bookings.BOOKING_ID
			AND bookings.LAST_UPDATED = updated_bookings.most_recent_update
			AND bookings.booking_status = 'COMPLETE'
) as updated_bookings on sessions.booking_id = updated_bookings.booking_id
group by global_session_id
order by booking_exists desc
) as grouped_bookings