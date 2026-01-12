/*
Session conversion rate is being treated as the proportion of total sessions
which have resulted in at least 1 complete booking. If a single session results
in multiple complete bookings, it will still be treated as a single conversion.

This query does not account for edge cases in which a single booking ID is tied
to multiple unique sessions. Handling these cases could be a potential goal if
given more time.
*/

WITH identify_complete_bookings AS (
	SELECT 
	se.global_session_id,
	b.booking_id,
	--Assigns rows a value of 1 if the given row is associated with a complete booking.
	CASE
		WHEN b.booking_status = 'COMPLETE' THEN 1
		ELSE 0
	END AS complete_booking
	FROM sami_sturdy.sessionised_events_v se
	/*
	Left joining sessionised_events with the deduped bookings view.
	Left join is used over inner join since sessions without bookings
	are relevant to this calculation.
	*/
	LEFT JOIN sami_sturdy.bookings_v b
		ON se.booking_id = b.booking_id
	/*
	Groups rows by global_session_id and booking_id. This will result in rows with NULL booking_ids for
	sessions which contain page_view events.
	*/
	GROUP BY 1,2,3
),

/*
Groups by global_session_id only, resulting in a single row for every unique global_session_id.
The MAX aggregation will assign each row a value of 1 if and only if the given session contains
an association with at least one booking with a COMPLETE status.
*/
identify_converted_sessions AS (
	SELECT 
		global_session_id,
		MAX(complete_booking) converted_session
	FROM identify_complete_bookings
	GROUP BY 1
)

SELECT
	/*
	Divides the number of sessions which contain an assocation with at least one complete booking
	by the total number of sessions and multiplies by 100 to show the rate as a percentage.
	*/
       SUM(converted_session)::FLOAT,
       COUNT(DISTINCT global_session_id)::FLOAT,
	(SUM(converted_session)::FLOAT/COUNT(DISTINCT global_session_id)::FLOAT) * 100 AS session_conversion_percentage
FROM identify_converted_sessions;


