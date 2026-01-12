SELECT
	--Coalesce to assign events with NULL member_id into an unknown bucket 'Unknown'
	COALESCE(m.member_age, 'Unknown') AS member_age,
	--Counting the distinct global session ids provides a count of sessions for each age bucket
	COUNT(DISTINCT se.global_session_id) AS session_count,
	--Count * provides the total number of events for each age bucket
	COUNT(*) AS total_events,
	--Total Events / Session Count provides the average number of events per session for each age bucket
	ROUND(COUNT(*)::NUMERIC/COUNT(DISTINCT se.global_session_id)::NUMERIC,3) AS avg_events_per_session
FROM sessionised_events_v se
/*
Left joins with the members view which has deduped and assigned age buckets to the members data.
A left join is used rather than an inner join since event data from users which haven't signed up
has NULL values for member_id, and that data is still relevant to this output.
*/
LEFT JOIN members_v m
	ON se.member_id = m.member_id
--Grouping by the age buckets to calculate session count and average events
GROUP BY 1