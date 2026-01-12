CREATE OR REPLACE VIEW sami_sturdy.sessionised_events_v AS (
	/*
	An assumption is made that a session is considered to be events made within 30 minutes of each other
	and attributed to both the same cookie_id and member_id, or just the same cookie_id in cases where
	member_id is NULL due to the user not yet being signed up and/or logged in.

	An argument could be made for including events with differing cookie_ids in the same session if they
	have the same member_id and are within the session timeout, caused by a user switching devices for example,
	but for simplicity this will not be implemented here.

	CTEs are used here to simplify the multi-step process of removing duplicates and then sessionising the event data.
	*/

	WITH identify_duplicates AS (
		SELECT
			id,
			territory,
			cookie_id,
			member_id,
			booking_id,
			event_name,
			event_tstamp,
			page_urlpath,
			schedule_tstamp,
			extracted_at,
			/*
			Partitioning data by all columns except id, extracted_date,
			and schedule_tstamp, then assigning a row number to identify
			duplicates which have been extracted multiple times. 
			When duplicates are found, the row with the latest extracted_at is kept.
			*/
			ROW_NUMBER() OVER(
				PARTITION BY
					territory,
					cookie_id,
					member_id,
					booking_id,
					event_name,
					event_tstamp,
					page_urlpath
				ORDER BY 
					extracted_at DESC
			) AS occurence_number
		FROM sami_sturdy.events_1
	),

	generate_last_event_tstamp AS (
		SELECT 
			id,
			territory,
			cookie_id,
			member_id,
			booking_id,
			event_name,
			event_tstamp,
			page_urlpath,
			schedule_tstamp,
			extracted_at,
			LAG(event_tstamp, 1) OVER(PARTITION BY cookie_id, member_id ORDER BY event_tstamp) AS last_event_tstamp
		FROM identify_duplicates
		--Filters out the duplicate rows identified in the previous step
		WHERE occurence_number = 1
	),

	identify_first_events_of_sessions AS (
		SELECT 
			id,
			territory,
			cookie_id,
			member_id,
			booking_id,
			event_name,
			event_tstamp,
			page_urlpath,
			schedule_tstamp,
			extracted_at,
			/*
			If the difference in seconds between the current event and the last event
			for the given cookie_id and member_id is greater than 1800 seconds (30 minutes)
			OR there is no last event timestamp THEN 1 ELSE 0.
			*/
			CASE 
				WHEN EXTRACT(EPOCH FROM (event_tstamp - last_event_tstamp)) < 1800 THEN 0 
				ELSE 1 
			END AS first_event_of_session
		FROM generate_last_event_tstamp
	)

	SELECT
		id,
		territory,
		cookie_id,
		member_id,
		booking_id,
		event_name,
		event_tstamp,
		page_urlpath,
		schedule_tstamp,
		extracted_at,
		/*
		Given that all events which are the start of a new session have a first_event_of_session value of 1,
		each row is assigned a global_session_id value which is the sum of the values of first_event_of_session
		of all preceding rows up to the current row. This effectively groups all events into globally unique session IDs.
		*/
		SUM(first_event_of_session) OVER (ORDER BY cookie_id, member_id, event_tstamp) AS global_session_id,
		/*
		Logic is as with global_session_id, except the sum is applied only to each given combination of cookie_id
		and member_id so it is easy to see how many sessions have occured for a given cookie_id and member_id.
		The fact that the user_session_id will treat a given member as a different member if using different devices 
		(and thus having different cookie_ids) is a shortcoming of this current implementation and a potential goal
		for improvement if given more time.
		*/
		SUM(first_event_of_session) OVER (PARTITION BY cookie_id, member_id ORDER BY event_tstamp) AS user_session_id
	FROM identify_first_events_of_sessions
);

SELECT * FROM sami_sturdy.sessionised_events_v sev;

SELECT COUNT(distinct sev.global_session_id) from sami_sturdy.sessionised_events_v sev