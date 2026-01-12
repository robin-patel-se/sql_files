CREATE OR REPLACE VIEW sami_sturdy.bookings_v AS (
	WITH identify_duplicates AS (
		SELECT
			id,
			member_id,
			booking_id,
			booking_date,
			last_updated,
			booking_status,
			schedule_tstamp,
			extracted_at,
			/*
			Partitioning data by booking_id, then assigning a row number
			to identify duplicate rows for a given booking_id.
			This may be caused by both status changes for a booking
			and/or the same data being extracted multiple times.
			The row with the latest value for schedule_tstamp and
			then extracted_at will be kept. This will remove duplicates
			and keep only the latest status for a given booking_id.
			*/
			ROW_NUMBER() OVER(
				PARTITION BY 
					booking_id
				ORDER BY 
					schedule_tstamp DESC, 
					extracted_at DESC
			) AS occurence_number
		FROM sami_sturdy.bookings_1
	)

	SELECT 
		id,
		member_id,
		booking_id,
		booking_date,
		last_updated,
		booking_status,
		schedule_tstamp,
		extracted_at
	FROM identify_duplicates
	--Filters out the duplicate/unwanted rows identified in the previous step
	WHERE occurence_number = 1
);


SELECT * FROM sami_sturdy.bookings_v bv;