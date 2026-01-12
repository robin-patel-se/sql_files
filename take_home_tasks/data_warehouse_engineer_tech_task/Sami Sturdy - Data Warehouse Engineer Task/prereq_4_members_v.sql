CREATE OR REPLACE VIEW sami_sturdy.members_v AS (
	WITH identify_duplicates AS (
		SELECT
			id,
			member_id,
			sign_up_date,
			last_updated,
			original_territory,
			current_territory,
			schedule_tstamp,
			extracted_at,
			/*
			Partitioning data by member_id, then assigning a row number
			to identify duplicate rows for a given member_id.
			This may be caused by both changes for a given member, such as 
			changes to current_territory, and/or the same data being extracted multiple times.
			The row with the latest value for schedule_tstamp and
			then extracted_at will be kept. This will remove duplicates
			and keep only the latest data for the given member.
			*/
			ROW_NUMBER() OVER(
				PARTITION BY 
					member_id
				ORDER BY 
					schedule_tstamp DESC, 
					extracted_at DESC
			) AS occurence_number
		FROM sami_sturdy.members_1
	)

	SELECT 
		id,
		member_id,
		sign_up_date,
		last_updated,
		original_territory,
		current_territory,
		schedule_tstamp,
		extracted_at,
		/*
		Member age is defined as the difference between the member's sign up date
		and the current date. This logic finds the age of the member in days and
		then groups the user into buckets of 1 year intervals, except for the 5+ year
		bucket which groups all users older than 5 years.
		
		It's worth noting that although this dataset will only fall into the '2 Years'
		and '3 Years' buckets, yearly intervals seem sensible for this use case,
		hence this implementation. For simplicity, 1 year is considered to be 365 days.
		*/
		CASE 
			WHEN CURRENT_DATE - sign_up_date::DATE < 0 OR sign_up_date IS NULL THEN 'Unknown'
			WHEN CURRENT_DATE - sign_up_date::DATE < 365 THEN '<1 Year'
			WHEN CURRENT_DATE - sign_up_date::DATE < 730 THEN '1 Year'
			WHEN CURRENT_DATE - sign_up_date::DATE < 1095 THEN '2 Years'
			WHEN CURRENT_DATE - sign_up_date::DATE < 1460 THEN '3 Years'
			WHEN CURRENT_DATE - sign_up_date::DATE < 1825 THEN '4 Years'
			ELSE '5+ Years'
		END AS member_age
	FROM identify_duplicates
	--Filters out the duplicate/unwanted rows identified in the previous step
	WHERE occurence_number = 1
)