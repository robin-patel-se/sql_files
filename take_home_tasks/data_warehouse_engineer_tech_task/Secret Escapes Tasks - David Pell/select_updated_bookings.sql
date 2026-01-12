SELECT 
	bookings.* 
FROM
	bookings
	inner join(
		SELECT 
		 BOOKING_ID, 
		 MAX(extracted_at) AS maxsign
		FROM bookings
		GROUP BY BOOKING_ID
	) most_recent ON bookings.BOOKING_ID = most_recent.BOOKING_ID
	and bookings.extracted_at = most_recent.maxsign