CREATE TABLE sami_sturdy.members (
	id serial NOT NULL CONSTRAINT members_pkey PRIMARY KEY,
	member_id VARCHAR(32),
	sign_up_date TIMESTAMP,
	last_updated TIMESTAMP,
	original_territory VARCHAR(2),
	current_territory VARCHAR(2),
	schedule_tstamp TIMESTAMP,
	extracted_at TIMESTAMP
	);

CREATE TABLE sami_sturdy.bookings (
	id serial NOT NULL CONSTRAINT bookings_pkey PRIMARY KEY,
	member_id VARCHAR(32),
	booking_id VARCHAR(32),
	booking_date TIMESTAMP,
	last_updated TIMESTAMP,
	booking_status VARCHAR(16),
	schedule_tstamp TIMESTAMP,
	extracted_at TIMESTAMP
	);

CREATE TABLE sami_sturdy.events (
	id serial NOT NULL CONSTRAINT events_pkey PRIMARY KEY,
	territory VARCHAR(2),
	cookie_id VARCHAR(36),
	member_id VARCHAR(32),
	booking_id VARCHAR(32),
	event_name VARCHAR(11),
	event_tstamp TIMESTAMP,
	page_urlpath TEXT,
	schedule_tstamp TIMESTAMP,
	extracted_at TIMESTAMP
	);
