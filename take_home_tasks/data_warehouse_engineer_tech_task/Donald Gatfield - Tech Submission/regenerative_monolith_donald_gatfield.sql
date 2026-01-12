/****************************************************************************************************************************
Developed on Postgres: 11.7

Title: regenerative_monolith_donald_gatfield.sql
Author: Donald Gatfield
About:  Take Home Challenge
Date: 11th May 2021
Notes (see also in-line comments and potential points of discussion):
	i) Assumes a database "monolith" has first been created by the user e.g.: createdb monolith
	ii) Please manually set the Feed*.csv locations in the script below to match your filesystem directory (cavet: a production offering would be parameterised/modularised) 
	iii) Run the script: psql -d monolith < regenerative_monolith_donald_gatfield.sql
	iv) As per the challenge requirements, the script is a single regenerative monolith (cavet: a production offering would be further modularised)
****************************************************************************************************************************/

/*************************************************************************************************************************************
Start of loading section
*************************************************************************************************************************************/

DROP TABLE IF EXISTS members CASCADE
;
CREATE UNLOGGED TABLE members
(
	id SERIAL NOT NULL CONSTRAINT members_pkey PRIMARY KEY,
	member_id VARCHAR(32),
	sign_up_date TIMESTAMP,
	last_updated TIMESTAMP,
	original_territory VARCHAR(2),
	current_territory VARCHAR(2),
	schedule_tstamp TIMESTAMP,
	extracted_at TIMESTAMP
)
;

COPY members(id, member_id, sign_up_date, last_updated, original_territory, current_territory, schedule_tstamp, extracted_at)
FROM '/var/lib/pgsql/Feed A - Members.csv'
DELIMITER ','
CSV HEADER
;

CREATE INDEX idx_members_member_id_extracted_at_sign_up_date ON members(member_id, extracted_at, sign_up_date)
;

DROP TABLE IF EXISTS bookings CASCADE
;
CREATE UNLOGGED TABLE bookings
(
	id SERIAL NOT NULL CONSTRAINT bookings_pkey PRIMARY KEY,
	member_id varchar(32),
	booking_id varchar(32),
	booking_date timestamp,
	last_updated timestamp,
	booking_status varchar(16),
	schedule_tstamp timestamp,
	extracted_at timestamp
)
;

COPY bookings(id, member_id, booking_id, booking_date, last_updated, booking_status, schedule_tstamp, extracted_at)
FROM '/var/lib/pgsql/Feed B - Bookings.csv'
DELIMITER ','
CSV HEADER
;

CREATE INDEX idx_bookings_member_id_booking_date ON bookings(member_id, booking_date)
;

DROP TABLE IF EXISTS events CASCADE
;
CREATE UNLOGGED TABLE events
(
	id SERIAL NOT NULL CONSTRAINT events_pkey PRIMARY KEY,
	territory varchar(2),
	cookie_id varchar(36),
	member_id varchar(32),
	booking_id varchar(32),
	event_name varchar(11),
	event_tstamp timestamp,
	page_urlpath text,
	schedule_tstamp timestamp,
	extracted_at timestamp
)
;

COPY events(id, territory, cookie_id, member_id, booking_id, event_name, event_tstamp, page_urlpath, schedule_tstamp, extracted_at)
FROM '/var/lib/pgsql/Feed C - Events.csv'
DELIMITER ','
CSV HEADER
;

CREATE INDEX idx_events_cookie_id_event_tstamp ON events(cookie_id, event_tstamp)
;
CREATE INDEX idx_events_member_id ON events(member_id)
;

/*************************************************************************************************************************************
Start of questions section
*************************************************************************************************************************************/

-- Sessions
-- Logic used:
-- An event (session) participant occurs within 30 minutes of its prior event (matching across cookie_id, event_tstamp)
-- An unbroken chain of such events forms a session

-- Discuss: also the intimation that cookie_id is unique per device, but member_id is consistent across devices providing a further view along a cross-device journey
DROP TABLE IF EXISTS session_identifier CASCADE;
CREATE UNLOGGED TABLE session_identifier
AS
WITH event_lags AS (
SELECT
        member_id,
        cookie_id,
        event_tstamp,
        event_tstamp - INTERVAL '30 min' AS event_tstamp_minus_thirty_mins,
        LAG(event_tstamp, 1) OVER (PARTITION BY cookie_id ORDER BY event_tstamp) AS event_prior,
        CASE
                WHEN LAG(event_tstamp, 1) OVER (PARTITION BY cookie_id ORDER BY event_tstamp) BETWEEN event_tstamp - INTERVAL '30 min' AND event_tstamp THEN 0
                ELSE 1
        END AS start_of_new_session_flag,
        page_urlpath
FROM events
ORDER BY
        cookie_id ASC,
        event_tstamp ASC
)
SELECT
	member_id,
        cookie_id,              
        event_tstamp,
	event_tstamp_minus_thirty_mins,
	event_prior,
	start_of_new_session_flag,
        SUM(start_of_new_session_flag) OVER (ORDER BY cookie_id, event_tstamp) AS master_session_identifier,
	page_urlpath
FROM event_lags
;

CREATE INDEX idx_session_identifier_master_session_identifier_covering ON session_identifier(master_session_identifier, event_tstamp, page_urlpath)
;

CREATE INDEX idx_session_identifier_session_identifier_member_id ON session_identifier(member_id)
;

/*
Discuss - there are some NULL member_ids
See:

SELECT COUNT(*) FROM events WHERE member_id IS NULL;
 count
-------
 10000
(1 row)


Discuss - events are not unique across - member_id, cookie_id, event_tstamp, page_urlpath
See:

SELECT * FROM  events WHERE member_id = '1a80ad64fb820def230974b0e0f22194' AND cookie_id = '810d8b25-587f-4400-90a2-641ad1b61cd4' AND event_tstamp = '2018-04-09 09:46:18.998';
   id   | territory |              cookie_id               |            member_id             | booking_id | event_name |      event_tstamp       |    page_urlpath    |   schedule_tstamp   |      extracted_at
--------+-----------+--------------------------------------+----------------------------------+------------+------------+-------------------------+--------------------+---------------------+-------------------------
 418739 | DE        | 810d8b25-587f-4400-90a2-641ad1b61cd4 | 1a80ad64fb820def230974b0e0f22194 |            | page_view  | 2018-04-09 09:46:18.998 | /aktuelle-angebote | 2018-04-09 09:00:00 | 2018-09-27 14:31:00.162
 931610 | DE        | 810d8b25-587f-4400-90a2-641ad1b61cd4 | 1a80ad64fb820def230974b0e0f22194 |            | page_view  | 2018-04-09 09:46:18.998 | /aktuelle-angebote | 2018-04-09 09:00:00 | 2018-09-27 14:31:00.162
*/



/*************************************************************************************************************************************
-- Question 1 - Top 10 session landing pages
        Discuss:
                i) The logic used for "landing page" - being the first page visited in (at the start of) each session
                ii) Therefore, the ratio of "landing page" to distinct session should be 1:1
*************************************************************************************************************************************/

-- encapsulate the question logic in a view (i.e. if this were part of a wider system)
DROP VIEW IF EXISTS top_ten_session_landing_pages;
CREATE VIEW top_ten_session_landing_pages
AS
SELECT 
	session_identifier.page_urlpath AS session_first_landing_page,
	COUNT(session_identifier.page_urlpath) AS session_count_of_first_landing_page,
	CASE 
		WHEN COUNT(session_identifier.page_urlpath) = COUNT(session_identifier.master_session_identifier) THEN TRUE
		ELSE FALSE
	END AS session_to_landing_page_ratio_is_good
FROM 
(
	-- landing page = "first event in session" - discuss
	SELECT
		master_session_identifier,
		MIN(event_tstamp) AS first_event
	FROM	session_identifier
	GROUP BY
		master_session_identifier
) landing_event_in_session
JOIN session_identifier
ON session_identifier.master_session_identifier = landing_event_in_session.master_session_identifier
AND session_identifier.event_tstamp = landing_event_in_session.first_event
GROUP BY session_identifier.page_urlpath
ORDER BY COUNT(session_identifier.page_urlpath) DESC
LIMIT 10
;

--  EXPLAIN SELECT * FROM top_ten_session_landing_pages;

SELECT 'Question 1:' AS Q;

SELECT * FROM top_ten_session_landing_pages
;

/*
Results:
 session_first_landing_page | session_count_of_first_landing_page | session_to_landing_page_ratio_is_good
----------------------------+-------------------------------------+---------------------------------------
 /current-sales             |                               85918 | t
 /your-subscriptions        |                                2690 | t
 /aktuelle-angebote         |                                2517 | t
 /search/search             |                                1872 | t
 /sale/currentSales         |                                1525 | t
 /offerte-in-corso          |                                1493 | t
 /                          |                                1435 | t
 /search/mbSearch/mbSearch  |                                1308 | t
 /your-account              |                                 721 | t
 /spa/filter                |                                 605 | t
*/



/*************************************************************************************************************************************
-- Question 2 - Session conversion rate
	Discuss:
		i) I have interpreted this - the percentage of sessions identified in Question 1 (matched to bookings), that have a corresponding booking status of "COMPLETE"
		ii) Sessions match to bookings on member_id and where the booking_date is between the first and last event_tstamp of the matched session 
		iii) I can see that using my logic a session can have >1 booking (e.g. booking_id: 39678, 39679 et. al.)		
*************************************************************************************************************************************/

-- encapsulate the question logic in a view (i.e. if this were part of a wider system)
DROP VIEW IF EXISTS session_events_first_last;
CREATE VIEW session_events_first_last
AS
SELECT
        session_members.member_id,
        session_range.first_event,
        session_range.last_event,
        session_range.master_session_identifier
FROM
(
        SELECT
                master_session_identifier,
                MIN(event_tstamp) AS first_event,
                MAX(event_tstamp) AS last_event
        FROM session_identifier
        GROUP BY
                master_session_identifier
        ) session_range
        INNER JOIN
        (
        SELECT
        DISTINCT
                member_id,
                master_session_identifier
        FROM session_identifier
        ) session_members
ON session_members.master_session_identifier = session_range.master_session_identifier
;
-- EXPLAIN SELECT * FROM sessions;

-- encapsulate the question logic in a view (i.e. if this were part of a wider system)
DROP VIEW IF EXISTS session_conversion_rate;
CREATE VIEW session_conversion_rate
AS
SELECT
	SUM(1) AS number_of_distinct_sessions,
	SUM(CASE 
		WHEN complete_bookings.member_id IS NULL THEN 0 
		ELSE 1 
	END) AS sessions_with_matched_booking,
	SUM(CASE 
		WHEN complete_bookings.member_id IS NULL THEN 1 
		ELSE 0 
	END) AS session_without_matched_booking,
        CAST(100.0 - ABS(((SUM(CASE 
					WHEN complete_bookings.member_id IS NULL THEN 0.0 
					ELSE 1.0 
				END) - SUM(1.0)) / SUM(1.0)) * 100.0) AS DECIMAL(3,2)) 
	AS session_to_matched_boooking_conversion_rate,
        CONCAT(CAST(CAST(100.0 - ABS(((SUM(CASE 
						WHEN complete_bookings.member_id IS NULL THEN 0.0 
						ELSE 1.0 
					END) - SUM(1.0)) / SUM(1.0)) * 100.0) AS DECIMAL(3,2)) AS VARCHAR(6)),'%') 
	AS expressed_as_text
FROM session_events_first_last AS sessions
LEFT JOIN bookings AS complete_bookings
ON sessions.member_id = complete_bookings.member_id
AND complete_bookings.booking_date BETWEEN sessions.first_event AND sessions.last_event
AND complete_bookings.booking_status = 'COMPLETE'
;

-- EXPLAIN SELECT * FROM session_conversion_rate;

SELECT 'Question 2:' AS Q;

SELECT * FROM session_conversion_rate
;

/*
Results:
 number_of_distinct_sessions | sessions_with_matched_booking | session_without_matched_booking | session_to_matched_boooking_conversion_rate | expressed_as_text
-----------------------------+-------------------------------+---------------------------------+---------------------------------------------+-------------------
                      225538 |                          1701 |                          223837 |                                        0.75 | 0.75%
*/


/*************************************************************************************************************************************
Question 3 - Of our member sessions, count of sessions and avg number of events by buckets of member age (age they have been a member)

Discuss:
	i) Taking MAX(extracted_at) as the lastest member record
	ii) Using my "session" logic from Question 1 (COUNT DISTINCT of master_session_identifier gives sessions across the set)
	iii) I chose to use "months" for the age bucket (if I had to pick just one)
		a) Year, examining the data - this seemed like too coarse a grain to derive insight
		a) Days, examining the data - this seemed like too fine a grain (although the end-user could optionally convert to a courser grain to suit)


-- Discuss - For e.g. the record below, I cannot see a change in the payload (schedule_tstamp maps to Last Modified in source)
SELECT * FROM members WHERE member_id = '000557597294aaa08705994d3eb5cfef';
  id   |            member_id             |    sign_up_date     |    last_updated     | original_territory | current_territory |   schedule_tstamp   |    extracted_at
-------+----------------------------------+---------------------+---------------------+--------------------+-------------------+---------------------+---------------------
 31303 | 000557597294aaa08705994d3eb5cfef | 2018-06-01 04:48:01 | 2018-06-01 04:48:01 | DE                 | DE                | 2018-08-05 00:00:00 | 2018-08-06 18:42:43
 31304 | 000557597294aaa08705994d3eb5cfef | 2018-06-01 04:48:01 | 2018-06-01 04:48:01 | DE                 | DE                | 2018-11-02 00:00:00 | 2018-11-14 13:38:42
*************************************************************************************************************************************/

-- encapsulate the question logic in a view (i.e. if this were part of a wider system)
DROP VIEW IF EXISTS member_sessions_event_age_buckets;
CREATE VIEW member_sessions_event_age_buckets
AS
SELECT
	COUNT(DISTINCT master_session_identifier) AS count_of_member_sessions,
	(DATE_PART('year', NOW()) - DATE_PART('year', members.sign_up_date)) * 12 + (DATE_PART('month', NOW()) - DATE_PART('month', members.sign_up_date)) AS member_age_months,
	CAST(AVG(number_of_events.number_of_events) AS INTEGER) AS avg_number_of_events
FROM session_identifier AS sessions
INNER JOIN (
	-- logic for latest member record
	SELECT 
		MAX(extracted_at) AS extracted_at,
		member_id
	FROM members
	GROUP BY member_id
) distinct_members
INNER JOIN members
ON members.member_id = distinct_members.member_id
AND members.extracted_at = distinct_members.extracted_at
ON members.member_id = sessions.member_id
INNER JOIN (
	-- logic for "events": the number of event records per member_id
	SELECT 
		member_id,
		COUNT(member_id) AS number_of_events
	FROM events
	GROUP BY member_id
) number_of_events
ON number_of_events.member_id = members.member_id
GROUP BY (DATE_PART('year', NOW()) - DATE_PART('year', members.sign_up_date)) * 12 + (DATE_PART('month', NOW()) - DATE_PART('month', members.sign_up_date))
;

SELECT 'Question 3:' AS Q;

-- EXPLAIN SELECT * FROM member_sessions_event_age_buckets;
SELECT * FROM member_sessions_event_age_buckets
;

/*
Results:
 count_of_member_sessions | member_age_months | avg_number_of_events
--------------------------+-------------------+----------------------
                     8474 |                28 |                   47
                     4739 |                29 |                   82
                     6672 |                30 |                   82
                     8058 |                31 |                  105
                     9047 |                32 |                  129
                    15638 |                33 |                  122
                    20552 |                34 |                  261
                    14843 |                35 |                  135
                    17260 |                36 |                  194
                    17140 |                37 |                  221
                    20970 |                38 |                  321
                    26379 |                39 |                  252
                    44995 |                40 |                  279
*/


/*
If you had more time (1-2 sentences for each): 

Q. How would you thoroughly test your modelling and queries to ensure faith in the resulting data? 

A. The design of the system can assist with testing. Arguably, if a design consists of smaller/decomposed objects it is easier to understand, reason about and test. Correspondingly, these smaller components can be tested in isolation. These components are then integrated to make up the wider system.

Q. How would you adjust your approach for incremental processing

A. For example, we can delta load the changes to existing data, and load brand new data. Discuss staging data, different approaches, SCD2 etc.

Q. Optional question: Assuming that many of our users have multiple devices and some users will share their devices with other family members, how would you use the cookie_id field in conjunction with the member_id field to improve our understanding of customer journeys?

A. The intimation is that cookie_id is unique per device, but member_id is consistent across devices, providing a further view along a cross-device journey (i.e. member_id could form part of an expanded session logic) i.e. if the member_id is the same, but the cookie is different (but not expired/new/within certain timeframe) we have a different device in use? (discuss)
*/
