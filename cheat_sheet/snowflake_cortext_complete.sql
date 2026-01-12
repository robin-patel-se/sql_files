-- check that the model is conscious of Secret Escapes
SELECT
	snowflake.cortex.complete(
			'openai-gpt-4.1',
			'what is secret escapes?'
	) AS query

SELECT
	ds.se_sale_id,
	ds.sale_name,
	CONCAT(
			'Tell me with a "YES" or "NO" plus a 2 sentence reason why this product name listed on the online travel company Secret Escapes is good for conversions or not "',
			ds.sale_name, '"') AS prompt,
	snowflake.cortex.complete(
			'openai-gpt-4.1',
			prompt
	)                          AS chat_gpt_approval_of_sale_name
FROM se.data.dim_sale ds
WHERE ds.sale_active
LIMIT 100
;


USE WAREHOUSE pipe_xlarge
;

SELECT
	ssa.longitude,
	ssa.latitude,
	ssa.company_name,
	'give me 5 interesting activities near a hotel with the following details that would be enticing to a person looking at hotels for a holiday"' ||
	'\n' ||
	'hotel name: "' || ssa.sale_name || '"' ||
	'\n' ||
	'longitude: ' || ssa.longitude ||
	'\n' ||
	'latitude: ' || ssa.latitude ||
	'\n' ||
	'country: ' || ssa.posu_country ||
	'\n' ||
	'For each suggestion, include a brief description, the estimated distance, and the approximate travel time by walking. Present the information in an array.'
		AS prompt,
	snowflake.cortex.complete(
			'openai-gpt-4.1',
			prompt
	)   AS chat_gpt_approval_of_sale_name
FROM se.data.se_sale_attributes ssa
WHERE ssa.sale_active
LIMIT 20
;

USE WAREHOUSE pipe_xlarge
;

WITH
	countries AS (
		SELECT DISTINCT
			ds.posu_country
		FROM se.data.dim_sale ds
		WHERE ds.sale_active
		LIMIT 10
	)
SELECT
	countries.posu_country,
	'what is the best time of year to visit ' || countries.posu_country AS prompt,
	snowflake.cortex.complete(
			'openai-gpt-4.1',
			prompt
	)                                                                   AS chat_gpt_approval_of_sale_name
FROM countries
;


SELECT
	ssa.company_name,
	'Give me 5 good things and 5 bad things for this ' || company_name
		AS prompt,
	snowflake.cortex.complete(
			'openai-gpt-4.1',
			prompt
	)   AS chat_gpt_approval_of_sale_name
FROM se.data.se_sale_attributes ssa
WHERE ssa.sale_active
LIMIT 20


/*
Royal Garden Villas Luxury Hotel


Certainly! Here are **5 good things** and **5 bad things** commonly mentioned about the **Royal Garden Villas Luxury Hotel** (located in Costa Adeje, Tenerife):

---

### 5 Good Things

1. **Private Villas with Pools**
   Each villa offers a private heated pool, ensuring privacy and luxury for guests.

2. **Exceptional Service**
   Staff are frequently praised for their friendliness, professionalism, and attention to detail.

3. **Beautiful Decor and Ambience**
   The hotel features stunning Balinese-inspired decor, lush gardens, and a tranquil atmosphere.

4. **Gourmet Dining**
   The on-site restaurant, Jardín, is known for its high-quality cuisine and romantic setting.

5. **Peaceful and Secluded Location**
   The hotel is set away from the busy tourist areas, providing a quiet and relaxing environment.

---

### 5 Bad Things

1. **Distance from the Beach**
   The hotel is not within walking distance to the beach, requiring a taxi or car ride.

2. **Limited On-Site Facilities**
   Compared to larger resorts, there are fewer amenities (e.g., no large communal pool, limited entertainment).

3. **Expensive Rates**
   The luxury experience comes at a high price, making it less accessible for budget travelers.

4. **Hilly Location**
   The property is on a hill, which may be challenging for guests with mobility issues.

5. **Occasional Maintenance Issues**
   Some guests have reported minor maintenance problems in villas, such as pool heating or air conditioning.

---

If you have specific preferences or concerns, let me know and I can tailor the list further!
*/


SELECT
	'Give me the tripadvisor rating for Polurrian on the Lizard in the form of a 2 digit one decimal place number only'
		AS prompt,
	snowflake.cortex.complete(
			'openai-gpt-4.1',
			prompt
	)   AS chat_gpt_approval_of_sale_name
LIMIT 20
;



SELECT
	ssa.company_name,
	'Get me the booking.com rating score for "' || ssa.company_name || '" in ' || ssa.posu_country ||
	'provide the just the number'
		AS prompt,
	snowflake.cortex.complete(
			'openai-gpt-4.1',
			prompt
	)   AS chat_gpt_approval_of_sale_name
FROM se.data.se_sale_attributes ssa
WHERE ssa.sale_active
LIMIT 20
;


WITH
	active_hotels AS (
		SELECT DISTINCT
			ssa.hotel_code,
			ssa.company_name,
			ssa.posu_country
		FROM se.data.se_sale_attributes ssa
		WHERE ssa.sale_active
		LIMIT 10
	)
SELECT
	active_hotels.company_name,
	'Get me the booking.com rating score for "' || active_hotels.company_name || '" in ' ||
	active_hotels.posu_country ||
	'provide the answer in a 1 decimal number only eg. 8.2'
		AS booking_prompt,
	snowflake.cortex.complete(
			'openai-gpt-4.1',
			booking_prompt
	)   AS booking_com_rating,
	'Get me the tripadvisor.com rating score for "' || active_hotels.company_name || '" in ' ||
	active_hotels.posu_country ||
	'provide the answer in a 1 decimal number only eg. 8.2'
		AS tripadvisor_prompt,
	snowflake.cortex.complete(
			'openai-gpt-4.1',
			tripadvisor_prompt
	)   AS tripadvisor_rating,
	'From tripadvisor Pull me a maximum 5 word summary of this hotel "' || active_hotels.company_name
		AS tripadvisor_summary_prompt,
	snowflake.cortex.complete(
			'openai-gpt-4.1',
			tripadvisor_summary_prompt
	)   AS tripadvisor_summary
FROM active_hotels
;


