SELECT *
FROM se.data.user_booking_review ubr
;

SELECT
	ubr.review_date,
	ubr.customer_score,
	ubr.follow_up_answer,
	snowflake.cortex.summarize(ubr.follow_up_answer) AS follow_up_answer_summarised
FROM se.data.user_booking_review ubr
LIMIT 10
;

/*
Upon arriving in our room, we  found that we were one hand towel short and only one cup (mug ). I mentioned this to the
receptionist on our way out and said that i would pick them up upon our return as my wife was taking a shower before
having a nap. My sister said that she was also a hand towel short. when wee got back there was a glass and towel behind
the desk. I went to pick them up but was told that my towel had been delivered to the room, but not my sister's. The
small glass was described by the receptionist as a cup, definitely not a glass.I should have asked for a mug.
When I got the bill for the four of us the first night, the £120 meal discount was not shown. I pointed this out to the
restaurant manager, but only £60 was deducted the next day. I pointed this out to reception on the way to dinner and was
told that the evening staff. When I looked on the TV screen the next morning an item for drinks was on the account even
though my sister had settled the bill that night. The a
 */

 /* Although the facilities were good, the hotel lacked some of the basics, eg shower caps, glasses in the room,
    chaos at breakfast on Sunday morning*/



SELECT
	follow_up_answer,
	SNOWFLAKE.CORTEX.EXTRACT_ANSWER(follow_up_answer,
    'What is the best thing about hotels?')
FROM se.data.user_booking_review ubr