SELECT ai_classify('One day I will see the world', ['travel', 'cooking'])
;

;

SELECT
	ubr.booking_id,
	ubr.customer_score,
	ubr.follow_up_answer,
	snowflake.cortex.summarize(ubr.follow_up_answer)                                                                        AS summarised_answer, -- I cheat using this to translate to avoid setting input output arguments for cortex translate
	ai_classify(ubr.follow_up_answer,
				['Likey Hotel','No likey Hotel', 'Likey Secret Escapes', 'Nothing to do with Hotel'])['labels']             AS classify,
	ai_classify(ubr.follow_up_answer,
				['Likey Hotel','No likey Hotel', 'Likey Secret Escapes', 'Nothing to do with Hotel'])['labels'][0]::VARCHAR AS likey_no_likey
FROM se.data.user_booking_review ubr
WHERE ubr.follow_up_answer IS NOT NULL
LIMIT 100
;


SELECT AI_SIMILARITY('I like this dish', 'This dish is very good');