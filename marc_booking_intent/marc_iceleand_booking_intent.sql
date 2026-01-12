-- we want to know the intent of users looking at iceland
-- looking at destination confidence and booking probability bucket


SELECT *
FROM data_science.operational_output.booking_intent_prediction_prod booking_intent
WHERE booking_intent.inference_ts >= CURRENT_DATE - 1
AND booking_intent.destination_country = 'Iceland'
;


SELECT * FROM se.data.scv_touched_feature_flags stff