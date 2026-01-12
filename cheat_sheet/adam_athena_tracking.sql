WITH json_ob AS (
    SELECT PARSE_JSON(
                   '{"element_name":"recommended for you","element_sales":[{"saleId":"A12565"},{"saleId":"112709"},{"saleId":"A16298"},{"saleId":"112654"},{"saleId":"A10479"},{"saleId":"112546"},{"saleId":"A8815"},{"saleId":"A4959"},{"saleId":"A11509"}]}') AS se_label_adam,
           PARSE_JSON('{
                          "element_name": "recommended for you",
                          "element_sale_ids":
                            {
                              "1": "A12565",
                              "2": "112709",
                              "3": "A16298",
                              "4": "112654",
                              "5": "A10479",
                              "6": "112546",
                              "7": "A8815",
                              "8": "A4959",
                              "9": "A11509"
                            }
                }')                                                                                                                                                                                                                        AS se_label_robin
)
SELECT jo.se_label_adam,
       jo.se_label_adam:element_name,
       jo.se_label_adam:element_sales,
       jo.se_label_robin,
       jo.se_label_robin:element_sale_ids,
       jo.se_label_robin:element_sale_ids['1']
FROM json_ob jo;



WITH json_ob AS (
    SELECT PARSE_JSON('{
                          "element_name": "recommended for you",
                          "element_sale_ids":
                            {
                              "1": "A12565",
                              "2": "112709",
                              "3": "A16298",
                              "4": "112654",
                              "5": "A10479",
                              "6": "112546",
                              "7": "A8815",
                              "8": "A4959",
                              "9": "A11509"
                            }
                }') AS se_label_robin
)
SELECT
--        jo.se_label_robin,
--        jo.se_label_robin:element_sale_ids,
--        jo.se_label_robin:element_sale_ids['1'],
element_sids.key,
element_sids.value
FROM json_ob jo,
     LATERAL FLATTEN(INPUT => jo.se_label_robin:element_sale_ids, OUTER => TRUE) element_sids;


USE WAREHOUSE pipe_xlarge;
CREATE TABLE scratch.robinpatel.event_test AS (
    SELECT *
    FROM snowplow.atomic.events e
    WHERE e.event_id = '6b4903d4-4830-4da6-94f1-7a0b9bc31c6d'
      AND e.etl_tstamp >= current_date - 2
);

SELECT et.se_label,
       element_sids.key,
       element_sids.value
FROM scratch.robinpatel.event_test et,
     LATERAL FLATTEN(INPUT => PARSE_JSON(et.se_label):element_sale_ids, OUTER => TRUE) element_sids;


SELECT et.se_label,
       parse_json(et.se_label):element_sales,
       element_sids.key,
       element_sids.value::VARCHAR
FROM (
         SELECT se_label
         FROM snowplow.atomic.events e
         WHERE e.event_id = '6b4903d4-4830-4da6-94f1-7a0b9bc31c6d'
           AND e.etl_tstamp >= current_date - 2
     ) et,
     LATERAL FLATTEN(INPUT => parse_json(et.se_label):element_sales, OUTER => TRUE) element_sids;


SELECT
PARSE_JSON('{"element_name":"recommended for you","element_sale":"A4959","list_position":6,"sale_type":"wrd"}'):element_sale

-- content_rendered or content_viewed

------------------------------------------------------------------------------------------------------------------------
WITH parse_js AS (
    SELECT PARSE_JSON(
                   '[
  {
    "elements": [
      "recommended for you"
    ],
    "sales": [
      {
        "element": "recommended for you",
        "sale_id": "A12144"
      },
      {
        "element": "recommended for you",
        "sale_id": "A5826"
      },
      {
        "element": "recommended for you",
        "sale_id": "A10026"
      },
      {
        "element": "recommended for you",
        "sale_id": "A16955"
      },
      {
        "element": "recommended for you",
        "sale_id": "A4904"
      },
      {
        "element": "recommended for you",
        "sale_id": "A4509"
      },
      {
        "element": "recommended for you",
        "sale_id": "A10640"
      },
      {
        "element": "recommended for you",
        "sale_id": "A9741"
      },
      {
        "element": "recommended for you",
        "sale_id": "A11643"
      }
    ]
  }
]') AS js
)
SELECT js,
       element_sids.value:sale_id::VARCHAR
FROM parse_js,
LATERAL FLATTEN(INPUT => js[0]['sales'], OUTER => TRUE) element_sids

------------------------------------------------------------------------------------------------------------------------
SELECT
PARSE_JSON('{

		"aid": "ios_app UK",
		"se_la": "you may also like",
		"res": "1536x2048",
		"p": "mob",
		"uid": "63",
		"co": "{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1","data":[{"schema":"iglu:com.secretescapes/content_rendered_context/jsonschema/1-0-0","data":{"sales":[{"sale_id":"A12822","element":"you may also like"}],"elements":"you may also like"}},{"schema":"iglu:com.snowplowanalytics.snowplow/mobile_context/jsonschema/1-0-1","data":{"osType":"ios","appleIdfa":"A0768A97-E429-45B2-994D-36977588F691","osVersion":"13.3.1","appleIdfv":"79BF0B12-E159-4994-9E61-6539273C1555","deviceManufacturer":"Apple Inc.","networkType":"wifi","deviceModel":"iPad6,3"}},{"schema":"iglu:com.snowplowanalytics.snowplow/client_session/jsonschema/1-0-1","data":{"previousSessionId":"37de94f9-fa9a-4f17-ac89-24c2aeac18ec","firstEventId":"37e0067a-e904-4814-8e6f-034f1f76482c","sessionId":"079de278-d1d5-4026-a8fb-28cc199bfe45","userId":"7cc6393b-b6b6-48d9-b05f-d2d51dc6a043","sessionIndex":3,"storageMechanism":"SQLITE"}}]}",
		"stm": "1602685239162",
		"se_ca": "content rendered",
		"dtm": "1602685238",
		"tv": "ios-1.1.5",
		"se_va": "0",
		"e": "se",
		"lang": "en-GB",
		"se_ac": "homepage panel",
		"vp": "1536x2048",
		"eid": "92fe6232-6e6a-4e35-b99c-cffafa23b732"
	}')