USE WAREHOUSE pipe_xlarge;
SELECT *
FROM se.data_pii.scv_touch_basic_attributes stba
    INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
WHERE stba.touch_hostname LIKE '%telegraph%'
  AND stmc.referrer_hostname LIKE '%telegraph%'
  AND stba.touch_start_tstamp >= '2022-01-01';

https://www.hand-picked.telegraph.co.uk/leicestershire-country-spa-hotel-stay-with-a-suite-fully-refundable-sketchley-grange-hotel-and-spa-hinckley-midlands/sale-hotel?noPasswordSignIn=true&utm_medium=email&utm_source=newsletter&utm_campaign=4358662&utm_platform=ITERABLE&utm_content=SEGMENT_CORE_UK_ACT_01M_ATHENA_PoC_A


https://www.hand-picked.telegraph.co.uk/sale/book-hotel?startDate=2022-7-22&endDate=2022-7-24&rooms=2&offerId=13598&saleId=13359&agentId=&numberOfAdults=2&selectedFlightJsonString=&flightIndex=&numberOfFlightResults=&maxAvailableRooms=10&singleResult=false&rateCodes=CJSD&rateCodes=CJSD&staffBooking=false


SELECT *
FROM se.data_pii.scv_touch_basic_attributes stba
    INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
WHERE stba.touch_hostname LIKE '%telegraph%'
  AND stmc.referrer_hostname LIKE '%telegraph%'
  AND stba.touch_start_tstamp >= '2022-01-01'
AND stba.attributed_user_id = '1797956';



TOUCH_LANDING_PAGE
https://www.hand-picked.telegraph.co.uk/sale/book-hotel?startDate=2022-3-6&endDate=2022-3-8&rooms=1&offerId=12045&saleId=11072&agentId=&numberOfAdults=2&selectedFlightJsonString=&flightIndex=&numberOfFlightResults=&maxAvailableRooms=5&singleResult=false&rateCodes=BBS&rateCodes=BBS&staffBooking=false&_gl=1*1v8gi5o*_up*MQ..
https://www.hand-picked.telegraph.co.uk/sale/book-hotel?startDate=2022-3-5&endDate=2022-3-8&rooms=1&offerId=21254&saleId=34934&agentId=&numberOfAdults=2&selectedFlightJsonString=&flightIndex=&numberOfFlightResults=&maxAvailableRooms=5&singleResult=false&rateCodes=SDOTRB&rateCodes=SDOTRB&rateCodes=SDOTRB&staffBooking=false&_gl=1*24ra70*_up*MQ..
https://www.hand-picked.telegraph.co.uk/sale/book-hotel?startDate=2022-3-6&endDate=2022-3-8&rooms=1&offerId=12045&saleId=11072&agentId=&numberOfAdults=2&selectedFlightJsonString=&flightIndex=&numberOfFlightResults=&maxAvailableRooms=5&singleResult=false&rateCodes=BBS&rateCodes=BBS&staffBooking=false&_gl=1*g0hjiu*_up*MQ..


TOUCH_REFERRER_URL
https://www.hand-picked.telegraph.co.uk/relaxing-northamptonshire-spa-hotel-stay-fully-refundable-kettering-park-hotel-and-spa-kettering-midlands/sale-hotel?noPasswordSignIn=true&utm_medium=email&utm_source=newsletter&utm_campaign=4377373&utm_platform=ITERABLE&utm_content=SEGMENT_CORE_UK_ACT_01M_ATHENA_PoC_A
https://www.hand-picked.telegraph.co.uk/historic-warwickshire-estate-in-a-serene-rural-setting-fully-refundable-wroxall-abbey-hotel-and-spa-wroxall-near-royal-leamington-spa/sale-hotel?noPasswordSignIn=true&utm_medium=email&utm_source=newsletter&utm_campaign=4377373&utm_platform=ITERABLE&utm_content=SEGMENT_CORE_UK_ACT_01M_ATHENA_PoC_A
