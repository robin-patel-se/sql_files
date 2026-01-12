------------------------------------------------------------------------------------------------------------------------
-- booking tables
--booking level
SELECT *
FROM se.data.master_se_booking_list msbl;
SELECT *
FROM se.data.master_tb_booking_list msbl;
SELECT *
FROM se.data.master_all_booking_list msbl;

SELECT *
FROM se.data.se_booking sb;
SELECT *
FROM se.data.tb_booking tb;

SELECT *
FROM se.data.fact_booking fb;
SELECT *
FROM se.data.fact_complete_booking fb;

SELECT GET_DDL('table', 'se.data.fact_complete_booking');

SELECT DISTINCT tech_platform
FROM se.data.fact_complete_booking fcb;

------------------------------------------------------------------------------------------------------------------------
-- sale/offer tables
SELECT *
FROM se.data.se_sale_attributes ssa
WHERE ssa.sale_active;
SELECT *
FROM se.data.tb_offer t;

SELECT *
FROM se.data.dim_sale ds;

------------------------------------------------------------------------------------------------------------------------
--credits
SELECT *
FROM se.data.se_credit sc;

------------------------------------------------------------------------------------------------------------------------
--vouchers
SELECT *
FROM se.data.se_voucher sv;


------------------------------------------------------------------------------------------------------------------------
--mari/allocation data
SELECT *
FROM se.data.se_room_type_rooms_and_rates srtrar
WHERE srtrar.room_type_id = '466';
SELECT *
FROM se.data.se_hotel_rooms_and_rates shrar;

SELECT *
FROM se.data.old_data_model_allocation odma;

------------------------------------------------------------------------------------------------------------------------
--crm data
--salesforce

SELECT *
FROM se.data.crm_jobs_list cjl;
SELECT *
FROM se.data.crm_events_sends ces;
SELECT *
FROM se.data.crm_events_opens ceo;
SELECT *
FROM se.data.crm_events_clicks cec;
SELECT *
FROM se.data.crm_events_unsubscribes ceu;


------------------------------------------------------------------------------------------------------------------------
--email/athena data
SELECT *
FROM se.data.email_performance ep;



------------------------------------------------------------------------------------------------------------------------
--scv data


------------------------------------------------------------------------------------------------------------------------
--user behaviour data


------------------------------------------------------------------------------------------------------------------------
--snowflake beta ui


------------------------------------------------------------------------------------------------------------------------


SELECT *
FROM collab.test.vw_image_tags vit;



SELECT GET_DDL('table', 'collab.test.vw_image_tags');

CREATE OR REPLACE VIEW collab.test.vw_image_tags COPY GRANTS AS
(
SELECT it.*,
       vdi.deal_id,
       vdi.territory_id,
       vdi.territory_name
FROM data_science.mart_analytics.image_tags it
    INNER JOIN data_science.predictive_modeling.vw_deal_images vdi ON
    it.photo_url = REGEXP_REPLACE(vdi.photo_url, '\\?.*')
    );

USE WAREHOUSE pipe_xlarge;

SELECT *
FROM raw_vault_mvp.cms_mysql.photo p
WHERE filename LIKE '%b3f70763_adf9_4b06_8013_182dbb0ccf45%'


SELECT REPLACE('https://secretescapes-web.imgix.net/hotels/2727/b3f70763_adf9_4b06_8013_182dbb0ccf45.jpg', 'https://secretescapes-web.imgix.net/')


SELECT REGEXP_SUBSTR(REPLACE(photo_url, 'https://secretescapes-web.imgix.net/'), '.*/(\\d*)/.*', 1, 1, 'e') AS id,
       *
FROM collab.test.vw_image_tags
;

SELECT it.*,
       vdi.deal_id,
       vdi.territory_id,
       vdi.territory_name
FROM data_science.mart_analytics.image_tags it
    INNER JOIN data_science.predictive_modeling.vw_deal_images vdi ON
    it.photo_url = REGEXP_REPLACE(vdi.photo_url, '\\?.*');


SELECT REGEXP_REPLACE(photo_url, '?')
FROM data_science.predictive_modeling.vw_deal_images;

SELECT REGEXP_REPLACE('https://secretescapes-web.imgix.net/sales/113320/b001f06e_5838_4dff_b407_9d56a8450700.jpg?auto=format,compress', '\\?.*');

SELECT * FROM collab.test.vw_image_tags;