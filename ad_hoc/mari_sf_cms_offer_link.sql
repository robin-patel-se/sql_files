SELECT *
FROM se.data.se_sale_attributes ssa
         INNER JOIN se.data.se_hotel_sale_offer shso ON ssa.se_sale_id = shso.sale_id
WHERE ssa.se_sale_id = 'A14951';


SELECT *
FROM se.data.se_offer_attributes soa
         INNER JOIN se.data.se_cms_mari_link scml ON soa.base_offer_id = scml.offer_id;


WITH mari_details AS (
    --collate mari details at rate plan level
    SELECT rps.id,
           rps.name                                           AS rate_plan_name,
           rts.name                                           AS room_type_name,
           rps.date_created,
           rps.room_type_id,
           hs.code || ':' || rps.code || ':' || rps.rack_code AS hotel_rate_rack_code,
           hs.name                                            AS hotel_name,
           hs.code                                            AS hotel_code,
           rps.code                                           AS rate_code,
           rps.rack_code,
           rps.currency
    FROM data_vault_mvp.mari_snapshots.rate_plan_snapshot rps
             INNER JOIN data_vault_mvp.mari_snapshots.room_type_snapshot rts ON rps.room_type_id = rts.id
             INNER JOIN data_vault_mvp.mari_snapshots.hotel_snapshot hs ON rts.hotel_id = hs.id
)
SELECT soa.se_offer_id,
       soa.base_offer_id,
       soa.offer_name,
       soa.offer_name_object,
       soa.offer_active,
       soa.hotel_rate_plan_id,
       soa.hotel_code,
       soa.rate_code,
       soa.rack_rate_code,
       scml.product_id,
       scml.hotel_code,
       scml.rate_code,
       scml.rack_rate_code,
       scml.hotel_rate_rack_code,
       md.id             AS mari_rate_plan_id,
       md.rate_plan_name AS mari_rate_plan_name,
       md.room_type_name AS mari_room_type_name,
       md.date_created   AS mari_date_created,
       md.room_type_id   AS mari_room_type_id,
       md.hotel_name     AS mari_hotel_name,
       md.hotel_code     AS mari_hotel_code,
       md.rate_code      AS mari_rate_code,
       md.rack_code      AS mari_rack_code,
       md.currency       AS mari_currency,
       os.id             AS sf_offer_id,
       os.opportunity__c AS sf_opportunity_id,
       o.accountid       AS sf_account_id
FROM se.data.se_offer_attributes soa
         INNER JOIN se.data.se_cms_mari_link scml ON soa.base_offer_id = scml.offer_id
         INNER JOIN mari_details md ON scml.hotel_rate_rack_code = md.hotel_rate_rack_code
         INNER JOIN data_vault_mvp.sfsc_snapshots.offers_snapshot os ON soa.base_offer_id::VARCHAR = os.offer_id__c
         INNER JOIN hygiene_snapshot_vault_mvp.sfsc.opportunity o ON os.opportunity__c = o.id;


