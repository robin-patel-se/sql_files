SELECT opp.id                  AS saleforce_opportunity_id,
       ac.id                   AS account_id,
       ac.name                 AS hotel_name,
       ac.booking_com_name__c,
       i.id                    AS inclusions_id,
       o.id                    AS offer_id,
       o.offer_id__c,
       o.name                  AS offer_name,
       o.board_basis__c        AS board_basis,
       o.room_type_ota_name__c AS room_type_ota_name,
       i.id                    AS inclusion_id,
       i.name                  AS inclusion_name,
       i.currencyisocode,
       i.inclusion_type__c,
       i.inclusion_rate__c,
       i.inclusion_level__c,
       i.currency_formula__c,
       i.inclusion_value_new__c,
       i.active_inclusion__c
FROM data_vault_mvp.sfsc_snapshots.offers_snapshot o
         LEFT JOIN hygiene_snapshot_vault_mvp.sfsc.opportunity opp ON opp.id = o.opportunity__c
         LEFT JOIN hygiene_snapshot_vault_mvp.sfsc.account ac ON ac.id = opp.accountid
         LEFT JOIN hygiene_snapshot_vault_mvp.sfsc.inclusion i ON i.offer__c = o.id
WHERE LEFT(opp.id, 15) = '0061r00001HQrq2'
ORDER BY o.id DESC
LIMIT 1;
