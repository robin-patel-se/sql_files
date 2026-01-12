SELECT * FROM se.finance.aviate_transactions a WHERE a.transaction_id = 'avi_D2034F34-7871-4007-B9D9-5C0BFE9B12C4'
;

SELECT * FROM raw_vault_mvp.aviate.tig_transaction_report ttr WHERE ttr.transaction_id = 'D2034F34-7871-4007-B9D9-5C0BFE9B12C4';
-- transaction was received 2021-07-17 03:13:15.193479000

SELECT * FROM raw_vault_mvp.aviate.tig_transaction_report ttr WHERE ttr.transaction_id = 'F58CA557-B32B-4D90-8FE7-AD70013D925B';
-- transaction was received 2021-08-18 03:15:27.958475000


SELECT * FROM hygiene_vault_mvp.enett.van_settlement_report vsr WHERE 'ent_' || SHA2(vsr.remote_filename || IFNULL(vsr.remote_file_row_number, 0)) = 'ent_fc21b9abb2ac2a122942d4f10211601c5e6bbb9bae2f2137eb53b520dd0cc2a9'

SELECT * FROM se.finance.travel_trust_money_out ttmo WHERE ttmo.transaction_id = 'ent_fc21b9abb2ac2a122942d4f10211601c5e6bbb9bae2f2137eb53b520dd0cc2a9'