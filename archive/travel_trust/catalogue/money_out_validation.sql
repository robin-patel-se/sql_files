--1451.61 (PRE_SETTLEMENT_TRAVEL_TRUST_MONEY_IN_CUMULATIVE)
SELECT *
FROM collab.travel_trust.travel_trust_money_out
WHERE booking_id = 'TB-21899978';
--747.17 + 704.44 = 1452.14 vs 1451.61
SELECT *
FROM se.finance.stripe_refund
WHERE booking_id = 'TB-21899978'; --correct
SELECT *
FROM se.finance.tb_order_payment_coupon
WHERE booking_id = 'TB-21899978';

--704.4400
--747.1700

------------------------------------------------------------------------------------------------------------------------

'TB-21900368'
'TB-21900708'
'TB-21897522'
'TB-21897590'
'TB-21897771'
'TB-21898721'
'TB-21899479'
'TB-21900381'
'TB-21900635'
'TB-21900708'


SELECT *
FROM collab.travel_trust.travel_trust_money_out
WHERE booking_id = 'TB-21900368';
SELECT *
FROM se.finance.stripe_refund
WHERE booking_id = 'TB-21900368';
SELECT *
FROM se.finance.tb_order_payment_coupon
WHERE booking_id = 'TB-21900368';

--money out , money in cumulative 1728.7400
--506.0200
--1222.7200

