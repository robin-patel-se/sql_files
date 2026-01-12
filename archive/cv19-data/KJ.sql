USE WAREHOUSE pipe_xlarge;

SELECT tr.touch_id,
       tr.event_tstamp,
       tr.booking_id,
       case
           when LEFT(tr.booking_id, 2) != 'TB' THEN 'se_booking'
           WHEN LEFT(tr.booking_id, 2) = 'TB' THEN 'tb_booking'
           else 'other' end    booking_code,
       b.attributed_user_id AS se_user_id,
       ch.touch_hostname_territory,
       ch.touch_affiliate_territory,
       b.stitched_identity_type,
       ch.affiliate,
       ch.touch_mkt_channel AS last_non_direct_channel,
       cb.margin_gross_of_toms_gbp
FROM data_vault_mvp.single_customer_view_stg.module_touched_transactions tr
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_attribution ta
                    ON tr.touch_id = ta.touch_id AND ta.attribution_model = 'last non direct'
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel ch
                    ON ta.attributed_touch_id = ch.touch_id
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes b
                    ON ta.attributed_touch_id = b.touch_id
         INNER JOIN se.data.fact_complete_booking cb ON tr.booking_id = cb.booking_id
WHERE tr.event_tstamp >= '2020-03-01' and tr.event_tstamp < '2020-03-02';

