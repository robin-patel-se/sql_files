USE WAREHOUSE pipe_xlarge;
--list of bookings and their non dir channel
SELECT tr.touch_id,
       tr.event_tstamp,
       tr.booking_id,
       b.attributed_user_id AS se_user_id,
       b.stitched_identity_type,
       ch.touch_hostname_territory,
       ch.touch_affiliate_territory,
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
WHERE tr.event_tstamp >= '2020-02-28';


--aggregated bookings by last non direct channel
SELECT tr.event_tstamp::DATE                                           AS booking_date,
       ch.touch_mkt_channel                                            AS last_non_direct_channel,
       SUM(CASE WHEN LEFT(tr.booking_id, 2) != 'TB' THEN 1 ELSE 0 END) AS se_bookings,
       SUM(CASE WHEN LEFT(tr.booking_id, 2) = 'TB' THEN 1 ELSE 0 END)  AS tb_bookings
FROM data_vault_mvp.single_customer_view_stg.module_touched_transactions tr
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_attribution ta
                    ON tr.touch_id = ta.touch_id AND ta.attribution_model = 'last non direct'
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel ch
                    ON ta.attributed_touch_id = ch.touch_id
WHERE tr.event_tstamp >= '2020-02-28'
GROUP BY 1, 2;

------------------------------------------------------------------------------------------------------------------------
