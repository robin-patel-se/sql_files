airflow backfill --start_date '2020-03-27 09:00:00' --end_date '2020-03-27 09:00:00' --task_regex '.*' incoming__travelbird_mysql__offers_offer__hourly
airflow backfill --start_date '2020-03-27 23:00:00' --end_date '2020-03-27 23:00:00' --task_regex '.*' incoming__travelbird_mysql__offers_offer__hourly
airflow backfill --start_date '2020-03-28 00:00:00' --end_date '2020-03-28 00:00:00' --task_regex '.*' incoming__travelbird_mysql__offers_offer__hourly
airflow backfill --start_date '2020-03-30 11:00:00' --end_date '2020-03-30 11:00:00' --task_regex '.*' incoming__travelbird_mysql__offers_offer__hourly
airflow backfill --start_date '2020-03-29 23:00:00' --end_date '2020-03-29 23:00:00' --task_regex '.*' incoming__travelbird_mysql__offers_offer__hourly
airflow backfill --start_date '2020-03-27 09:00:00' --end_date '2020-03-27 09:00:00' --task_regex '.*' hygiene_snapshots__travelbird_mysql__offers_offer__hourly
airflow backfill --start_date '2020-03-27 23:00:00' --end_date '2020-03-27 23:00:00' --task_regex '.*' hygiene_snapshots__travelbird_mysql__offers_offer__hourly
airflow backfill --start_date '2020-03-28 00:00:00' --end_date '2020-03-28 00:00:00' --task_regex '.*' hygiene_snapshots__travelbird_mysql__offers_offer__hourly
airflow backfill --start_date '2020-03-30 11:00:00' --end_date '2020-03-30 11:00:00' --task_regex '.*' hygiene_snapshots__travelbird_mysql__offers_offer__hourly
airflow backfill --start_date '2020-03-29 23:00:00' --end_date '2020-03-29 23:00:00' --task_regex '.*' hygiene_snapshots__travelbird_mysql__offers_offer__hourly

airflow backfill --start_date '2020-03-27 09:00:00' --end_date '2020-03-27 09:00:00' --task_regex '.*' dwh__transactional__sale__hourly
airflow backfill --start_date '2020-03-28 00:00:00' --end_date '2020-03-28 00:00:00' --task_regex '.*' dwh__transactional__sale__hourly
airflow backfill --start_date '2020-03-30 11:00:00' --end_date '2020-03-30 11:00:00' --task_regex '.*' dwh__transactional__sale__hourly
airflow backfill --start_date '2020-03-30 23:00:00' --end_date '2020-03-30 23:00:00' --task_regex '.*' dwh__transactional__sale__hourly
airflow backfill --start_date '2020-03-30 00:00:00' --end_date '2020-03-30 00:00:00' --task_regex '.*' dwh__transactional__sale__hourly

airflow backfill --start_date '2020-03-27 00:00:00' --end_date '2020-03-27 00:00:00' --task_regex '.*' dwh__transactional__booking__hourly
airflow backfill --start_date '2020-03-27 23:00:00' --end_date '2020-03-27 23:00:00' --task_regex '.*' dwh__transactional__booking__hourly
airflow backfill --start_date '2020-03-27 09:00:00' --end_date '2020-03-27 09:00:00' --task_regex '.*' dwh__transactional__booking__hourly
airflow backfill --start_date '2020-03-28 00:00:00' --end_date '2020-03-28 00:00:00' --task_regex '.*' dwh__transactional__booking__hourly
airflow backfill --start_date '2020-03-30 13:00:00' --end_date '2020-03-30 13:00:00' --task_regex '.*' dwh__transactional__booking__hourly
airflow backfill --start_date '2020-03-30 23:00:00' --end_date '2020-03-30 23:00:00' --task_regex '.*' dwh__transactional__booking__hourly
airflow backfill --start_date '2020-03-29 23:00:00' --end_date '2020-03-29 23:00:00' --task_regex '.*' dwh__transactional__booking__hourly
airflow backfill --start_date '2020-04-04 23:00:00' --end_date '2020-04-04 23:00:00' --task_regex '.*' dwh__transactional__booking__hourly
airflow backfill --start_date '2020-04-05 23:00:00' --end_date '2020-04-05 23:00:00' --task_regex '.*' dwh__transactional__booking__hourly

airflow backfill --start_date '2020-03-27 00:00:00' --end_date '2020-03-27 00:00:00' --task_regex '.*' single_customer_view__daily
airflow backfill --start_date '2020-03-30 00:00:00' --end_date '2020-03-30 00:00:00' --task_regex '.*' single_customer_view__daily
airflow backfill --start_date '2020-04-06 00:00:00' --end_date '2020-04-06 00:00:00' --task_regex '.*' single_customer_view__daily

airflow backfill --start_date '2020-03-29 14:00:00' --end_date '2020-03-29 14:00:00' --task_regex '.*' incoming__cms_mysql__booking__hourly

airflow backfill --start_date '2020-03-29 14:00:00' --end_date '2020-03-29 14:00:00' --task_regex '.*' hygiene_snapshots__cms_mysql__booking__hourly
airflow backfill --start_date '2020-03-30 11:00:00' --end_date '2020-03-30 11:00:00' --task_regex '.*' hygiene_snapshots__cms_mysql__booking__hourly
airflow backfill --start_date '2020-03-30 12:00:00' --end_date '2020-03-30 12:00:00' --task_regex '.*' hygiene_snapshots__cms_mysql__booking__hourly

airflow backfill --start_date '2020-03-29 14:00:00' --end_date '2020-03-29 14:00:00' --task_regex '.*'  hygiene_snapshots__cms_mysql__reservation__hourly
airflow backfill --start_date '2020-03-30 11:00:00' --end_date '2020-03-30 11:00:00' --task_regex '.*' hygiene_snapshots__cms_mysql__reservation__hourly
airflow backfill --start_date '2020-03-30 23:00:00' --end_date '2020-03-30 23:00:00' --task_regex '.*' hygiene_snapshots__cms_mysql__reservation__hourly

airflow backfill --start_date '2020-03-29 14:00:00' --end_date '2020-03-29 14:00:00' --task_regex '.*' incoming__cms_mongodb__booking_summary__hourly

airflow backfill --start_date '2020-03-29 14:00:00' --end_date '2020-03-29 14:00:00' --task_regex '.*' incoming__cms_mysql__base_sale__hourly
airflow backfill --start_date '2020-03-29 14:00:00' --end_date '2020-03-29 14:00:00' --task_regex '.*' incoming__cms_mysql__sale__hourly
airflow backfill --start_date '2020-03-29 14:00:00' --end_date '2020-03-29 14:00:00' --task_regex '.*' incoming__cms_mysql__hotel__hourly
airflow backfill --start_date '2020-03-29 14:00:00' --end_date '2020-03-29 14:00:00' --task_regex '.*' incoming__cms_mysql__sale_flight_config__hourly
airflow backfill --start_date '2020-03-30 23:00:00' --end_date '2020-03-30 23:00:00' --task_regex '.*' incoming__cms_mysql__sale_flight_config__hourly
airflow backfill --start_date '2020-03-30 23:00:00' --end_date '2020-03-30 23:00:00' --task_regex '.*' hygiene_snapshots__cms_mysql__sale_flight_config__hourly


airflow backfill --start_date '2020-03-30 11:00:00' --end_date '2020-03-30 11:00:00' --task_regex '.*' hygiene_snapshots__cms_mysql__hotel__hourly
airflow backfill --start_date '2020-03-30 23:00:00' --end_date '2020-03-30 23:00:00' --task_regex '.*' hygiene_snapshots__cms_mysql__hotel__hourly


airflow backfill --start_date '2020-03-30 11:00:00' --end_date '2020-03-30 11:00:00' --task_regex '.*' hygiene_snapshots__cms_mysql__sale__hourly
airflow backfill --start_date '2020-03-30 23:00:00' --end_date '2020-03-30 23:00:00' --task_regex '.*' hygiene_snapshots__cms_mysql__sale__hourly

airflow backfill --start_date '2020-03-30 11:00:00' --end_date '2020-03-30 11:00:00' --task_regex '.*' hygiene_snapshots__cms_mysql__sale_flight_config__hourly
airflow backfill --start_date '2020-03-30 13:00:00' --end_date '2020-03-30 13:00:00' --task_regex '.*' hygiene_snapshots__cms_mysql__sale_flight_config__hourly

airflow backfill --start_date '2020-03-30 11:00:00' --end_date '2020-03-30 11:00:00' --task_regex '.*' hygiene_snapshots__cms_mongodb__booking_summary__hourly


airflow backfill --start_date '2020-03-29 14:00:00' --end_date '2020-03-29 14:00:00' --task_regex '.*' hygiene_snapshots__cms_mongodb__booking_summary__hourly
airflow backfill --start_date '2020-03-30 13:00:00' --end_date '2020-03-30 13:00:00' --task_regex '.*' hygiene_snapshots__cms_mongodb__booking_summary__hourly

airflow backfill --start_date '2020-03-30 08:00:00' --end_date '2020-03-30 08:00:00' --task_regex '.*' hygiene_snapshots__cms_mysql__base_sale__hourly
airflow backfill --start_date '2020-03-30 13:00:00' --end_date '2020-03-30 13:00:00' --task_regex '.*' hygiene_snapshots__cms_mysql__base_sale__hourly

airflow backfill --start_date '2020-03-29 14:00:00' --end_date '2020-03-29 14:00:00' --task_regex '.*' hygiene_snapshots__cms_mysql__sale_flight_config__hourly
airflow backfill --start_date '2020-03-29 14:00:00' --end_date '2020-03-29 14:00:00' --task_regex '.*' hygiene_snapshots__cms_mysql__hotel__hourly
airflow backfill --start_date '2020-03-29 14:00:00' --end_date '2020-03-29 14:00:00' --task_regex '.*' hygiene_snapshots__cms_mysql__sale__hourly
airflow backfill --start_date '2020-03-30 13:00:00' --end_date '2020-03-30 13:00:00' --task_regex '.*' hygiene_snapshots__cms_mysql__reservation__hourly
airflow backfill --start_date '2020-03-30 13:00:00' --end_date '2020-03-30 13:00:00' --task_regex '.*' hygiene_snapshots__cms_mysql__hotel__hourly
airflow backfill --start_date '2020-03-30 13:00:00' --end_date '2020-03-30 13:00:00' --task_regex '.*' hygiene_snapshots__cms_mysql__sale_flight_config__hourly
airflow backfill --start_date '2020-03-30 13:00:00' --end_date '2020-03-30 13:00:00' --task_regex '.*' hygiene_snapshots__cms_mysql__sale__hourly

airflow backfill --start_date '2020-03-29 14:00:00' --end_date '2020-03-29 14:00:00' --task_regex '.*' incoming__travelbird_mysql__currency_exchangerateupdate__hourly
airflow backfill --start_date '2020-03-29 14:00:00' --end_date '2020-03-29 14:00:00' --task_regex '.*' hygiene_snapshots__travelbird_mysql__currency_exchangerateupdate__hourly

airflow backfill --start_date '2020-03-29 14:00:00' --end_date '2020-03-29 14:00:00' --task_regex '.*' incoming__travelbird_mysql__django_content_type__hourly
airflow backfill --start_date '2020-03-29 14:00:00' --end_date '2020-03-29 14:00:00' --task_regex '.*' hygiene_snapshots__travelbird_mysql__django_content_type__hourly

airflow backfill --start_date '2020-03-29 10:00:00' --end_date '2020-03-29 10:00:00' --task_regex '.*' hygiene_snapshots__travelbird_mysql__offers_offer__hourly
airflow backfill --start_date '2020-03-29 10:00:00' --end_date '2020-03-29 10:00:00' --task_regex '.*' incoming__travelbird_mysql__offers_offer__hourly
airflow backfill --start_date '2020-03-27 10:00:00' --end_date '2020-03-27 10:00:00' --task_regex '.*' incoming__travelbird_mysql__offers_offer__hourly
airflow backfill --start_date '2020-03-27 10:00:00' --end_date '2020-03-27 10:00:00' --task_regex '.*' hygiene_snapshots__travelbird_mysql__offers_offer__hourly

airflow backfill --start_date '2020-03-29 14:00:00' --end_date '2020-03-29 14:00:00' --task_regex '.*' hygiene_snapshots__travelbird_mysql__orders_order__hourly
airflow backfill --start_date '2020-03-29 14:00:00' --end_date '2020-03-29 14:00:00' --task_regex '.*' hygiene_snapshots__travelbird_mysql__orders_orderitembase__hourly


airflow backfill --start_date '2020-04-02 14:00:00' --end_date '2020-04-02 14:00:00' --task_regex '.*' hygiene_snapshots__cms_mysql__booking__hourly
airflow backfill --start_date '2020-04-03 00:00:00' --end_date '2020-04-03 00:00:00' --task_regex '.*' hygiene_snapshots__cms_mysql__booking__hourly

airflow backfill --start_date '2020-04-02 14:00:00' --end_date '2020-04-02 14:00:00' --task_regex '.*' dwh__transactional__booking__hourly
airflow backfill --start_date '2020-04-02 23:00:00' --end_date '2020-04-02 23:00:00' --task_regex '.*' dwh__transactional__booking__hourly


airflow backfill --start_date '2020-04-03 17:00:00' --end_date '2020-04-03 17:00:00' --task_regex '.*' hygiene_snapshots__travelbird_mysql__orders_orderitembase__hourly
airflow backfill --start_date '2020-04-03 17:00:00' --end_date '2020-04-03 17:00:00' --task_regex '.*' incoming__travelbird_mysql__orders_orderitembase__hourly

airflow backfill --start_date '2020-04-03 23:00:00' --end_date '2020-04-03 23:00:00' --task_regex '.*' incoming__cms_mysql__sale_flight_config__hourly
airflow backfill --start_date '2020-04-03 15:00:00' --end_date '2020-04-03 15:00:00' --task_regex '.*' hygiene_snapshots__cms_mysql__sale_flight_config__hourly

airflow backfill --start_date '2020-04-03 23:00:00' --end_date '2020-04-03 23:00:00' --task_regex '.*' dwh__transactional__booking__hourly
airflow backfill --start_date '2020-04-04 00:00:00' --end_date '2020-04-04 00:00:00' --task_regex '.*' dwh__transactional__booking__hourly
airflow backfill --start_date '2020-04-04 01:00:00' --end_date '2020-04-05 23:00:00' --task_regex '.*' -m dwh__transactional__booking__hourly


airflow backfill --start_date '2020-04-03 18:00:00' --end_date '2020-04-03 18:00:00' --task_regex '.*' hygiene_snapshots__cms_mysql__booking__hourly


airflow backfill --start_date '2020-04-04 23:00:00' --end_date '2020-04-04 23:00:00' --task_regex '.*' hygiene_snapshots__cms_mysql__booking__hourly
airflow backfill --start_date '2020-04-05 00:00:00' --end_date '2020-04-05 22:00:00' -m --task_regex '.*' hygiene_snapshots__cms_mysql__booking__hourly


airflow backfill --start_date '2020-04-04 22:00:00' --end_date '2020-04-04 22:00:00' -m --task_regex '.*' incoming__cms_mysql__booking__hourly



airflow backfill --start_date '2020-04-04 23:00:00' --end_date '2020-04-04 23:00:00' --task_regex '.*' hygiene_snapshots__cms_mysql__sale__hourly
airflow backfill --start_date '2020-04-05 00:00:00' --end_date '2020-04-05 22:00:00' --task_regex '.*' -m hygiene_snapshots__cms_mysql__sale__hourly


airflow backfill --start_date '2020-04-04 18:00:00' --end_date '2020-04-04 22:00:00' --task_regex '.*' -m hygiene_snapshots__cms_mysql__sale_flight_config__hourly
airflow backfill --start_date '2020-04-05 00:00:00' --end_date '2020-04-05 22:00:00' --task_regex '.*' -m hygiene_snapshots__cms_mysql__sale_flight_config__hourly

airflow backfill --start_date '2020-04-05 23:00:00' --end_date '2020-04-05 23:00:00' --task_regex '.*' hygiene_snapshots__cms_mysql__sale_flight_config__hourly
airflow backfill --start_date '2020-04-06 00:00:00' --end_date '2020-04-05 14:00:00' --task_regex '.*' -m hygiene_snapshots__cms_mysql__sale_flight_config__hourly

airflow backfill --start_date '2020-04-05 23:00:00' --end_date '2020-04-05 23:00:00' --task_regex '.*' incoming__cms_mysql__sale_flight_config__hourly
airflow backfill --start_date '2020-04-05 14:00:00' --end_date '2020-04-05 22:00:00' --task_regex '.*' -m incoming__cms_mysql__sale_flight_config__hourly





--turn dag off, mark any currently running job as failed, then clear, then run backfill command, once command complete, turn dag back on.

--jobs that failed, ran earliest job that failed to ensure no data is unprocessed
airflow backfill --start_date '2020-04-05 00:00:00' --end_date '2020-04-05 00:00:00' --task_regex '.*' hygiene_snapshots__cms_mysql__booking__hourly
airflow backfill --start_date '2020-04-05 00:00:00' --end_date '2020-04-05 00:00:00' --task_regex '.*' incoming__cms_mysql__booking__hourly

--jobs that were running slow, just ran a more recent required run to allow downstream dependencies to continue.
airflow backfill --start_date '2020-04-06 23:00:00' --end_date '2020-04-06 23:00:00' --task_regex '.*' hygiene_snapshots__travelbird_mysql__offers_offer__hourly
airflow backfill --start_date '2020-04-06 23:00:00' --end_date '2020-04-06 23:00:00' --task_regex '.*' incoming__travelbird_mysql__orders_orderitembase__hourly
airflow backfill --start_date '2020-04-06 23:00:00' --end_date '2020-04-06 23:00:00' --task_regex '.*' hygiene_snapshots__travelbird_mysql__orders_orderitembase__hourly
airflow backfill --start_date '2020-04-05 23:00:00' --end_date '2020-04-05 23:00:00' --task_regex '.*' hygiene_snapshots__cms_mysql__base_sale__hourly
airflow backfill --start_date '2020-04-06 23:00:00' --end_date '2020-04-06 23:00:00' --task_regex '.*' hygiene_snapshots__cms_mysql__base_sale__hourly

--backfill jobs that didn't run and mark success
airflow backfill --start_date '2020-04-06 19:00:00' --end_date '2020-04-06 22:00:00' --task_regex '.*' -m incoming__travelbird_mysql__orders_orderitembase__hourly
airflow backfill --start_date '2020-04-06 13:00:00' --end_date '2020-04-06 22:00:00' --task_regex '.*' -m hygiene_snapshots__travelbird_mysql__orders_orderitembase__hourly
airflow backfill --start_date '2020-04-02 00:00:00' --end_date '2020-04-04 20:00:00' --task_regex '.*' -m incoming__sfmc__events_clicks__hourly
airflow backfill --start_date '2020-04-04 00:00:00' --end_date '2020-04-04 13:00:00' --task_regex '.*' -m hygiene_snapshots__cms_mysql__base_sale__hourly
airflow backfill --start_date '2020-04-05 06:00:00' --end_date '2020-04-05 15:00:00' --task_regex '.*' -m dwh__transactional__sale__hourly
airflow backfill --start_date '2020-04-05 16:00:00' --end_date '2020-04-05 22:00:00' --task_regex '.*' -m dwh__transactional__sale__hourly
airflow backfill --start_date '2020-04-06 00:00:00' --end_date '2020-04-06 22:00:00' --task_regex '.*' -m dwh__transactional__sale__hourly


--hourly jobs failed due to upstream jobs being stuck. Run these to ensure that daily jobs can continue
airflow backfill --start_date '2020-04-05 00:00:00' --end_date '2020-04-05 00:00:00' --task_regex '.*' dwh__transactional__booking__hourly
airflow backfill --start_date '2020-04-05 23:00:00' --end_date '2020-04-05 23:00:00' --task_regex '.*' dwh__transactional__booking__hourly
airflow backfill --start_date '2020-04-06 23:00:00' --end_date '2020-04-06 23:00:00' --task_regex '.*' dwh__transactional__booking__hourly
airflow backfill --start_date '2020-04-05 23:00:00' --end_date '2020-04-05 23:00:00' --task_regex '.*' dwh__transactional__sale__hourly
airflow backfill --start_date '2020-04-06 23:00:00' --end_date '2020-04-06 23:00:00' --task_regex '.*' dwh__transactional__sale__hourly

--daily jobs that failed due to upstream hourly jobs being stuck
airflow backfill --start_date '2020-04-06 00:00:00' --end_date '2020-04-06 00:00:00' --task_regex '.*' customer_model_last7days_uk_de__daily


------------------------------------------------------------------------------------------------------------------------
--stuck dags
airflow backfill --start_date '2020-04-07 23:00:00' --end_date '2020-04-07 23:00:00' --task_regex '.*' incoming__cms_mysql__reservation__hourly
airflow backfill --start_date '2020-04-07 23:00:00' --end_date '2020-04-07 23:00:00' --task_regex '.*' hygiene_snapshots__cms_mysql__reservation__hourly
airflow backfill --start_date '2020-04-07 23:00:00' --end_date '2020-04-07 23:00:00' --task_regex '.*' incoming__travelbird_mysql__django_site__hourly
airflow backfill --start_date '2020-04-07 23:00:00' --end_date '2020-04-07 23:00:00' --task_regex '.*' hygiene_snapshots__travelbird_mysql__django_content_type__hourly
airflow backfill --start_date '2020-04-07 23:00:00' --end_date '2020-04-07 23:00:00' --task_regex '.*' incoming__cms_mysql__profile__hourly
airflow backfill --start_date '2020-04-07 23:00:00' --end_date '2020-04-07 23:00:00' --task_regex '.*' cms_mysql_snapshot__hourly

--catch up hourly jobs to allow downstream daily jobs to run
airflow backfill --start_date '2020-04-07 16:00:00' --end_date '2020-04-07 16:00:00' --task_regex '.*' dwh__transactional__booking__hourly
airflow backfill --start_date '2020-04-07 23:00:00' --end_date '2020-04-07 23:00:00' --task_regex '.*' dwh__transactional__booking__hourly

--fill in missing runs with mark success
airflow backfill --start_date '2020-04-07 20:00:00' --end_date '2020-04-07 22:00:00' --task_regex '.*' -m incoming__cms_mysql__profile__hourly
airflow backfill --start_date '2020-04-07 17:00:00' --end_date '2020-04-07 22:00:00' --task_regex '.*' -m incoming__cms_mysql__reservation__hourly
airflow backfill --start_date '2020-04-07 19:00:00' --end_date '2020-04-07 22:00:00' --task_regex '.*' -m cms_mysql_snapshot__hourly
airflow backfill --start_date '2020-04-07 16:00:00' --end_date '2020-04-07 22:00:00' --task_regex '.*' -m hygiene_snapshots__cms_mysql__reservation__hourly



