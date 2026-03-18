biapp/archive/task_catalogue/staging/hygiene/sfsc/rebooking_request_cases.py:# dep: ProductionIngestOperation=incoming.sfsc.salesforce_cases::incoming__sfsc__salesforce_cases__daily_at_04h00
biapp/bau/sort_dependencies/sort_dependencies.py:# dep: LatestRecordsOperation=incoming.sfsc.user::incoming__sfsc__user__daily_at_01h00
biapp/task_catalogue/dv/dwh/master_booking_list/master_se_booking_list.py:# dep: LatestRecordsOperation=incoming.sfsc.salesforce_cases::incoming__sfsc__salesforce_cases__daily_at_04h00
biapp/task_catalogue/dv/dwh/master_booking_list/master_tb_booking_list.py:# dep: LatestRecordsOperation=incoming.sfsc.salesforce_cases::incoming__sfsc__salesforce_cases__daily_at_04h00
biapp/task_catalogue/dv/dwh/sfsc/account.py:# dep: LatestRecordsOperation=incoming.sfsc.account_a_l::incoming__sfsc__account_a_l__hourly
biapp/task_catalogue/dv/dwh/sfsc/account.py:# dep: LatestRecordsOperation=incoming.sfsc.account_m_z::incoming__sfsc__account_m_z__hourly
biapp/task_catalogue/dv/dwh/sfsc/agent_work.py:# dep: LatestRecordsOperation=incoming.sfsc.agent_work::incoming__sfsc__agent_work__daily_at_01h00
biapp/task_catalogue/dv/dwh/sfsc/agent_work.py:# dep: LatestRecordsOperation=incoming.sfsc.user::incoming__sfsc__user__daily_at_01h00
biapp/task_catalogue/dv/dwh/sfsc/cases_enhanced.py:# dep: LatestRecordsOperation=incoming.sfsc.case::incoming__sfsc__case__daily_at_01h00
biapp/task_catalogue/dv/dwh/sfsc/cases_enhanced.py:# dep: LatestRecordsOperation=incoming.sfsc.user::incoming__sfsc__user__daily_at_01h00
biapp/task_catalogue/dv/dwh/sfsc/cases_enhanced.py:# dep: LatestRecordsOperation=incoming.sfsc.inclusion::incoming__sfsc__inclusion__daily_at_01h00
biapp/task_catalogue/dv/dwh/sfsc/live_chat_transcript.py:# dep: LatestRecordsOperation=incoming.sfsc.live_chat_transcript::incoming__sfsc__live_chat_transcript__daily_at_01h00
biapp/task_catalogue/dv/dwh/sfsc/offer_inclusion.py:# dep: LatestRecordsOperation=incoming.sfsc.inclusion::incoming__sfsc__inclusion__daily_at_01h00
biapp/task_catalogue/dv/dwh/sfsc/offer_inclusion.py:# dep: LatestRecordsOperation=incoming.sfsc.offers::incoming__sfsc__offers__daily_at_01h00
biapp/task_catalogue/dv/dwh/sfsc/opportunity.py:# dep: LatestRecordsOperation=incoming.sfsc.opportunity_a_l::incoming__sfsc__opportunity_a_l__hourly
biapp/task_catalogue/dv/dwh/sfsc/opportunity.py:# dep: LatestRecordsOperation=incoming.sfsc.opportunity_m_z::incoming__sfsc__opportunity_m_z__hourly
biapp/task_catalogue/dv/dwh/sfsc/opportunity_daily.py:# dep: LatestRecordsOperation=incoming.sfsc.opportunity_a_l::incoming__sfsc__opportunity_a_l__hourly
biapp/task_catalogue/dv/dwh/sfsc/opportunity_daily.py:# dep: LatestRecordsOperation=incoming.sfsc.opportunity_m_z::incoming__sfsc__opportunity_m_z__hourly
biapp/task_catalogue/dv/dwh/sfsc/rebooking_request_cases.py:# dep: LatestRecordsOperation=incoming.sfsc.salesforce_cases::incoming__sfsc__salesforce_cases__daily_at_04h00
biapp/task_catalogue/dv/dwh/sfsc/salesforce_sale_opportunity.py:# dep: LatestRecordsOperation=incoming.sfsc.user::incoming__sfsc__user__daily_at_01h00
biapp/task_catalogue/dv/dwh/sfsc/vonage_call_summary.py:# dep: LatestRecordsOperation=incoming.sfsc.nvm_call_summary::incoming__sfsc__nvm_call_summary__daily_at_01h00
biapp/task_catalogue/dv/dwh/sfsc/vonage_call_summary.py:# dep: LatestRecordsOperation=incoming.sfsc.user::incoming__sfsc__user__daily_at_01h00
biapp/task_catalogue/dv/dwh/transactional/global_sale_attributes.py:# dep: LatestRecordsOperation=incoming.sfsc.user::incoming__sfsc__user__daily_at_01h00
biapp/task_catalogue/dv/dwh/transactional/sale_component.py:# dep: LatestRecordsOperation=incoming.sfsc.package_destination::incoming__sfsc__package_destination__daily_at_01h00
biapp/task_catalogue/dv/dwh/transactional/sale_component.py:# dep: LatestRecordsOperation=incoming.sfsc.user::incoming__sfsc__user__daily_at_01h00
biapp/task_catalogue/dv/dwh/transactional/se_booking.py:# dep: LatestRecordsOperation=incoming.sfsc.offers::incoming__sfsc__offers__daily_at_01h00
biapp/task_catalogue/se/data/sfsc/salesforce_cases.py:# soft dep: LatestRecordsOperation=incoming.sfsc.salesforce_cases::incoming__sfsc__salesforce_cases__daily_at_04h00



salesforce_cases
user
salesforce_cases
salesforce_cases
account_a_l__hourly
account_m_z__hourly
agent_work
user
case
user
inclusion
live_chat_transcript
inclusion
offers
opportunity_a_l__hourly
opportunity_m_z__hourly
opportunity_a_l__hourly
opportunity_m_z__hourly
salesforce_cases
user
nvm_call_summary
user
user
package_destination
user
offers
salesforce_cases


inspect_dependencies biapp/manifests/incoming/cms_mysql/affiliate.json --downstream

inspect_dependencies biapp/manifests/incoming/sfsc/account_a_l.json --downstream


biapp/manifests/incoming/sfsc/account_a_l.json
biapp/manifests/incoming/sfsc/account_accommodation.json
biapp/manifests/incoming/sfsc/account_m_z.json
biapp/manifests/incoming/sfsc/agent_work.json
biapp/manifests/incoming/sfsc/assigned_attribute.json
biapp/manifests/incoming/sfsc/assigned_fact.json
biapp/manifests/incoming/sfsc/attribute.json
biapp/manifests/incoming/sfsc/case.json
biapp/manifests/incoming/sfsc/copy_requirements.json
biapp/manifests/incoming/sfsc/fact_group_fact.json
biapp/manifests/incoming/sfsc/fact_group.json
biapp/manifests/incoming/sfsc/fact.json
biapp/manifests/incoming/sfsc/inclusion.json
biapp/manifests/incoming/sfsc/live_chat_transcript_event.json
biapp/manifests/incoming/sfsc/live_chat_transcript.json
biapp/manifests/incoming/sfsc/nvm_call_summary.json
biapp/manifests/incoming/sfsc/offers.json
biapp/manifests/incoming/sfsc/opportunity_a_l.json
biapp/manifests/incoming/sfsc/opportunity_accommodation.json
biapp/manifests/incoming/sfsc/opportunity_m_z.json
biapp/manifests/incoming/sfsc/package_destination.json
biapp/manifests/incoming/sfsc/record_type.json
biapp/manifests/incoming/sfsc/salesforce_cases.json
biapp/manifests/incoming/sfsc/task.json
biapp/manifests/incoming/sfsc/unit.json
biapp/manifests/incoming/sfsc/user.json



biapp/archive/task_catalogue/dv/dwh/trustyou/se_trustyou_matched_properties.py:# dep: SelfDescribingOperation=dv/dwh/sfsc/account.py::dwh__sfsc__account__hourly
biapp/archive/task_catalogue/staging/outgoing/chiasma/salesforce_deal_attributes/modelling.py:# dep: SelfDescribingOperation=dv/dwh/sfsc/account.py::dwh__sfsc__account__hourly
biapp/archive/task_catalogue/staging/outgoing/trustyou/property_input_mapping/modelling.py:# dep: SelfDescribingOperation=dv/dwh/sfsc/account.py::dwh__sfsc__account__hourly
biapp/task_catalogue/dv/dwh/holibob/camilla/booking_events.py:# dep: SelfDescribingOperation=dv/dwh/sfsc/account.py::dwh__sfsc__account__hourly
biapp/task_catalogue/dv/dwh/juniper/juniper_to_se_property_matching.py:# dep: SelfDescribingOperation=dv/dwh/sfsc/account.py::dwh__sfsc__account__hourly
biapp/task_catalogue/dv/dwh/sfsc/cases_enhanced.py:# dep: SelfDescribingOperation=dv/dwh/sfsc/account.py::dwh__sfsc__account__hourly
biapp/task_catalogue/dv/dwh/sfsc/offer_inclusion.py:# dep: SelfDescribingOperation=dv/dwh/sfsc/account.py::dwh__sfsc__account__hourly
biapp/task_catalogue/dv/dwh/sfsc/salesforce_sale_opportunity.py:# dep: SelfDescribingOperation=dv/dwh/sfsc/account.py::dwh__sfsc__account__hourly
biapp/task_catalogue/dv/dwh/transactional/sale_component.py:# dep: SelfDescribingOperation=dv/dwh/sfsc/account.py::dwh__sfsc__account__hourly
biapp/task_catalogue/dv/dwh/transactional/se_sale.py:# dep: SelfDescribingOperation=dv/dwh/sfsc/account.py::dwh__sfsc__account__hourly
biapp/task_catalogue/dv/dwh/transactional/se_sale__old_data_model.py:# dep: SelfDescribingOperation=dv/dwh/sfsc/account.py::dwh__sfsc__account__hourly
biapp/task_catalogue/dv/finance/netsuite_tnt/vendor.py:# dep: SelfDescribingOperation=dv/dwh/sfsc/account.py::dwh__sfsc__account__hourly
biapp/task_catalogue/se/data_pii/sfsc/sfsc_account_object.py:# soft_dep: SelfDescribingOperation=dv/dwh/sfsc/account.py::dwh__sfsc__account__hourly



biapp/archive/task_catalogue/dv/fornova/se_offers_inclusions_rates_old.py:# dep: SelfDescribingOperation=dv/dwh/sfsc/offer_inclusion.py::dwh__sfsc__offer_inclusion__daily_at_03h00
biapp/task_catalogue/dv/dwh/fornova/se_offers_inclusions_rates.py:# dep: SelfDescribingOperation=dv/dwh/sfsc/offer_inclusion.py::dwh__sfsc__offer_inclusion__daily_at_03h00
biapp/task_catalogue/dv/dwh/sale/opportunity_clustering.py:# dep: SelfDescribingOperation=dv/dwh/sfsc/offer_inclusion.py::dwh__sfsc__offer_inclusion__daily_at_03h00
biapp/task_catalogue/dv/dwh/sale/sale_clustering.py:# dep: SelfDescribingOperation=dv/dwh/sfsc/offer_inclusion.py::dwh__sfsc__offer_inclusion__daily_at_03h00
biapp/task_catalogue/dv/dwh/sfsc/offer_inclusion.py:# dag_id=dwh__sfsc__offer_inclusion__daily_at_03h00
biapp/task_catalogue/dv/dwh/sfsc/offer_inclusion_active_snapshot.py:# dep: SelfDescribingOperation=dv/dwh/sfsc/offer_inclusion.py::dwh__sfsc__offer_inclusion__daily_at_03h00
biapp/task_catalogue/se/data/sfsc/salesforce_offer_inclusion.py:# soft_dep: SelfDescribingOperation=dv/dwh/sfsc/offer_inclusion.py::dwh__sfsc__offer_inclusion__daily_at_03h00
biapp/task_catalogue/staging/outgoing/phoenix/salesforce_inclusions/modelling.py:# dep: SelfDescribingOperation=dv/dwh/sfsc/offer_inclusion.py::dwh__sfsc__offer_inclusion__daily_at_03h00




biapp/archive/task_catalogue/dv/fornova/se_offers_inclusions_rates_old.py:# dep: SelfDescribingOperation=dv/dwh/sfsc/opportunity.py::dwh__sfsc__opportunity__hourly
biapp/archive/task_catalogue/staging/outgoing/chiasma/salesforce_deal_attributes/modelling.py:# dep: SelfDescribingOperation=dv/dwh/sfsc/opportunity.py::dwh__sfsc__opportunity__hourly
biapp/archive/task_catalogue/staging/outgoing/ops/partner_fund_split_by_hotel/modelling.py:# dep: SelfDescribingOperation=dv/dwh/sfsc/opportunity.py::dwh__sfsc__opportunity__hourly
biapp/archive/task_catalogue/staging/outgoing/sfsc/top_up_emails/modelling.py:# dep: SelfDescribingOperation=dv/dwh/sfsc/opportunity.py::dwh__sfsc__opportunity__hourly
biapp/task_catalogue/dv/dwh/fornova/price_comparison_for_se.py:# dep: SelfDescribingOperation=dv/dwh/sfsc/opportunity.py::dwh__sfsc__opportunity__hourly
biapp/task_catalogue/dv/dwh/sfsc/cases_enhanced.py:# dep: SelfDescribingOperation=dv/dwh/sfsc/opportunity.py::dwh__sfsc__opportunity__hourly
biapp/task_catalogue/dv/dwh/sfsc/offer_inclusion.py:# dep: SelfDescribingOperation=dv/dwh/sfsc/opportunity.py::dwh__sfsc__opportunity__hourly
biapp/task_catalogue/dv/dwh/sfsc/opportunity.py:# dag_id=dwh__sfsc__opportunity__hourly
biapp/task_catalogue/dv/dwh/sfsc/salesforce_sale_opportunity.py:# dep: SelfDescribingOperation=dv/dwh/sfsc/opportunity.py::dwh__sfsc__opportunity__hourly
biapp/task_catalogue/dv/dwh/transactional/dim_sale.py:# dep: SelfDescribingOperation=dv/dwh/sfsc/opportunity.py::dwh__sfsc__opportunity__hourly
biapp/task_catalogue/dv/dwh/transactional/sale_component.py:# dep: SelfDescribingOperation=dv/dwh/sfsc/opportunity.py::dwh__sfsc__opportunity__hourly
biapp/task_catalogue/dv/dwh/transactional/se_sale.py:# dep: SelfDescribingOperation=dv/dwh/sfsc/opportunity.py::dwh__sfsc__opportunity__hourly
biapp/task_catalogue/dv/dwh/transactional/se_sale__old_data_model.py:# dep: SelfDescribingOperation=dv/dwh/sfsc/opportunity.py::dwh__sfsc__opportunity__hourly
biapp/task_catalogue/se/data_pii/sfsc/sfsc_opportunity_object.py:# soft_dep: SelfDescribingOperation=dv/dwh/sfsc/opportunity.py::dwh__sfsc__opportunity__hourly



biapp/task_catalogue/dv/dwh/fornova/se_offers_inclusions_rates.py:# dep: SelfDescribingOperation=dv/dwh/sfsc/opportunity_daily.py::dwh__sfsc__opportunity__daily_at_01h00
biapp/task_catalogue/dv/dwh/sfsc/opportunity_daily.py:# dag_id=dwh__sfsc__opportunity__daily_at_01h00
biapp/task_catalogue/se/data_pii/sfsc/sfsc_opportunity_object_daily.py:# soft_dep: SelfDescribingOperation=dv/dwh/sfsc/opportunity_daily.py::dwh__sfsc__opportunity__daily_at_01h00



biapp/archive/task_catalogue/dv/dwh/trustyou/se_trustyou_matched_properties.py:# dep: SelfDescribingOperation=dv/dwh/transactional/global_sale_attributes.py::dwh__transactional__global_sale_attributes__daily_at_03h00
biapp/archive/task_catalogue/staging/outgoing/trustyou/property_input_mapping/modelling.py:# dep: SelfDescribingOperation=dv/dwh/transactional/global_sale_attributes.py::dwh__transactional__global_sale_attributes__daily_at_03h00
biapp/task_catalogue/dv/bi/tableau/deal_count_model/deal_count.py:# dep: SelfDescribingOperation=dv/dwh/transactional/global_sale_attributes.py::dwh__transactional__global_sale_attributes__daily_at_03h00
biapp/task_catalogue/dv/bi/tableau/deal_model/dim_sale_territory.py:# dep: SelfDescribingOperation=dv/dwh/transactional/global_sale_attributes.py::dwh__transactional__global_sale_attributes__daily_at_03h00
biapp/task_catalogue/dv/bi/tableau/deal_model/global_sale_deal_segmentation.py:# dep: SelfDescribingOperation=dv/dwh/transactional/global_sale_attributes.py::dwh__transactional__global_sale_attributes__daily_at_03h00
biapp/task_catalogue/dv/dwh/holibob/camilla/booking_events.py:# dep: SelfDescribingOperation=dv/dwh/transactional/global_sale_attributes.py::dwh__transactional__global_sale_attributes__daily_at_03h00
biapp/task_catalogue/dv/dwh/juniper/juniper_to_se_property_matching.py:# dep: SelfDescribingOperation=dv/dwh/transactional/global_sale_attributes.py::dwh__transactional__global_sale_attributes__daily_at_03h00
biapp/task_catalogue/dv/dwh/sale/opportunity_clustering.py:# dep: SelfDescribingOperation=dv/dwh/transactional/global_sale_attributes.py::dwh__transactional__global_sale_attributes__daily_at_03h00
biapp/task_catalogue/dv/dwh/sale/sale_clustering.py:# dep: SelfDescribingOperation=dv/dwh/transactional/global_sale_attributes.py::dwh__transactional__global_sale_attributes__daily_at_03h00
biapp/task_catalogue/dv/dwh/transactional/global_sale_attributes.py:# dag_id=dwh__transactional__global_sale_attributes__daily_at_03h00
biapp/task_catalogue/dv/dwh/transactional/global_sale_attributes_snapshot.py:# dep: SelfDescribingOperation=dv/dwh/transactional/global_sale_attributes.py::dwh__transactional__global_sale_attributes__daily_at_03h00
biapp/task_catalogue/dv/dwh/transactional/tb_offer.py:# dep: SelfDescribingOperation=dv/dwh/transactional/global_sale_attributes.py::dwh__transactional__global_sale_attributes__daily_at_03h00
biapp/task_catalogue/dv/dwh/user_attributes/user_booking_metrics.py:# dep: SelfDescribingOperation=dv/dwh/transactional/global_sale_attributes.py::dwh__transactional__global_sale_attributes__daily_at_03h00
biapp/task_catalogue/se/data/dwh/global_sale_attributes.py:# soft_dep: SelfDescribingOperation=dv/dwh/transactional/global_sale_attributes.py::dwh__transactional__global_sale_attributes__daily_at_03h00
biapp/task_catalogue/se/data_pii/dwh/global_sale_attributes.py:# soft_dep: SelfDescribingOperation=dv/dwh/transactional/global_sale_attributes.py::dwh__transactional__global_sale_attributes__daily_at_03h00




biapp/archive/task_catalogue/dv/finance/aviate/transactions.py:# dep: SelfDescribingOperation=dv/dwh/transactional/tb_booking.py::dwh__transactional__booking__daily_at_00h30
biapp/archive/task_catalogue/dv/finance/aviate/transactions_duplicate_snapshot.py:# dep: SelfDescribingOperation=dv/dwh/transactional/tb_booking.py::dwh__transactional__booking__daily_at_00h30
biapp/archive/task_catalogue/dv/finance/cash_flow/travel_trust/secret_escapes/se_travel_trust_money_out_flight.py:# dep: SelfDescribingOperation=dv/dwh/transactional/se_booking.py::dwh__transactional__booking__daily_at_00h30
biapp/archive/task_catalogue/dv/finance/cash_flow/travel_trust/secret_escapes/se_travel_trust_money_out_refund.py:# dep: SelfDescribingOperation=dv/dwh/transactional/se_booking.py::dwh__transactional__booking__daily_at_00h30
biapp/archive/task_catalogue/dv/finance/cash_flow/travel_trust/secret_escapes/se_travel_trust_money_out_transaction_discrepancy.py:# dep: SelfDescribingOperation=dv/dwh/transactional/se_booking.py::dwh__transactional__booking__daily_at_00h30
biapp/archive/task_catalogue/dv/finance/cash_flow/travel_trust/secret_escapes/se_travel_trust_money_out_travelled.py:# dep: SelfDescribingOperation=dv/dwh/transactional/se_booking.py::dwh__transactional__booking__daily_at_00h30
biapp/archive/task_catalogue/dv/finance/cash_flow/travel_trust/travelbird/tb_travel_trust_money_out_flight.py:# dep: SelfDescribingOperation=dv/dwh/transactional/tb_booking.py::dwh__transactional__booking__daily_at_00h30
biapp/archive/task_catalogue/dv/finance/cash_flow/travel_trust/travelbird/tb_travel_trust_money_out_refund.py:# dep: SelfDescribingOperation=dv/dwh/transactional/tb_booking.py::dwh__transactional__booking__daily_at_00h30
biapp/archive/task_catalogue/dv/finance/cash_flow/travel_trust/travelbird/tb_travel_trust_money_out_transaction_discrepancy.py:# dep: SelfDescribingOperation=dv/dwh/transactional/tb_booking.py::dwh__transactional__booking__daily_at_00h30
biapp/archive/task_catalogue/dv/finance/cash_flow/travel_trust/travelbird/tb_travel_trust_money_out_travelled.py:# dep: SelfDescribingOperation=dv/dwh/transactional/tb_booking.py::dwh__transactional__booking__daily_at_00h30
biapp/archive/task_catalogue/dv/finance/netsuite/billing_harmonised_data_base.py:# dep: SelfDescribingOperation=dv/dwh/transactional/booking.py::dwh__transactional__booking__daily_at_00h30
biapp/archive/task_catalogue/dv/finance/netsuite/billing_harmonised_data_base.py:# dep: SelfDescribingOperation=dv/dwh/flight_service/orders_orderchange.py::dwh__transactional__booking__daily_at_00h30
biapp/archive/task_catalogue/dv/finance/netsuite/booking.py:# dep: SelfDescribingOperation=dv/dwh/transactional/booking.py::dwh__transactional__booking__daily_at_00h30
biapp/archive/task_catalogue/dv/finance/netsuite/invoicing_harmonised_data_base.py:# dep: SelfDescribingOperation=dv/dwh/transactional/booking.py::dwh__transactional__booking__daily_at_00h30
biapp/archive/task_catalogue/dv/finance/netsuite/invoicing_harmonised_data_derived.py:# dep: SelfDescribingOperation=dv/dwh/transactional/booking.py::dwh__transactional__booking__daily_at_00h30
biapp/archive/task_catalogue/dv/finance/netsuite/journals_invoice_harmonised_data.py:# dep: SelfDescribingOperation=dv/dwh/transactional/se_booking.py::dwh__transactional__booking__daily_at_00h30
biapp/archive/task_catalogue/dv/finance/netsuite/journals_invoice_harmonised_data.py:# dep: SelfDescribingOperation=dv/dwh/transactional/tb_booking.py::dwh__transactional__booking__daily_at_00h30
biapp/archive/task_catalogue/dv/finance/worldpay/worldpay_cash_on_booking.py:# dep: SelfDescribingOperation=dv/dwh/transactional/se_booking.py::dwh__transactional__booking__daily_at_00h30
biapp/archive/task_catalogue/dv/finance/worldpay/worldpay_chargeback.py:# soft_dep: SelfDescribingOperation=dv/dwh/transactional/se_booking.py::dwh__transactional__booking__daily_at_00h30
biapp/archive/task_catalogue/dv/finance/worldpay/worldpay_chargeback_event_log.py:# soft_dep: SelfDescribingOperation=dv/dwh/transactional/se_booking.py::dwh__transactional__booking__daily_at_00h30
biapp/archive/task_catalogue/dv/finance/worldpay/worldpay_refund.py:# soft_dep: SelfDescribingOperation=dv/dwh/transactional/se_booking.py::dwh__transactional__booking__daily_at_00h30
biapp/archive/task_catalogue/se/finance/cash_flow/travel_trust_booking_components.py:# soft_dep: SelfDescribingOperation=dv/dwh/transactional/tb_booking.py::dwh__transactional__booking__daily_at_00h30
biapp/task_catalogue/dv/bi/tableau/deal_model/fact_sale_metrics.py:# dep: SelfDescribingOperation=dv/dwh/transactional/tb_booking.py::dwh__transactional__booking__daily_at_00h30
biapp/task_catalogue/dv/bi/tableau/deal_model/room_type_metrics.py:# dep: SelfDescribingOperation=dv/dwh/transactional/se_booking.py::dwh__transactional__booking__daily_at_00h30
biapp/task_catalogue/dv/bi/tableau/site_funnels/site_funnels.py:# dep: SelfDescribingOperation=dv/dwh/transactional/tb_booking.py::dwh__transactional__booking__daily_at_00h30


SELECT * FROM se.data.dim_sale ds;