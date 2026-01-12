SELECT dim.salesforce_opportunity_id,
       dim.posu_cluster,
       dim.posu_cluster_region,
       dim.posu_cluster_sub_region,
       dim.sale_start_date,
       oir.offer_name,
       oir.supplementary_inclusions_mari,
       oir.core_inclusions_mari,
       oir.currency_local,

       --need to convert to gbp
       oir.supplementary_inclusions_mari +
       oir.core_inclusions_mari AS total_inclusions, --from salesforce
       --need to convert to gbp
       oir.rate_local                                --from mari in supplier currency
FROM se.data.se_offers_inclusions_rates oir
    LEFT JOIN se.data.dim_sale dim
              ON oir.salesforce_opportunity_id = dim.salesforce_opportunity_id
WHERE dim.sale_active = TRUE;

SELECT oir.salesforce_opportunity_id,
       oir.deal_stage_rank,
       oir.deal_type,
       oir.hotel_id,
       oir.hotel_code,
       oir.hotel_name,
       oir.salesforce_hotel_name,
       oir.booking_com_name,
       oir.ota_posa,
       oir.offer_name,
       oir.room_type_name,
       oir.rate_plan_name,
       oir.rate_plan_rack_code,
       oir.hotel_rate_rack_code,
       oir.no_total_rooms,
       oir.no_available_rooms,
       oir.min_length_of_stay,
       oir.max_length_of_stay,
       oir.rate_type,
       oir.fixed_length_of_stay,
       oir.los,
       oir.rate_local_calculated,
       oir.occupancy_adults,
       oir.cms_offer_id,
       oir.salesforce_offer_id,
       oir.currency_local,
       oir.allocation_date,
       oir.rate_local,
       oir.single_rate_local,
       oir.room_type_ota_name,
       oir.board_basis,
       oir.inclusion_level_agg,
       oir.inclusion_type_agg,
       oir.currency_code_local_agg,
       oir.inclusion_value_local_agg,
       oir.core_inclusions_mari,
       oir.supplementary_inclusions_mari,
       oir.core_per_person_first_night,
       oir.core_per_person_per_stay,
       oir.core_per_person_per_day,
       oir.core_per_room_first_night,
       oir.core_per_room_per_stay,
       oir.core_per_room_per_day,
       oir.supplementary_per_person_first_night,
       oir.supplementary_per_person_per_stay,
       oir.supplementary_per_person_per_day,
       oir.supplementary_per_room_first_night,
       oir.supplementary_per_room_per_stay,
       oir.supplementary_per_room_per_day,
       oir.fornova_validation_check_hotel_id,
       oir.fornova_validation_check_currency_local,
       oir.fornova_validation_check_booking_com_name,
       oir.fornova_validation_check_ota_posa,
       oir.fornova_validation_check_los,
       oir.fornova_validation_check_rate_local_calculated,
       oir.fornova_validation_check_room_type_ota_name,
       oir.fornova_validation_check_allocation_date,
       oir.fornova_validation_check_deal_type,
       oir.fornova_validation_check_board_basis,
       oir.fornova_validation_check
FROM se.data.se_offers_inclusions_rates oir


SELECT dim.salesforce_opportunity_id,
       dim.posu_cluster,
       dim.posu_cluster_region,
       dim.posu_cluster_sub_region,
       dim.sale_start_date,
       oir.offer_name,
       oir.supplementary_inclusions_mari,
       oir.core_inclusions_mari,
       oir.currency_local,

       --need to convert to gbp
       oir.supplementary_inclusions_mari +
       oir.core_inclusions_mari AS total_inclusions, --from salesforce
       --need to convert to gbp
       oir.rate_local                                --from mari in supplier currency
FROM se.data.se_offers_inclusions_rates oir
    LEFT JOIN se.data.dim_sale dim
              ON oir.salesforce_opportunity_id = dim.salesforce_opportunity_id
WHERE dim.sale_active = TRUE
  AND dim.salesforce_opportunity_id;

-- trying to find if it has at least one offer that has total inclusion value of more than 20% of ADR/AOV


SELECT oir.offer_name,
       oir.supplementary_inclusions_mari,
       oir.core_inclusions_mari,
       oir.currency_local,
       --need to convert to gbp
       oir.supplementary_inclusions_mari +
       oir.core_inclusions_mari AS total_inclusions, --from salesforce
       --need to convert to gbp
       oir.rate_local                                --from mari in supplier currency
FROM se.data.se_offers_inclusions_rates oir

SELECT oir.allocation_date,
       oir.rate_local,
       oir.rate_local_calculated,
       oir.inclusion_type_agg,
       oir.core_inclusions_mari,
       oir.rate_type,
       oir.rate_plan_name,
       oir.min_length_of_stay,
       oir.currency_local,
       oir.rate_local,
       oir.rate_local_calculated
FROM se.data.se_offers_inclusions_rates oir
WHERE oir.salesforce_opportunity_id = '0066900001Vl6mx'
  AND oir.allocation_date >= CURRENT_DATE
ORDER BY allocation_date;


SELECT ssa.hotel_code
FROM se.data.se_sale_attributes ssa
WHERE ssa.salesforce_opportunity_id = '0066900001Vl6mx';


SELECT shrar.date,
       shrar.hotel_code,
       shrar.hotel_name,
       shrar.available_lead_rate_gbp,
       shrar.lead_rate_gbp,
       shrar.rate_currency,
       shrar.available_lead_rate_rc,
       shrar.lead_rate_rc,
       shrar.lead_rate_plan_code,
       shso.offer_id,
       shso.se_offer_id,
       ssa.se_sale_id,
       ssa.salesforce_opportunity_id,
       soi.inclusion_value_local,
       soi.inclusion_value_gbp
FROM se.data.se_hotel_rooms_and_rates shrar
    LEFT JOIN se.data.se_cms_mari_link scml ON shrar.hotel_code = scml.hotel_code AND shrar.lead_rate_plan_code = scml.rate_code
    LEFT JOIN se.data.se_hotel_sale_offer shso ON scml.offer_id = shso.offer_id
    LEFT JOIN se.data.se_sale_attributes ssa ON shso.se_sale_id = ssa.se_sale_id
    LEFT JOIN se.data.salesforce_offer_inclusion soi ON shso.offer_id = soi.cms_manual_offer_id AND soi.offer_data_model = 'New Data Model'
WHERE ssa.salesforce_opportunity_id = '0066900001Vl6mx';

SELECT *
FROM se.data.salesforce_offer_inclusion soi
WHERE soi.salesforce_opportunity_id = '0066900001Vl6mx';




