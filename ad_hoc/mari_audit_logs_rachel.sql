SELECT DISTINCT
	key
FROM latest_vault.mari.taxes_audit_log tal,
	 LATERAL FLATTEN(INPUT => tal.record, OUTER => TRUE) element_sids
;


KEY
category
fields
source
TIMESTAMP


SELECT *
FROM collab.operations_pii.mari_user_log_audit_enhanced
WHERE log_date >= CURRENT_DATE - 10
  AND log_type = 'rate_plan_audit_log'



SELECT GET_DDL('table', 'collab.operations_pii.mari_user_log_audit_enhanced')
;


CREATE OR REPLACE VIEW mari_user_log_audit_enhanced
			(
			 log_date,
			 user_email,
			 hotel_code,
			 hotel_name,
			 sf_opportunity_id,
			 sale_start_date,
			 sale_end_date,
			 opportunity_cms_status,
			 inventory_increase_or_decrease,
			 rate_increase_or_decrease,
			 rates_from_value,
			 rates_to_value,
			 rack_rates_from_value,
			 rack_rates_to_value,
			 rate_date,
			 rate_release_period_increase_or_decrease,
			 rateplan_release_period_increase_or_decrease,
			 log_type,
			 room_type_audit_log_component_updated_names,
			 rate_plan_audit_log_component_updated_names,
			 rates_audit_log_component_updated_names,
			 is_bulk_update,
			 bulk_update_id,
			 discount_reset,
			 rate_plan_code,
			 input_mode,
			 mari_audit_action,
			 mari_audit_status,
			 last_updated_item_state,
			 mari_log_inventory_dates,
			 mari_log_room_type_codes
				)
AS
(
SELECT *
FROM dbt.bi_staging.stg_mari__user_log_audit_enhanced

	)
;


SELECT
	rpal.log_timestamp,
	rpal.record,
	rpal.record['fields']['components']['max_los']['to_value']        AS max_los,
	rpal.record['fields']['components']['min_los']['to_value']        AS min_los,
	rpal.record['fields']['components']['release_period']['to_value'] AS release_period
FROM latest_vault.mari.rate_plan_audit_log rpal
WHERE rpal.log_timestamp >= CURRENT_DATE - 1
;


-- list of who has added advanced restrictions


/*
{
  'source': 'ari-loader',
  'category': 'rate-plan',
  'timestamp': '2020-10-20T08:00:00',
  'fields': {
    'hotel_code': 'HOTEL',
    'room_type_code': 'ROOM',
    'rate_plan_code': 'RATE',
    'type': 'PER_NIGHT',
    'board_basis': 'FULL_BOARD',
    'is_cts': False,
    'user_email': 'test@secretescapes.com',
    'status': 'SUCCESS',
    'action': 'CREATED',
    'components': {
      'name': {
        'to_value': 'Rate'
      },
      'free_children': {
        'to_value': 0
      },
      'free_infants': {
        'to_value': 0
      },
      'release_period': {
        'to_value': 0
      },
      'room_type_code': {
        'to_value': 'ROOM'
      },
      'currency': {
        'to_value': 'GBP'
      },
      'min_los': {
        'to_value': 1
      },
      'max_los': {
        'to_value': 0
      },
      'advanced_restrictions': {
        'to_value': {
          'min_los': {
            'type': 'per_day_of_week',
            'value': {
              'friday': None,
              'monday': None,
              'saturday': None,
              'sunday': None,
              'thursday': None,
              'tuesday': 7,
              'wednesday': None
            }
          }
        }
      }
    }
  }
}*/



{
  "category": "rate-plan",
  "fields": {
    "action": "CREATED",
    "board_basis": "HALF_BOARD",
    "components": {
      "currency": {
        "to_value": "EUR"
      },
      "free_children": {
        "to_value": 0
      },
      "free_infants": {
        "to_value": 0
      },
      "max_los": {
        "to_value": 0
      },
      "min_los": {
        "to_value": 1
      },
      "name": {
        "to_value": "Junior Suite HB - Winter"
      },
      "release_period": {
        "to_value": 0
      },
      "room_type_code": {
        "to_value": "JS"
      }
    },
    "hotel_code": "001w000001VEhbE",
    "is_cts": FALSE,
    "rate_plan_code": "JSHW",
    "room_type_code": "JS",
    "status": "SUCCESS",
    "type": "PER_NIGHT",
    "user_email": "valentina.dellatorre@secretescapes.com"
  },
  "source": "ari-loader",
  "timestamp": "2024-03-05T09:00:01.942761"
}


-- https://mari.secretescapes.com/#/hotel/001Ve000002hI75/room/T/rate-plan/TRP

-- adjusted one hotel in production, wait until 7th march, if payload contains advanced_restrictions as expected then
-- we can assume no one has used the functionality

SELECT *
FROM latest_vault.mari.rate_plan_audit_log rpal
WHERE rpal.fields['hotel_code']::VARCHAR = '001Ve000002hI75';


{
  "category": "rate-plan",
  "fields": {
    "action": "CREATED",
    "board_basis": "BED_AND_BREAKFAST",
    "components": {
      "advanced_restrictions": {
        "to_value": {
          "max_los": {
            "type": "per_date_range",
            "value": [
              {
                "date_from": "02-01",
                "date_to": "02-01",
                "value": 2
              }
            ]
          },
          "min_los": {
            "type": "per_date_range",
            "value": [
              {
                "date_from": "02-01",
                "date_to": "02-01",
                "value": 1
              }
            ]
          }
        }
      },
      "currency": {
        "to_value": "GBP"
      },
      "free_children": {
        "to_value": 0
      },
      "free_infants": {
        "to_value": 0
      },
      "max_los": {
        "to_value": 0
      },
      "min_los": {
        "to_value": 1
      },
      "name": {
        "to_value": "Test rate plan"
      },
      "release_period": {
        "to_value": 0
      },
      "room_type_code": {
        "to_value": "T"
      }
    },
    "hotel_code": "001Ve000002hI75",
    "is_cts": false,
    "rate_plan_code": "TRP",
    "room_type_code": "T",
    "status": "SUCCESS",
    "type": "PER_NIGHT",
    "user_email": "talha.junaid@secretescapes.com"
  },
  "source": "ari-loader",
  "timestamp": "2024-03-06T11:42:37.737872"
}


SELECT *
FROM latest_vault.mari.rate_plan_audit_log rpal
WHERE rpal.fields LIKE '%advanced%'