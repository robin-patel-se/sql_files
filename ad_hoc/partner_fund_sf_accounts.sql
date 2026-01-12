SELECT *
FROM unload_vault_mvp_dev_carmen.ops.partner_fund_split_by_hotel__20200708t040000__daily_at_04h00__model_data AS a
WHERE a.refunded IS DISTINCT FROM TRUE -- already included in the CSVs so exclude these
  AND a.sf_view IN (
                    '**COVID-19 DACH P1/P2 Refusal View**',
                    '**COVID-19 DACH Parked View**',
                    '**COVID-19 UK/US and INTL Parked View**',
                    '**COVID-19 UK&INTL P1/P2 Refusal View**',
                    '**Social Media View**', '**Restrictions View**'
    )                                  -- not refunded
  -- all other eligibility rules we used for the CSVs continue to apply here
  AND LOWER(a.type) = 'hotel'
  AND a.check_in_date >= '2020-03-17'
  AND DATE_TRUNC('day', a.date_booked) <= '2020-05-01'
  AND LOWER(a.dynamic_flight_booked) = 'n'
  AND a.sf_account_id IS NOT NULL      -- we've matched it to SOME salesforce account
  AND a.is_partner_fund_company        -- we've matched it to the Partner Fund sheet
  AND a.is_dmc IS DISTINCT FROM 'DMC' -- DMCs will be handled manually
;


------------------------------------------------------------------------------------------------------------------------
--list of all sf accounts regardless of refunds
SELECT sf_account_id, company, country
FROM unload_vault_mvp_dev_carmen.ops.partner_fund_split_by_hotel__20200708t040000__daily_at_04h00__model_data AS a
WHERE

  -- all other eligibility rules we used for the CSVs continue to apply here
    LOWER(a.type) = 'hotel'
  AND a.check_in_date >= '2020-03-17'
  AND DATE_TRUNC('day', a.date_booked) <= '2020-05-01'
  AND LOWER(a.dynamic_flight_booked) = 'n'
  AND a.sf_account_id IS NOT NULL     -- we've matched it to SOME salesforce account
  AND a.is_partner_fund_company       -- we've matched it to the Partner Fund sheet
  AND a.is_dmc IS DISTINCT FROM 'DMC' -- DMCs will be handled manually
GROUP BY 1, 2, 3;

------------------------------------------------------------------------------------------------------------------------

--sf accounts with at least 1 refund
SELECT sf_account_id, company, country
FROM unload_vault_mvp_dev_carmen.ops.partner_fund_split_by_hotel__20200708t040000__daily_at_04h00__model_data AS a
WHERE a.refunded = TRUE

  -- all other eligibility rules we used for the CSVs continue to apply here
  AND LOWER(a.type) = 'hotel'
  AND a.check_in_date >= '2020-03-17'
  AND DATE_TRUNC('day', a.date_booked) <= '2020-05-01'
  AND LOWER(a.dynamic_flight_booked) = 'n'
  AND a.sf_account_id IS NOT NULL     -- we've matched it to SOME salesforce account
  AND a.is_partner_fund_company       -- we've matched it to the Partner Fund sheet
  AND a.is_dmc IS DISTINCT FROM 'DMC' -- DMCs will be handled manually
GROUP BY 1, 2, 3;

------------------------------------------------------------------------------------------------------------------------

--list of all sf accounts with NO refunds
WITH union_set AS (
    SELECT sf_account_id, company, country
    FROM unload_vault_mvp_dev_carmen.ops.partner_fund_split_by_hotel__20200708t040000__daily_at_04h00__model_data AS a
    WHERE

      -- all other eligibility rules we used for the CSVs continue to apply here
        LOWER(a.type) = 'hotel'
      AND a.check_in_date >= '2020-03-17'
      AND DATE_TRUNC('day', a.date_booked) <= '2020-05-01'
      AND LOWER(a.dynamic_flight_booked) = 'n'
      AND a.sf_account_id IS NOT NULL     -- we've matched it to SOME salesforce account
      AND a.is_partner_fund_company       -- we've matched it to the Partner Fund sheet
      AND a.is_dmc IS DISTINCT FROM 'DMC' -- DMCs will be handled manually
    GROUP BY 1, 2, 3

    MINUS

    SELECT sf_account_id, company, country
    FROM unload_vault_mvp_dev_carmen.ops.partner_fund_split_by_hotel__20200708t040000__daily_at_04h00__model_data AS a
    WHERE a.refunded = TRUE
      -- all other eligibility rules we used for the CSVs continue to apply here
      AND LOWER(a.type) = 'hotel'
      AND a.check_in_date >= '2020-03-17'
      AND DATE_TRUNC('day', a.date_booked) <= '2020-05-01'
      AND LOWER(a.dynamic_flight_booked) = 'n'
      AND a.sf_account_id IS NOT NULL     -- we've matched it to SOME salesforce account
      AND a.is_partner_fund_company       -- we've matched it to the Partner Fund sheet
      AND a.is_dmc IS DISTINCT FROM 'DMC' -- DMCs will be handled manually
    GROUP BY 1, 2, 3
)
SELECT *
FROM union_set
WHERE REGEXP_REPLACE(company, '\'', '') NOT IN
('Iberostar 70 Park Ave',
       'W Dubai The Palm',
       'Cassa Time Square',
       'Soibelmanns Hotel Alexandersbad',
       'Le Royal Meridien Abu Dhabi',
       'Bab Al Qasr Hotel & Residences',
       'Wyndham Dubai Marina',
       'Zanzibar Bay Resort',
       'Hotel du Tresor',
       'L Oriental Medina Riad & Spa',
       'Kempinski Hotel Muscat',
       'Dream Midtown',
       'Andaz Wall Street New York',
       'GLo Best Western Brooklyn NYC',
       'Movenpick Jumeirah Beach Hotel',
       'Hotel Restaurant Gierer',
       'Intercontinental Dubai Marina',
       'Waldorf Astoria Ras Al Khaimah',
       'Hotel Rossl',
       'Melia White House',
       'Riad Dar Lalla Fdila (HO)',
       'Hilton Naples',
       'Riad Clefs d Orient',
       'Caesars Resort Bluewaters Dubai',
       'Riad Azad',
       'Riad Miral',
       'Conrad Dubai',
       'Taj Atlas Wellness Boutique Hotel & Spa',
       'The Plymouth Miami Beach',
       'Reef & Beach Resort',
       'Filao Beach Zanzibar',
       'Millennium Hilton New York Downtown',
       'FIVE Jumeirah Village',
       'Fumba Beach Lodge',
       'Ona Hotels Terra',
       'Radisson Blu Hotel Dubai Waterfront',
       'Caesars Palace Bluewaters Dubai',
       'Hotel Beacon New York',
       'Zamek Kliczkow',
       'De Vere Tortworth Court',
       'Royalton Grenada',
       'White Hart Hotel',
       'Hotel Prinsenhof',
       'Hustyns Luxury Hotel & Spa',
       'The Talbot Hotel, Eatery and Coffee House',
       'Briig Boutique Hotel',
       'The Feathers Hotel',
       'Al Porto Suites',
       'New Hall Hotel & Spa',
       'The Castle Inn Hotel',
       'Mercure Warwickshire Walton Hall',
       'Novotel London ExCeL',
       'Ardencote Manor Country Club and Spa',
       'Hotel Post Am See')
;



--872 non refunded
--666 refunded
--54 not identifiable by sf account id


SELECT sf_account_id, company, country
FROM unload_vault_mvp_dev_carmen.ops.partner_fund_split_by_hotel__20200708t040000__daily_at_04h00__model_data AS a
WHERE a.refunded = TRUE

  -- all other eligibility rules we used for the CSVs continue to apply here
  AND LOWER(a.type) = 'hotel'
  AND a.check_in_date >= '2020-03-17'
  AND DATE_TRUNC('day', a.date_booked) <= '2020-05-01'
  AND LOWER(a.dynamic_flight_booked) = 'n'
  AND a.sf_account_id IS NOT NULL     -- we've matched it to SOME salesforce account
  AND a.is_partner_fund_company       -- we've matched it to the Partner Fund sheet
  AND a.is_dmc IS DISTINCT FROM 'DMC' -- DMCs will be handled manually
  AND REGEXP_REPLACE(company, '\'', '') NOT IN
      ('Iberostar 70 Park Ave',
       'W Dubai The Palm',
       'Cassa Time Square',
       'Soibelmanns Hotel Alexandersbad',
       'Le Royal Meridien Abu Dhabi',
       'Bab Al Qasr Hotel & Residences',
       'Wyndham Dubai Marina',
       'Zanzibar Bay Resort',
       'Hotel du Tresor',
       'L Oriental Medina Riad & Spa',
       'Kempinski Hotel Muscat',
       'Dream Midtown',
       'Andaz Wall Street New York',
       'GLo Best Western Brooklyn NYC',
       'Movenpick Jumeirah Beach Hotel',
       'Hotel Restaurant Gierer',
       'Intercontinental Dubai Marina',
       'Waldorf Astoria Ras Al Khaimah',
       'Hotel Rossl',
       'Melia White House',
       'Riad Dar Lalla Fdila (HO)',
       'Hilton Naples',
       'Riad Clefs d Orient',
       'Caesars Resort Bluewaters Dubai',
       'Riad Azad',
       'Riad Miral',
       'Conrad Dubai',
       'Taj Atlas Wellness Boutique Hotel & Spa',
       'The Plymouth Miami Beach',
       'Reef & Beach Resort',
       'Filao Beach Zanzibar',
       'Millennium Hilton New York Downtown',
       'FIVE Jumeirah Village',
       'Fumba Beach Lodge',
       'Ona Hotels Terra',
       'Radisson Blu Hotel Dubai Waterfront',
       'Caesars Palace Bluewaters Dubai',
       'Hotel Beacon New York',
       'Zamek Kliczkow',
       'De Vere Tortworth Court',
       'Royalton Grenada',
       'White Hart Hotel',
       'Hotel Prinsenhof',
       'Hustyns Luxury Hotel & Spa',
       'The Talbot Hotel, Eatery and Coffee House',
       'Briig Boutique Hotel',
       'The Feathers Hotel',
       'Al Porto Suites',
       'New Hall Hotel & Spa',
       'The Castle Inn Hotel',
       'Mercure Warwickshire Walton Hall',
       'Novotel London ExCeL',
       'Ardencote Manor Country Club and Spa',
       'Hotel Post Am See')
GROUP BY 1, 2, 3;

