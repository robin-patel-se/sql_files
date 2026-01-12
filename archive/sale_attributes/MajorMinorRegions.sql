/*
Mappings of clusters & country into Major and Minor Regions

Created a line for each output item to allow for easier editing in future

Duplicated definitions between major and minor regions to make minor region not be dependent on major region.
Would simplify code if they were dependent.
*/

SELECT ss.se_sale_id,
       ss.base_sale_id,
       ss.sale_id,
       ss.salesforce_opportunity_id,
       ss.sale_name,
       ss.sale_name_object,
       ss.sale_active,
       ss.class,
       ss.has_flights_available,
       ss.default_preferred_airport_code,
       ss.type,
       ss.hotel_chain_link,
       ss.closest_airport_code,
       ss.is_team20package,
       ss.sale_able_to_sell_flights,
       ss.sale_product,
       ss.sale_type,
       ss.product_type,
       ss.product_configuration,
       ss.product_line,
       ss.data_model,
       ss.hotel_location_info_id,
       ss.active,
       ss.default_hotel_offer_id,
       ss.commission,
       ss.commission_type,
       ss.original_contractor_id,
       ss.original_contractor_name,
       ss.original_joint_contractor_id,
       ss.original_joint_contractor_name,
       ss.current_contractor_id,
       ss.current_contractor_name,
       ss.current_joint_contractor_id,
       ss.current_joint_contractor_name,
       ss.date_created,
       ss.destination_type,
       ss.start_date,
       ss.end_date,
       ss.hotel_id,
       ss.base_currency,
       ss.city_district_id,
       ss.company_id,
       ss.company_name,
       ss.hotel_code,
       ss.latitude,
       ss.longitude,
       ss.location_info_id,
       ss.posa_territory,
       ss.posa_country,
       ss.posa_currency,
       ss.posu_division,
       ss.posu_country,
       ss.posu_city,
       ss.supplier_id,
       ss.supplier_name,
       ss.deal_category,
       ss.travel_type,
       ss.salesforce_opportunity_id_full,
       ss.salesforce_account_id,
       ss.deal_profile,
       ss.salesforce_proposed_start_date,
       ss.salesforce_deal_label_multi,
       ss.salesforce_stage_name,
       ss.promotion_label,
       ss.promotion_description,
       ss.se_api_lead_rate,
       ss.se_api_lead_rate_per_person,
       ss.se_api_currency,
       ss.se_api_show_discount,
       ss.se_api_show_prices,
       ss.se_api_discount,
       ss.se_api_url,
       COALESCE(pc.posu_sub_region, 'Other') AS posu_sub_region,
       COALESCE(pc.posu_region, 'Other')     AS posu_region,
       COALESCE(pc.posu_cluster, 'Other')    AS posu_cluster,
       CASE
           WHEN posu_cluster = COALESCE(pc.posu_cluster, 'Other') = '1' THEN
               CASE
                   WHEN ss.posu_country IN ('England', 'Scotland', 'Wales/Cymru', 'Northern Ireland', 'Republic Of Ireland', 'Ireland' ) THEN 'UK'
                   WHEN ss.posu_country IN ('France', 'Monaco') THEN 'France'
                   WHEN (ss.posu_country IN ('Denmark', 'Sweden') AND ss.posu_city NOT IN ('Arjeplog', 'Gallivare Kommun', 'Jokkmokk', 'Kiruna', 'Lund', 'Lycksele', 'Skelleftea', 'Vidsel' ))
                       OR (ss.posu_country = 'Norway' AND ss.posu_city IN ('Gardermoen', 'Oslo', 'Oslo County', 'sundvolden'))
                       THEN 'Scandi'
                   WHEN (ss.posu_country IN ('Finland', 'Iceland', 'Norway', 'Greenland', 'Aland Islands', 'Faroe Islands', 'Svalbard And Jan Mayen' )
                       OR (ss.posu_country = 'Sweden'
                           AND ss.posu_city IN ('Arjeplog', 'Gallivare Kommun', 'Jokkmokk', 'Kiruna', 'Lund', 'Lycksele', 'Skelleftea', 'Vidsel'))) THEN 'Arctic'
                   WHEN ss.posu_country IN ('Switzerland') THEN 'Switzerland (French speaking)'
                   END

           WHEN posu_cluster = COALESCE(pc.posu_cluster, 'Other') = '2' THEN
               CASE
                   WHEN ss.posu_country IN ('Germany') THEN 'Germany'
                   WHEN ss.posu_country IN ('Austria', 'Liechtenstein') THEN 'Austria'
                   WHEN ss.posu_country IN ('Switzerland') THEN 'Switzerland'
                   WHEN ss.posu_country IN ('Belgium', 'Luxemburg') THEN 'Belgium'
                   WHEN ss.posu_country IN ('Netherlands') THEN 'Netherlands'
                   WHEN ss.posu_country IN
                        ('Russia', 'Poland', 'Slovakia', 'Hungary', 'Czech Republic', 'Armenia', 'Estonia', 'Georgia', 'Belarus',
                         'Latvia', 'Lithuania', 'Ukraine') THEN 'Non-Balkan CEE'
                   WHEN ss.posu_country IN
                        ('Russia', 'Poland', 'Slovakia', 'Hungary', 'Czech Republic', 'Armenia', 'Estonia', 'Georgia', 'Belarus',
                         'Latvia', 'Lithuania', 'Ukraine') THEN 'Non-Balkan CEE'
                   WHEN ss.posu_country IN ('Italy') THEN 'Italy'
                   END

           WHEN posu_cluster = COALESCE(pc.posu_cluster, 'Other') = '3' THEN
               CASE
                   WHEN ss.posu_country IN
                        ('Croatia', 'Bulgaria', 'Albania', 'Bosnia', 'Romania', 'Montenegro', 'Serbia', 'Slovenia',
                         'Bosnia And Herzegovina', 'Macedonia') THEN 'Balkan CEE'
                   WHEN ss.posu_country IN ('Italy', 'San Marino') THEN 'Italy'
                   WHEN ss.posu_country IN ('Greece') THEN 'Greece'
                   WHEN ss.posu_country IN ('Spain', 'Andorra') THEN 'Spain'
                   WHEN ss.posu_country IN ('Cyprus', 'Turkey', 'Malta') THEN 'Eastern Med'
                   WHEN ss.posu_country IN ('Algeria', 'Egypt', 'Morocco', 'Tunisia') THEN 'North Africa'
                   WHEN ss.posu_country IN ('Portugal') THEN 'Portugal'
                   WHEN ss.posu_country IN ('Switzerland') THEN 'Switzerland (Italian speaking)'
                   ELSE 'Other'
                   END

           WHEN posu_cluster = COALESCE(pc.posu_cluster, 'Other') = '4' THEN
               CASE
                   WHEN posu_country IN (
                                         'Angola', 'Botswana', 'Cape Verde', 'Chad', 'Democratic Republic Of The Congo',
                                         'Eritrea', 'Ethiopia', 'Gambia', 'Guinea-Bissau', 'Kenya', 'Madagascar', 'Mozambique',
                                         'Namibia', 'Nigeria', 'Senegal', 'Sierra Leone', 'South Africa', 'Swaziland', 'Tanzania',
                                         'Uganda', 'Zambia', 'Zimbabwe'
                       ) THEN 'Sub-Saharan Africa'

                   WHEN posu_country IN (
                                         'Angola', 'Botswana', 'Cape Verde', 'Chad', 'Democratic Republic Of The Congo',
                                         'Eritrea', 'Ethiopia', 'Gambia', 'Guinea-Bissau', 'Kenya', 'Madagascar', 'Mozambique',
                                         'Namibia', 'Nigeria', 'Senegal', 'Sierra Leone', 'South Africa', 'Swaziland', 'Tanzania',
                                         'Uganda', 'Zambia', 'Zimbabwe'
                       ) THEN 'Sub-Saharan Africa'
                   WHEN ss.posu_country IN (
                                            'Anguilla', 'Antigua And Barbuda', 'Bahamas', 'Barbados', 'Bermuda',
                                            'Bonaire, Saint Eustatius And Saba', 'British Virgin Islands', 'Cayman Islands',
                                            'Cuba', 'Dominica', 'Dominican Republic',
                                            'Dutch Caribbean', 'Grenada', 'Guadeloupe', 'Haiti', 'Jamaica', 'Martinique',
                                            'Puerto Rico', 'Saint Barthélemy', 'Saint Kitts And Nevis', 'Saint Lucia',
                                            'Saint Vincent And The Grenadines', 'St. Maarten-St. Martin', 'Trinidad And Tobago',
                                            'Turks and Caicos', 'U.S. Virgin Islands'
                       ) THEN 'Caribbean'
                   WHEN ss.posu_country IN (
                                            'Argentina', 'Bolivia', 'Brazil', 'Chile', 'Colombia', 'Ecuador', 'Paraguay', 'Peru',
                                            'Suriname', 'Uruguay', 'Venezuela'
                       ) THEN 'South America'
                   WHEN ss.posu_country IN ('Australia', 'New Zealand') THEN 'AUNZ'
                   WHEN ss.posu_country IN (
                                            'Bahrain', 'Israel', 'Jordan', 'Lebanon', 'Oman', 'Palestinian Territory', 'Qatar',
                                            'Saudi Arabia', 'United Arab Emirates'
                       ) THEN 'Middle East'
                   WHEN ss.posu_country IN (
                                            'Belize', 'Costa Rica', 'El Salvador', 'Guatemala', 'Honduras', 'Mexico', 'Nicaragua',
                                            'Panama'
                       ) THEN 'Central America'
                   WHEN ss.posu_country IN (
                                            'Bangladesh', 'Bhutan', 'China', 'Hong Kong', 'India', 'Iran', 'Japan', 'Kazakhstan',
                                            'Kyrgyzstan', 'Macao', 'Mongolia', 'Nepal', 'Pakistan', 'South Korea', 'Taiwan',
                                            'Uzbekistan'
                       ) THEN 'Other Asia'
                   WHEN ss.posu_country IN (
                                            'Cambodia', 'Indonesia', 'Laos', 'Malaysia', 'Myanmar', 'Philippines', 'Singapore',
                                            'Thailand', 'Vietnam'
                       ) THEN 'South East Asia'
                   WHEN ss.posu_country IN ('Canada', 'USA') THEN 'North America'
                   WHEN ss.posu_country IN ('Fiji', 'French Polynesia', 'Reunion', 'Samoa') THEN 'Pacific Islands'
                   WHEN ss.posu_country IN ('Maldives', 'Mauritius', 'Seychelles', 'Sri Lanka') THEN 'Indian Ocean'
                   END
           ELSE 'Other'
           END                               AS posu_major_region,
       CASE
           WHEN posu_cluster = '1' AND
                posu_country IN ('England', 'Scotland', 'Wales/Cymru', 'Northern Ireland', 'Republic Of Ireland', 'Ireland')
               THEN posu_sub_region
           WHEN posu_cluster = '1' AND posu_country IN ('France') THEN posu_sub_region
           WHEN posu_cluster = '1' AND posu_country IN ('Monaco') THEN 'Monaco'
           WHEN posu_cluster = '1' AND (posu_country IN ('Denmark', 'Sweden') AND posu_city NOT IN
                                                                                  ('Arjeplog', 'Gallivare Kommun', 'Jokkmokk',
                                                                                   'Kiruna', 'Lund', 'Lycksele', 'Skelleftea',
                                                                                   'Vidsel') OR (posu_country = 'Norway' AND
                                                                                                 posu_city IN ('Gardermoen', 'Oslo', 'Oslo County', 'sundvolden')))
               THEN posu_country
           WHEN posu_cluster = '1' AND (posu_country IN
                                        ('Finland', 'Iceland', 'Norway', 'Greenland', 'Aland Islands', 'Faroe Islands',
                                         'Svalbard And Jan Mayen') OR (posu_country = 'Sweden' AND posu_city IN ('Arjeplog',
                                                                                                                 'Gallivare Kommun',
                                                                                                                 'Jokkmokk',
                                                                                                                 'Kiruna', 'Lund',
                                                                                                                 'Lycksele',
                                                                                                                 'Skelleftea',
                                                                                                                 'Vidsel')))
               THEN posu_country
           WHEN posu_cluster = '1' AND posu_country IN ('Switzerland') THEN posu_division
           WHEN posu_cluster = '1' THEN posu_country

           WHEN posu_cluster = '2' AND posu_country IN ('Germany') THEN posu_division
           WHEN posu_cluster = '2' AND posu_country IN ('Austria') THEN posu_division
           WHEN posu_cluster = '2' AND posu_country IN ('Liechtenstein') THEN 'Liechtenstein'
           WHEN posu_cluster = '2' AND posu_country IN ('Switzerland') THEN posu_division
           WHEN posu_cluster = '2' AND posu_country IN ('Belgium') THEN posu_division
           WHEN posu_cluster = '2' AND posu_country IN ('Luxemburg') THEN 'Luxemburg'
           WHEN posu_cluster = '2' AND posu_country IN ('Netherlands') THEN posu_division
           WHEN posu_cluster = '2' AND posu_country IN
                                       ('Russia', 'Poland', 'Slovakia', 'Hungary', 'Czech Republic', 'Armenia', 'Estonia',
                                        'Georgia', 'Belarus', 'Latvia', 'Lithuania', 'Ukraine') THEN posu_country
           WHEN posu_cluster = '2' AND posu_country IN ('Italy') THEN 'South Tyrol'
           WHEN posu_cluster = '2' THEN posu_country

           WHEN posu_cluster = '3' AND posu_country IN
                                       ('Croatia', 'Bulgaria', 'Albania', 'Bosnia', 'Romania', 'Montenegro', 'Serbia', 'Slovenia',
                                        'Bosnia And Herzegovina', 'Macedonia') THEN posu_country
           WHEN posu_cluster = '3' AND posu_country IN ('Italy', 'San Marino') THEN posu_sub_region
           WHEN posu_cluster = '3' AND posu_country IN ('Greece') THEN posu_sub_region
           WHEN posu_cluster = '3' AND posu_country IN ('Spain') THEN posu_sub_region
           WHEN posu_cluster = '3' AND posu_country IN ('Andorra') THEN 'Andorra'
           WHEN posu_cluster = '3' AND posu_country IN ('Cyprus', 'Turkey', 'Malta') THEN posu_country
           WHEN posu_cluster = '3' AND posu_country IN ('Algeria', 'Egypt', 'Morocco', 'Tunisia') THEN posu_country
           WHEN posu_cluster = '3' AND posu_country IN ('Portugal') THEN posu_division
           WHEN posu_cluster = '3' AND posu_country IN ('Switzerland') THEN posu_division
           WHEN posu_cluster = '3' THEN posu_country

           WHEN posu_cluster = '4' AND posu_country IN (
                                                        'Angola',
                                                        'Botswana',
                                                        'Cape Verde',
                                                        'Chad',
                                                        'Democratic Republic Of The Congo',
                                                        'Eritrea',
                                                        'Ethiopia',
                                                        'Gambia',
                                                        'Guinea-Bissau',
                                                        'Kenya',
                                                        'Madagascar',
                                                        'Mozambique',
                                                        'Namibia',
                                                        'Nigeria',
                                                        'Senegal',
                                                        'Sierra Leone',
                                                        'South Africa',
                                                        'Swaziland',
                                                        'Tanzania',
                                                        'Uganda',
                                                        'Zambia',
                                                        'Zimbabwe'
               ) THEN posu_country
           WHEN posu_cluster = '4' AND posu_country IN (
                                                        'Anguilla',
                                                        'Antigua And Barbuda',
                                                        'Bahamas',
                                                        'Barbados',
                                                        'Bermuda',
                                                        'Bonaire, Saint Eustatius And Saba',
                                                        'British Virgin Islands',
                                                        'Cayman Islands',
                                                        'Cuba',
                                                        'Dominica',
                                                        'Dominican Republic',
                                                        'Dutch Caribbean',
                                                        'Grenada',
                                                        'Guadeloupe',
                                                        'Haiti',
                                                        'Jamaica',
                                                        'Martinique',
                                                        'Puerto Rico',
                                                        'Saint Barthélemy',
                                                        'Saint Kitts And Nevis',
                                                        'Saint Lucia',
                                                        'Saint Vincent And The Grenadines',
                                                        'St. Maarten-St. Martin',
                                                        'Trinidad And Tobago',
                                                        'Turks and Caicos',
                                                        'U.S. Virgin Islands'
               ) THEN posu_country
           WHEN posu_cluster = '4' AND posu_country IN (
                                                        'Argentina',
                                                        'Bolivia',
                                                        'Brazil',
                                                        'Chile',
                                                        'Colombia',
                                                        'Ecuador',
                                                        'Paraguay',
                                                        'Peru',
                                                        'Suriname',
                                                        'Uruguay',
                                                        'Venezuela'
               ) THEN posu_country
           WHEN posu_cluster = '4' AND posu_country IN ('Australia', 'New Zealand') THEN posu_country
           WHEN posu_cluster = '4' AND posu_country IN (
                                                        'Bahrain',
                                                        'Israel',
                                                        'Jordan',
                                                        'Lebanon',
                                                        'Oman',
                                                        'Palestinian Territory',
                                                        'Qatar',
                                                        'Saudi Arabia',
                                                        'United Arab Emirates'
               ) THEN posu_country
           WHEN posu_cluster = '4' AND posu_country IN (
                                                        'Belize',
                                                        'Costa Rica',
                                                        'El Salvador',
                                                        'Guatemala',
                                                        'Honduras',
                                                        'Mexico',
                                                        'Nicaragua',
                                                        'Panama'
               ) THEN posu_country
           WHEN posu_cluster = '4' AND posu_country IN (
                                                        'Bangladesh',
                                                        'Bhutan',
                                                        'China',
                                                        'Hong Kong',
                                                        'India',
                                                        'Iran',
                                                        'Japan',
                                                        'Kazakhstan',
                                                        'Kyrgyzstan',
                                                        'Macao',
                                                        'Mongolia',
                                                        'Nepal',
                                                        'Pakistan',
                                                        'South Korea',
                                                        'Taiwan',
                                                        'Uzbekistan'
               ) THEN posu_country
           WHEN posu_cluster = '4' AND posu_country IN (
                                                        'Cambodia',
                                                        'Indonesia',
                                                        'Laos',
                                                        'Malaysia',
                                                        'Myanmar',
                                                        'Philippines',
                                                        'Singapore',
                                                        'Thailand',
                                                        'Vietnam'
               ) THEN posu_country
           WHEN posu_cluster = '4' AND posu_country IN ('Canada', 'USA') THEN posu_country
           WHEN posu_cluster = '4' AND posu_country IN ('Fiji', 'French Polynesia', 'Reunion', 'Samoa') THEN posu_country
           WHEN posu_cluster = '4' AND posu_country IN ('Maldives', 'Mauritius', 'Seychelles', 'Sri Lanka') THEN posu_country
           WHEN posu_cluster = '4' THEN posu_country
           END                               AS posu_minor_region
FROM data_vault_mvp.dwh.se_sale ss
         LEFT JOIN hygiene_snapshot_vault_mvp.fpa_gsheets.posu_categorisation pc
                   ON ss.posu_categorisation_id = pc.posu_categorisation_id
WHERE class IS DISTINCT FROM 'com.flashsales.sale.ConnectedWebRedirectSale'


