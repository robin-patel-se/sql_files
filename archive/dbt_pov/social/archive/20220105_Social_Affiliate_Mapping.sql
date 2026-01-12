CREATE OR REPLACE VIEW collab.performance_analytics.social_mapping AS
(
WITH f_affiliates AS (
    SELECT
        fa.account_name,
        account_id,
        CASE
            WHEN account_id = 1070451729639743 THEN '10,495,508,517,1281,1528,1588,1940,2192,2527,2619,2792,2998'
            WHEN account_id = 1059740157377567 THEN '603,606,1734'
            WHEN account_id = 1059740324044217 THEN '667,668,1733'
            WHEN account_id = 1071717252846524 THEN '1381,1382,1994,2275,2629'
            WHEN account_id = 1071717412846508 THEN '1377,1378,1837,2628,2844'
            WHEN account_id = 1071717479513168 THEN '575,576'
            WHEN account_id = 1090139467670969 THEN '370,497,516,519,537,748,749,928,1047,1260,2015,2620,2621,2625,2704,2793,2999,3034'
            WHEN account_id = 1116324351719147 THEN '1582,1583,1993,2276,2630,2804,2811,2874'
            WHEN account_id = 1143392432345672 THEN '1597,1598,1617,1618,1619,1620,1621,1622,1623,1624,2214'
            WHEN account_id = 1194514240566824 THEN '988,989,1425,2627'
            WHEN account_id = 1398448170173429 THEN '1940,2327,3419'
            WHEN account_id = 1398450660173180 THEN '2015,2391,3006,3017,3420'
            WHEN account_id = 1786335501384692 THEN '3202,3203'
            WHEN account_id = 1786338911384351 THEN '2632,2923'
            WHEN account_id = 1786339421384300 THEN '2631,2873,2924'
            WHEN account_id = 1786340221384220 THEN '2633,2925'
            WHEN account_id = 1786342588050650 THEN '2634,2926'
            WHEN account_id = 1787186524632923 THEN '2635,2922,3184'
            WHEN account_id = 249221668612876 THEN '1523,1940,3035'
            WHEN account_id = 264806867055571 THEN '352,496,515,518,1996,2528,2622,2977'
            WHEN account_id = 321132944719937 THEN '370,497,516,519,537,928,1047,1260,2015'
            WHEN account_id = 356201715249823 THEN '3161'
            WHEN account_id = 377944316400458 THEN '3162'
            WHEN account_id = 440940833361223 THEN '3142'
            WHEN account_id = 853121811735729 THEN '3163'
            WHEN account_id = 957217454296505 THEN '603,606,1734,2624'
            WHEN account_id = 957217757629808 THEN '667,668,1733,2623'
            WHEN account_id = 959679597383624 THEN '924,925,2837'
            WHEN account_id = 959680677383516 THEN '926,927'
            WHEN account_id = 972252689459648 THEN '890,891,988,989,1425,1427,1995,2529,2626,2705,2836,2875'
            WHEN account_id = 989107444440839 THEN '575,576'


            ELSE '0' END AS affilaite_id

    FROM latest_vault.facebook_marketing.ads fa

    GROUP BY 1, 2
),

     f_accounts AS (


         SELECT
             account_id,
             account_name,
             s.value::INT AS affiliate_id
         FROM f_affiliates fa,
              LATERAL FLATTEN(INPUT => SPLIT(affilaite_id, ',')) s
     )
        ,
     f_data AS (

         SELECT
             fa.affiliate_id,
             fa.account_id,
             fa.account_name,
             af.affiliate_name,
             af.territory_id,
             CASE
                 WHEN territory_id = 1 THEN 'UK'
                 WHEN territory_id = 2 THEN 'SE'
                 WHEN territory_id = 4 THEN 'DE'
                 WHEN territory_id = 7 THEN 'US'
                 WHEN territory_id = 8 THEN 'DK'
                 WHEN territory_id = 9 THEN 'NO'
                 WHEN territory_id = 10 THEN 'CH'
                 WHEN territory_id = 11 THEN 'IT'
                 WHEN territory_id = 12 THEN 'NL'
                 WHEN territory_id = 13 THEN 'ES'
                 WHEN territory_id = 14 THEN 'BE'
                 WHEN territory_id = 15 THEN 'FR'
                 WHEN territory_id = 17 THEN 'SG'
                 WHEN territory_id = 18 THEN 'HK'
                 WHEN territory_id = 19 THEN 'PH'
                 WHEN territory_id = 20 THEN 'ID'
                 WHEN territory_id = 21 THEN 'MY'
                 WHEN territory_id = 22 THEN 'AT'
                 WHEN territory_id = 25 THEN 'TB-BE_FR'
                 WHEN territory_id = 26 THEN 'TB-BE_NL'
                 WHEN territory_id = 27 THEN 'TB-NL'
                 ELSE 'N/A'
                 END AS territory,
             CASE
                 WHEN affiliate_id IN (508, 515, 516, 517, 518, 519, 576, 606, 668, 749, 891, 925, 927, 928, 989, 1260, 1378,
                                       1382, 1425, 1427, 1523, 1582, 1598, 1618, 1620, 1622, 1624, 1837, 2192, 2214, 2275, 2276) THEN 'Mobile'
                 WHEN affiliate_id IN (10, 352, 370, 495, 496, 497, 537, 575, 603, 667, 748, 890, 924, 926, 988, 1281, 1377, 1381,
                                       1583, 1588, 1597, 1617, 1619, 1621, 1623) THEN 'Desktop'
                 ELSE 'Both'
                 END AS platform,
             CASE
                 WHEN affiliate_id IN (1047, 1528, 1940, 2015, 2327, 2391, 2631, 2632, 2633, 2634, 2635, 2922,
                                       2923, 2924, 2925, 2926, 3006, 3017, 3142, 3184, 3202, 3203, 3419, 3420) THEN 'CPA'
                 ELSE 'CPL'
                 END AS goal,
             CASE
                 WHEN affiliate_id IN (1260, 1425, 1427, 1523, 1837, 2214, 2275, 2276) THEN 'Instagram'
                 ELSE 'Facebook'
                 END AS partner,
             CASE
                 WHEN affiliate_id IN (3161, 3162, 3163, 3419, 3420) THEN 'APP'
                 WHEN affiliate_id IN (2619, 2620, 2621, 2622, 2623, 2624, 2625, 2626, 2627, 2628, 2629, 2630, 3202) THEN 'Auto'
                 WHEN affiliate_id IN (1047, 1733, 1734, 1940, 1996, 2015, 2922, 2923, 2924, 2925, 2926) THEN 'Both'
                 WHEN affiliate_id IN (2873, 2874, 2875, 2977, 2998, 2999) THEN 'Competitors'
                 WHEN affiliate_id IN (3035) THEN 'Budget'
                 WHEN affiliate_id IN (2327, 2391, 2631, 2632, 2633, 2634, 2635, 3006, 3142, 3203) THEN 'DAT'
                 WHEN affiliate_id IN (1528, 3017, 3184) THEN 'DPA'
                 WHEN affiliate_id IN (1993, 1994, 1995) THEN 'Flow'
                 WHEN affiliate_id IN (1260, 1425, 1427, 1523, 1837, 2214, 2275, 2276) THEN 'Instagram'
                 WHEN affiliate_id IN (3034) THEN 'LeadOpt'
                 WHEN affiliate_id IN (1281, 2529) THEN 'LG'
                 WHEN affiliate_id IN (2527, 2528, 2704, 2705, 2804, 2844) THEN 'LTV'
                 WHEN affiliate_id IN (508, 515, 516, 517, 518, 519, 576, 606, 668, 749, 891, 925, 927, 928, 989, 1378, 1382, 1582, 1598, 1618, 1620, 1622, 1624) THEN 'Mobile'
                 WHEN affiliate_id IN (2792, 2793, 2811, 2836, 2837) THEN 'Travel-intent'
                 WHEN affiliate_id IN (2192) THEN 'Video'
                 ELSE 'Desktop'
                 END AS filter,
             CASE
                 WHEN affiliate_id IN (2327, 2391, 2631, 2632, 2633, 2634, 2635, 3006, 3142, 3203) THEN 'DAT'
                 ELSE 'N/A'
                 END AS dat,
             CASE
                 WHEN affiliate_id IN (10, 352, 370, 370, 495, 496, 497, 497, 508, 515, 516, 516, 517, 518, 519, 519, 537, 537,
                                       575, 575, 576, 576, 603, 603, 606, 606, 667, 667, 668, 668, 748, 749, 890, 891, 924, 925,
                                       926, 927, 928, 928, 988, 988, 989, 989, 1047, 1047, 1260, 1260, 1281, 1377, 1378, 1381,
                                       1382, 1425, 1425, 1427, 1523, 1528, 1582, 1583, 1588, 1597, 1598, 1617, 1618, 1619, 1620,
                                       1621, 1622, 1623, 1624, 1733, 1733, 1734, 1734, 1837, 1940, 1940, 1940, 1993, 1994, 1995, 1996,
                                       2015, 2015, 2015, 2192, 2214, 2275, 2276, 2327, 2391, 2527, 2528, 2529, 2619, 2620, 2621, 2622,
                                       2623, 2624, 2625, 2626, 2627, 2628, 2629, 2630, 2631, 2632, 2633, 2634, 2635, 2704, 2705, 2792,
                                       2793, 2804, 2811, 2836, 2837, 2844, 2873, 2874, 2875, 2922, 2923, 2924, 2925, 2926, 2977,
                                       2998, 2999, 3006, 3017, 3034, 3035, 3142, 3161, 3162, 3163, 3184, 3202, 3203, 3419, 3420) THEN 'Facebook Ireland Limited (GBP)'
                 ELSE 'N/A'
                 END AS supplier,
             CASE
                 WHEN affiliate_id IN (10, 352, 370, 370, 495, 496, 497, 497, 508, 515, 516, 516, 517, 518, 519, 519, 537, 537,
                                       575, 575, 576, 576, 603, 603, 606, 606, 667, 667, 668, 668, 748, 749, 890, 891, 924, 925,
                                       926, 927, 928, 928, 988, 988, 989, 989, 1047, 1047, 1260, 1260, 1281, 1377, 1378, 1381,
                                       1382, 1425, 1425, 1427, 1523, 1528, 1582, 1583, 1588, 1597, 1598, 1617, 1618, 1619, 1620,
                                       1621, 1622, 1623, 1624, 1733, 1733, 1734, 1734, 1837, 1940, 1940, 1940, 1993, 1994, 1995, 1996,
                                       2015, 2015, 2015, 2192, 2214, 2275, 2276, 2327, 2391, 2527, 2528, 2529, 2619, 2620, 2621, 2622,
                                       2623, 2624, 2625, 2626, 2627, 2628, 2629, 2630, 2631, 2632, 2633, 2634, 2635, 2704, 2705, 2792,
                                       2793, 2804, 2811, 2836, 2837, 2844, 2873, 2874, 2875, 2922, 2923, 2924, 2925, 2926, 2977,
                                       2998, 2999, 3006, 3017, 3034, 3035, 3142, 3161, 3162, 3163, 3184, 3202, 3203, 3419, 3420) THEN 'FACE001'
                 END AS supplier_code


         FROM f_accounts fa
             JOIN se.data.se_affiliate af ON fa.affiliate_id = af.id
     )

SELECT *
FROM f_data
    );

