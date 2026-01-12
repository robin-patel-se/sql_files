CREATE OR REPLACE VIEW collab.performance_analytics.ppc_mapping COPY GRANTS AS
(
WITH google_affiliates AS (
    SELECT
        ca.customer_descriptive_name,
        customer_id,
        CASE
            WHEN customer_id = 1330596767 THEN '493,494,879'
            WHEN customer_id = 1233354842 THEN '2340'
            WHEN customer_id = 9154427126 THEN '1148'
            WHEN customer_id = 9951336549 THEN '2341'
            WHEN customer_id = 5658542837 THEN '2406'
            WHEN customer_id = 2565066652 THEN '2264'
            WHEN customer_id = 8122366917 THEN '1310'
            WHEN customer_id = 9671110661 THEN '669,670,2003'
            WHEN customer_id = 3994787320 THEN '720,2257'
            WHEN customer_id = 3976452784 THEN '2132'
            WHEN customer_id = 6674430348 THEN '3105'
            WHEN customer_id = 8560110876 THEN '1795'
            WHEN customer_id = 9187799678 THEN '2984,2985'
            WHEN customer_id = 9363978285 THEN '977,978,1959'
            WHEN customer_id = 4336933538 THEN '198'
            WHEN customer_id = 9356510742 THEN '3108'
            WHEN customer_id = 8583248614 THEN '966,968,2001'
            WHEN customer_id = 4288037679 THEN '1651,1652,1878'
            WHEN customer_id = 4286897181 THEN '900'
            WHEN customer_id = 8595307935 THEN '2080,2602'
            WHEN customer_id = 8775084034 THEN '604,605,2002'
            WHEN customer_id = 8149282490 THEN '2255,2256,2688,2976,3239,3429'
            WHEN customer_id = 7341018744 THEN '2407'
            WHEN customer_id = 2089247422 THEN '307,948,2073,2080'
            WHEN customer_id = 5698763482 THEN '2328,2329,2345,2397,3229,3431'
            WHEN customer_id = 1777669083 THEN '1131,3278,3279'
            WHEN customer_id = 8259175969 THEN '918,919,1675'
            WHEN customer_id = 7651792933 THEN '2777,2778,2780,3236'
            WHEN customer_id = 3636705094 THEN '3111'
            WHEN customer_id = 5723452185 THEN '920,921,1702'
            WHEN customer_id = 1889889685 THEN '1052'
            WHEN customer_id = 6377974099 THEN '2195'
            WHEN customer_id = 7776965165 THEN '1174'
            WHEN customer_id = 6838772352 THEN '1965,2152,2254,3240,3251'
            WHEN customer_id = 5380348253 THEN '1114'
            WHEN customer_id = 2595508279 THEN '368,369,859'
            WHEN customer_id = 8843137082 THEN '3426'
            WHEN customer_id = 7724309078 THEN '3427'
            WHEN customer_id = 9845349988 THEN '200'
            WHEN customer_id = 3358523562 THEN '3418'
            WHEN customer_id = 7745368142 THEN '1704,3232,3233'
            WHEN customer_id = 3151800318 THEN '742,743,861'
            WHEN customer_id = 7225171857 THEN '350,351,2004'
            WHEN customer_id = 6506746572 THEN '12,200,1681'
            WHEN customer_id = 4443948718 THEN '864,865,1601'
            WHEN customer_id = 8900366577 THEN '1147'
            WHEN customer_id = 3649068293 THEN '2081'
            WHEN customer_id = 5526218771 THEN '1203'
            WHEN customer_id = 1604497841 THEN '2363,2364,2365,2431,3235,3433,3456'
            WHEN customer_id = 5189814406 THEN '1026,375'
            WHEN customer_id = 7927739238 THEN '571,574,781'
            WHEN customer_id = 9670277438 THEN '1173,3275,3276'
            WHEN customer_id = 2953371732 THEN '374,1310'
            WHEN customer_id = 7660640941 THEN '307,2076,3232,3233,3436,3457'
            WHEN customer_id = 4740078382 THEN '3393'
            WHEN customer_id = 6011924603 THEN '3009'
            WHEN customer_id = 2832892151 THEN '745'
            WHEN customer_id = 6148505542 THEN '2343'
            WHEN customer_id = 7667651400 THEN '2506'
            WHEN customer_id = 7462158489 THEN '1183'
            WHEN customer_id = 7103645090 THEN '2062'
            WHEN customer_id = 8270489363 THEN '1603,1604,1871'
            WHEN customer_id = 8929094108 THEN '1589,1590,1738'
            WHEN customer_id = 9797606323 THEN '1913'
            WHEN customer_id = 3157506415 THEN '1937'
            WHEN customer_id = 6031799486 THEN '2124'
            WHEN customer_id = 6112217416 THEN '705,1007'
            WHEN customer_id = 1206205678 THEN '2130'
            WHEN customer_id = 1586971412 THEN '1202'
            WHEN customer_id = 1728804718 THEN '1610,1611'
            WHEN customer_id = 1849008701 THEN '2520'
            WHEN customer_id = 7549342624 THEN '979,980'
            WHEN customer_id = 2450929334 THEN '1632,1633,1872'
            WHEN customer_id = 2443428038 THEN '1608,1609,1735'
            WHEN customer_id = 3620730378 THEN '2063'
            WHEN customer_id = 8651054146 THEN '2507'
            WHEN customer_id = 2505419441 THEN '1704'
            WHEN customer_id = 8094662607 THEN '2400'
            WHEN customer_id = 6577558806 THEN '736,1016'
            WHEN customer_id = 2278410647 THEN '2134'

            ELSE '0' END AS affilaite_id
    FROM collab.marketing_api.prod_google_ads_customer_attributes ca
        -- where ca.CUSTOMER_DESCRIPTIVE_NAME = 'Austria Search'
    GROUP BY 1, 2
),

     g_accounts AS (
         SELECT
             customer_id,
             customer_descriptive_name,
             s.value::INT AS affiliate_id
         FROM google_affiliates ga,
              LATERAL FLATTEN(INPUT => SPLIT(affilaite_id, ',')) s
         WHERE customer_id NOT IN (1178348142, 5067259658, 8769463425, 5044939262, 5535267005)
     ),

     g_data AS (
         SELECT
             ac.customer_descriptive_name,
             ac.customer_id,
             ac.affiliate_id,
             af.affiliate_name,

             CASE
                 WHEN ac.affiliate_id IN (198, 374, 375, 705, 736, 745, 900, 1007, 1016, 1026, 1052, 1183, 1202, 1203, 1310, 1795, 1913, 1937, 2062, 2063, 2081, 2124, 2130,
                                          2132, 2134, 2264, 2340, 2341, 2343, 2400, 2406, 2407, 2506, 2507, 2520, 2984, 2985, 3009, 3123, 3124, 3125, 3404, 3405) THEN 'Display'
                 ELSE 'Search' END AS format,
             CASE
                 WHEN ac.affiliate_id IN (200, 252, 351, 369, 395, 397, 493, 574, 605, 617, 670, 700, 732, 743, 865, 919, 921, 962, 968, 978, 980, 1138, 1141, 1169, 1590, 1604, 1609, 1610,
                                          1633, 1635, 1652, 1884, 1886, 1936, 2080, 2195, 2345, 2431, 2688, 2778, 3104, 3105,
                                          3107, 3108, 3110, 3111, 3247, 3248, 3394, 3418, 3426, 3427) THEN 'Brand'
                 ELSE 'Non-Brand'
                 END               AS brand,
             CASE
                 WHEN affiliate_id IN (307, 720, 948, 1704, 1965, 2073, 2076, 2080, 2081, 2152, 2195, 2254, 2255, 2256, 2257, 2264, 2328, 2329, 2340, 2341, 2345,
                                       2363, 2364, 2365, 2397, 2400, 2406, 2407, 2431, 2506, 2507, 2520, 2602, 2688, 2777, 2778, 2780, 2807, 2976, 3009, 3103, 3104,
                                       3106, 3107, 3109, 3110, 3122, 3123, 3124, 3125, 3132, 3133, 3134, 3141, 3177, 3179, 3181, 3221, 3229, 3232, 3233, 3235, 3236,
                                       3239, 3240, 3242, 3243, 3244, 3245, 3246, 3247, 3248, 3249, 3250, 3251, 3291, 3393, 3394, 3404, 3405, 3429, 3431, 3433, 3435, 3436) THEN 'CPA'
                 ELSE 'CPL'
                 END               AS goal,
             CASE
                 WHEN affiliate_id IN (3233, 3243, 3251, 3429, 3431, 3433, 3435) THEN 'Area'
                 WHEN affiliate_id IN (781, 859, 861, 879, 948, 1601, 1675, 1681, 1702, 1735, 1738, 1871, 1872, 1878, 1959, 2001, 2002,
                                       2003, 2004, 2076, 2152, 2255, 2257, 2328, 2363, 2780, 2986, 3122, 3132, 3133, 3134, 3221, 3223) THEN 'DSA'
                 WHEN affiliate_id IN (1007, 1016, 1026, 1183, 1310, 2062, 2063, 2124, 2130, 2134, 2343, 2520, 2985) THEN 'GSP'
                 WHEN affiliate_id IN (3177, 3179, 3181, 3229, 3232, 3235, 3236, 3239, 3240, 3242, 3244, 3245, 3246) THEN 'Live'
                 WHEN affiliate_id IN (2073, 2254, 2365, 2397, 2976) THEN 'Youtube'
                 WHEN affiliate_id IN (3457, 3456) THEN 'Combined'
                 WHEN affiliate_id = 2839 THEN 'CPA'
                 WHEN affiliate_id = 3436 THEN 'PKG'
                 ELSE 'N/A'
                 END               AS filter,
             CASE
                 WHEN affiliate_id IN (250, 252, 394, 395, 396, 397, 616, 617, 699, 700, 731, 732, 961, 962, 1137, 1138, 1141, 1142, 1169, 1170, 1883, 1884,
                                       1885, 1886, 1927, 1935, 1936, 2408, 2807, 2814, 2839, 2986, 3122, 3141, 3221, 3223, 3242, 3243, 3244, 3245, 3246, 3247,
                                       3248, 3249, 3250, 3291, 3394, 3404, 3405, 3435) THEN 'Bing'
                 ELSE 'Google'
                 END               AS partner,
             CASE
                 WHEN affiliate_id IN (1883, 1884, 1885, 1886, 742, 743, 879, 861, 493, 494) THEN 'EUR'
                 ELSE 'GBP'
                 END               AS currency,
             CASE
                 WHEN affiliate_id IN (493, 494, 742, 743, 861, 879) THEN 'GOOG001'
                 WHEN affiliate_id IN (12, 198, 200, 307, 350, 351, 368, 369, 374, 375, 571, 574, 604, 605, 669, 670, 705, 720, 736, 745, 781, 859,
                                       864, 865, 900, 918, 919, 920, 921, 948, 966, 968, 977, 978, 979, 980, 1007, 1016, 1026, 1052, 1114, 1131, 1147, 1148,
                                       1173, 1174, 1183, 1202, 1203, 1310, 1589, 1590, 1601, 1603, 1604, 1608, 1609, 1610, 1611, 1632, 1633, 1634, 1635, 1651,
                                       1652, 1675, 1681, 1702, 1704, 1735, 1738, 1795, 1871, 1872, 1878, 1913, 1937, 1959, 1965, 2001, 2002, 2003, 2004, 2062, 2063,
                                       2073, 2076, 2080, 2081, 2124, 2130, 2132, 2134, 2152, 2195, 2254, 2255, 2256, 2257, 2264, 2328, 2329, 2340, 2341, 2343, 2345, 2363,
                                       2364, 2365, 2397, 2400, 2406, 2407, 2431, 2506, 2507, 2520, 2602, 2688, 2777, 2778, 2780, 2976, 2984, 2985, 3009, 3103, 3104,
                                       3105, 3106, 3107, 3108, 3109, 3110, 3111, 3123, 3124, 3125, 3132, 3133, 3134, 3177, 3179, 3181, 3229, 3232, 3233, 3235, 3236,
                                       3239, 3240, 3251, 3275, 3276, 3278, 3279, 3393, 3418, 3426, 3427, 3429, 3431, 3433, 3436, 3457, 3456) THEN 'GOOG002'
                 WHEN affiliate_id IN (1883, 1884, 1885, 1886) THEN 'BING002'
                 ELSE 'BING001'
                 END               AS supplier_code,
             CASE
                 WHEN affiliate_id IN (1883, 1884, 1885, 1886) THEN 'Bing Ads (EUR)'
                 WHEN affiliate_id IN (250, 252, 394, 395, 396, 397, 616, 617, 699, 700, 731, 732, 961, 962, 1137, 1138, 1141, 1142, 1169, 1170, 1927,
                                       1935, 1936, 2807, 2814, 2839, 2986, 3122, 3141, 3221, 3223, 3242, 3243, 3244, 3245, 3246, 3247, 3248, 3249, 3250,
                                       3291, 3394, 3404, 3405, 3435) THEN 'Bing Ads (GBP)'
                 WHEN affiliate_id IN (493, 494, 742, 743, 861, 879) THEN 'Google Ireland(EUR)'
                 ELSE 'Google Ireland (GBP)'
                 END               AS supplier

         FROM g_accounts ac
             JOIN se.data.se_affiliate af ON ac.affiliate_id = af.id
     ),

--- for bing dataset
     b_affiliates AS (
         SELECT
             pba.account_number,
             pba.account_name,
             pba.account_id,
             CASE
                 WHEN account_number = 'B01759Q3' THEN '1137, 1138'
                 WHEN account_number = 'B0175SBB' THEN '1141, 1142'
                 WHEN account_number = 'B0179Q4F' THEN '1883, 1884'
                 WHEN account_number = 'B017GWJP' THEN '1169, 1170'
                 WHEN account_number = 'B017L7LZ' THEN '1885, 1886'
                 WHEN account_number = 'B017WFV7' THEN '731, 732'
                 WHEN account_number = 'B017XEH3' THEN '961, 962'
                 WHEN account_number = 'F11518Z8' THEN '3122, 3141, 3244, 3394, 3435'
                 WHEN account_number = 'F11536T4' THEN '3291,3458,3459'
                 WHEN account_number = 'F1154STP' THEN '3405'
                 WHEN account_number = 'F1157ZJH' THEN '3404'
                 WHEN account_number = 'F115DEK5' THEN '1935, 1936, 3223'
                 WHEN account_number = 'F115DF9H' THEN '3245, 3247, 3250'
                 WHEN account_number = 'F115F5J2' THEN '2807, 3221, 3242, 3243'
                 WHEN account_number = 'F115FK4N' THEN '2814'
                 WHEN account_number = 'F115G917' THEN '1927'
                 WHEN account_number = 'F115J39F' THEN '2408'
                 WHEN account_number = 'F115AF7H' THEN '3455,3454'
                 WHEN account_number = 'F115JL73' THEN '3246, 3248, 3249'
                 WHEN account_number = 'X0008TF9' THEN '396, 397'
                 WHEN account_number = 'X000Q1KM' THEN '616, 617'
                 WHEN account_number = 'X000XEP6' THEN '699, 700, 2839'
                 WHEN account_number = 'X000Y16B' THEN '394, 395'
                 WHEN account_number = 'X1563136' THEN '250, 252, 2986'
                 ELSE '0' END AS affiliate_id

         FROM collab.marketing_api.prod_bing_ads_account_attributes pba

     ),

     b_accounts AS (
         SELECT
             account_number,
             account_name,
             account_id,
             s.value::INT AS affiliate_id
         FROM b_affiliates,
              LATERAL FLATTEN(INPUT => SPLIT(affiliate_id, ',')) s
     ),

     b_data AS (
         SELECT
             ac.account_name,
             ac.account_number,
             ac.account_id,
             ac.affiliate_id,
             af.affiliate_name,

             CASE
                 WHEN ac.affiliate_id IN (198, 374, 375, 705, 736, 745, 900, 1007, 1016, 1026, 1052, 1183, 1202, 1203, 1310, 1795, 1913, 1937, 2062, 2063, 2081, 2124, 2130,
                                          2132, 2134, 2264, 2340, 2341, 2343, 2400, 2406, 2407, 2506, 2507, 2520, 2984, 2985, 3009, 3123, 3124, 3125, 3404, 3405) THEN 'Display'
                 ELSE 'Search' END AS format,
             CASE
                 WHEN ac.affiliate_id IN (200, 252, 351, 369, 395, 397, 493, 574, 605, 617, 670, 700, 732, 743, 865, 919, 921, 962, 968, 978, 980, 1138, 1141, 1169, 1590, 1604, 1609, 1610,
                                          1633, 1635, 1652, 1884, 1886, 1936, 2080, 2195, 2345, 2431, 2688, 2778, 3104, 3105,
                                          3107, 3108, 3110, 3111, 3247, 3248, 3394, 3418, 3426, 3427) THEN 'Brand'
                 ELSE 'Non-Brand'
                 END               AS brand,
             CASE
                 WHEN affiliate_id IN (307, 720, 948, 1704, 1965, 2073, 2076, 2080, 2081, 2152, 2195, 2254, 2255, 2256, 2257, 2264, 2328, 2329, 2340, 2341, 2345,
                                       2363, 2364, 2365, 2397, 2400, 2406, 2407, 2431, 2506, 2507, 2520, 2602, 2688, 2777, 2778, 2780, 2807, 2976, 3009, 3103, 3104,
                                       3106, 3107, 3109, 3110, 3122, 3123, 3124, 3125, 3132, 3133, 3134, 3141, 3177, 3179, 3181, 3221, 3229, 3232, 3233, 3235, 3236,
                                       3239, 3240, 3242, 3243, 3244, 3245, 3246, 3247, 3248, 3249, 3250, 3251, 3291, 3393, 3394, 3404, 3405, 3429, 3431, 3433, 3435, 3436, 3453, 3455, 3458, 3459)
                     THEN 'CPA'
                 ELSE 'CPL'
                 END               AS goal,
             CASE
                 WHEN affiliate_id IN (3233, 3243, 3251, 3429, 3431, 3433, 3435, 3458, 3454) THEN 'Area'
                 WHEN affiliate_id IN (781, 859, 861, 879, 948, 1601, 1675, 1681, 1702, 1735, 1738, 1871, 1872, 1878, 1959, 2001, 2002,
                                       2003, 2004, 2076, 2152, 2255, 2257, 2328, 2363, 2780, 2986, 3122, 3132, 3133, 3134, 3221, 3223) THEN 'DSA'
                 WHEN affiliate_id IN (1007, 1016, 1026, 1183, 1310, 2062, 2063, 2124, 2130, 2134, 2343, 2520, 2985) THEN 'GSP'
                 WHEN affiliate_id IN (3177, 3179, 3181, 3229, 3232, 3235, 3236, 3239, 3240, 3242, 3244, 3245, 3246, 3459, 3455) THEN 'Live'
                 WHEN affiliate_id IN (2073, 2254, 2365, 2397, 2976) THEN 'Youtube'
                 WHEN affiliate_id = 2839 THEN 'CPA'
                 WHEN affiliate_id = 3436 THEN 'PKG'
                 ELSE 'N/A'
                 END               AS filter,
             CASE
                 WHEN affiliate_id IN (250, 252, 394, 395, 396, 397, 616, 617, 699, 700, 731, 732, 961, 962, 1137, 1138, 1141, 1142, 1169, 1170, 1883, 1884,
                                       1885, 1886, 1927, 1935, 1936, 2408, 2807, 2814, 2839, 2986, 3122, 3141, 3221, 3223, 3242, 3243, 3244, 3245, 3246, 3247,
                                       3248, 3249, 3250, 3291, 3394, 3404, 3405, 3435, 3454, 3455, 3458, 3459) THEN 'Bing'
                 ELSE 'Google'
                 END               AS partner,
             CASE
                 WHEN affiliate_id IN (1883, 1884, 1885, 1886, 742, 743, 879, 861, 493, 494) THEN 'EUR'
                 ELSE 'GBP'
                 END               AS currency,
             CASE
                 WHEN affiliate_id IN (493, 494, 742, 743, 861, 879) THEN 'GOOG001'
                 WHEN affiliate_id IN (12, 198, 200, 307, 350, 351, 368, 369, 374, 375, 571, 574, 604, 605, 669, 670, 705, 720, 736, 745, 781, 859,
                                       864, 865, 900, 918, 919, 920, 921, 948, 966, 968, 977, 978, 979, 980, 1007, 1016, 1026, 1052, 1114, 1131, 1147, 1148,
                                       1173, 1174, 1183, 1202, 1203, 1310, 1589, 1590, 1601, 1603, 1604, 1608, 1609, 1610, 1611, 1632, 1633, 1634, 1635, 1651,
                                       1652, 1675, 1681, 1702, 1704, 1735, 1738, 1795, 1871, 1872, 1878, 1913, 1937, 1959, 1965, 2001, 2002, 2003, 2004, 2062, 2063,
                                       2073, 2076, 2080, 2081, 2124, 2130, 2132, 2134, 2152, 2195, 2254, 2255, 2256, 2257, 2264, 2328, 2329, 2340, 2341, 2343, 2345, 2363,
                                       2364, 2365, 2397, 2400, 2406, 2407, 2431, 2506, 2507, 2520, 2602, 2688, 2777, 2778, 2780, 2976, 2984, 2985, 3009, 3103, 3104,
                                       3105, 3106, 3107, 3108, 3109, 3110, 3111, 3123, 3124, 3125, 3132, 3133, 3134, 3177, 3179, 3181, 3229, 3232, 3233, 3235, 3236,
                                       3239, 3240, 3251, 3275, 3276, 3278, 3279, 3393, 3418, 3426, 3427, 3429, 3431, 3433, 3436) THEN 'GOOG002'
                 WHEN affiliate_id IN (1883, 1884, 1885, 1886) THEN 'BING002'
                 ELSE 'BING001'
                 END               AS supplier_code,
             CASE
                 WHEN affiliate_id IN (1883, 1884, 1885, 1886) THEN 'Bing Ads (EUR)'
                 WHEN affiliate_id IN (250, 252, 394, 395, 396, 397, 616, 617, 699, 700, 731, 732, 961, 962, 1137, 1138, 1141, 1142, 1169, 1170, 1927,
                                       1935, 1936, 2807, 2814, 2839, 2986, 3122, 3141, 3221, 3223, 3242, 3243, 3244, 3245, 3246, 3247, 3248, 3249, 3250,
                                       3291, 3394, 3404, 3405, 3435, 3454, 3455, 3458, 3459) THEN 'Bing Ads (GBP)'
                 WHEN affiliate_id IN (493, 494, 742, 743, 861, 879) THEN 'Google Ireland(EUR)'
                 ELSE 'Google Ireland (GBP)'
                 END               AS supplier

         FROM b_accounts ac
             JOIN se.data.se_affiliate af ON ac.affiliate_id = af.id
     )

--- union tables
SELECT
    g.affiliate_id,
    g.customer_id::VARCHAR AS id,
    g.affiliate_name,
    g.format,
    g.brand,
    g.goal,
    g.filter,
    g.supplier,
    g.supplier_code,
    g.currency,
    g.partner
FROM g_data g
UNION ALL
SELECT
    b.affiliate_id,
    b.account_number::VARCHAR,
    b.affiliate_name,
    b.format,
    b.brand,
    b.goal,
    b.filter,
    b.supplier,
    b.supplier_code,
    b.currency,
    b.partner
FROM b_data b

    );



SELECT
    bgm.affiliate_id,
    bgm.id,
    bgm.affiliate_name,
    bgm.format,
    bgm.brand,
    bgm.goal,
    bgm.filter,
    bgm.supplier,
    bgm.supplier_code,
    bgm.currency,
    bgm.partner
FROM latest_vault.marketing_gsheets.google_ads_and_bing_ads_affiliate_mapping bgm;


SELECT
    bgm.affiliate_id,
    bgm.id,
    bgm.affiliate_name,
    bgm.format,
    bgm.brand,
    bgm.goal,
    bgm.filter,
    bgm.supplier,
    bgm.supplier_code,
    bgm.currency,
    bgm.partner
FROM latest_vault.marketing_gsheets.facebook_ads_affiliate_mapping faam bgm;


SELECT *
FROM collab.marketing_api.prod_google_ads_customer_attributes

SELECT GET_DDL('table', 'collab.marketing_api.prod_google_ads_customer_attributes');

CREATE OR REPLACE VIEW collab.marketing_api.prod_google_ads_customer_attributes COPY GRANTS
AS
SELECT *
FROM latest_vault.google_ads.customer_attributes;

SELECT *
FROM collab.marketing_api.prod_bing_ads_account_attributes;

SELECT GET_DDL('table', 'collab.marketing_api.prod_bing_ads_account_attributes');

CREATE OR REPLACE VIEW collab.marketing_api.prod_bing_ads_account_attributes COPY GRANTS AS
SELECT *
FROM hygiene_snapshot_vault_mvp.bing_ads.account_attributes;



SELECT *,
       CASE
           WHEN id IN (198, 374, 375, 705, 736, 745, 900, 1007, 1016, 1026, 1052, 1183, 1202, 1203, 1310, 1795, 1913, 1937, 2062, 2063, 2081, 2124, 2130,
                       2132, 2134, 2264, 2340, 2341, 2343, 2400, 2406, 2407, 2506, 2507, 2520, 2984, 2985, 3009, 3123, 3124, 3125, 3404, 3405) THEN 'Display'
           ELSE 'Search' END AS format,
       CASE
           WHEN id IN (200, 252, 351, 369, 395, 397, 493, 574, 605, 617, 670, 700, 732, 743, 865, 919, 921, 962, 968, 978, 980, 1138, 1141, 1169, 1590, 1604, 1609, 1610,
                       1633, 1635, 1652, 1884, 1886, 1936, 2080, 2195, 2345, 2431, 2688, 2778, 3104, 3105,
                       3107, 3108, 3110, 3111, 3247, 3248, 3394, 3418, 3426, 3427) THEN 'Brand'
           ELSE 'Non-Brand'
           END               AS brand,
       CASE
           WHEN id IN (307, 720, 948, 1704, 1965, 2073, 2076, 2080, 2081, 2152, 2195, 2254, 2255, 2256, 2257, 2264, 2328, 2329, 2340, 2341, 2345,
                       2363, 2364, 2365, 2397, 2400, 2406, 2407, 2431, 2506, 2507, 2520, 2602, 2688, 2777, 2778, 2780, 2807, 2976, 3009, 3103, 3104,
                       3106, 3107, 3109, 3110, 3122, 3123, 3124, 3125, 3132, 3133, 3134, 3141, 3177, 3179, 3181, 3221, 3229, 3232, 3233, 3235, 3236,
                       3239, 3240, 3242, 3243, 3244, 3245, 3246, 3247, 3248, 3249, 3250, 3251, 3291, 3393, 3394, 3404, 3405, 3429, 3431, 3433, 3435, 3436, 3453, 3455, 3458, 3459)
               THEN 'CPA'
           ELSE 'CPL'
           END               AS goal,
       CASE
           WHEN id IN (3233, 3243, 3251, 3429, 3431, 3433, 3435, 3458, 3454) THEN 'Area'
           WHEN id IN (781, 859, 861, 879, 948, 1601, 1675, 1681, 1702, 1735, 1738, 1871, 1872, 1878, 1959, 2001, 2002,
                       2003, 2004, 2076, 2152, 2255, 2257, 2328, 2363, 2780, 2986, 3122, 3132, 3133, 3134, 3221, 3223) THEN 'DSA'
           WHEN id IN (1007, 1016, 1026, 1183, 1310, 2062, 2063, 2124, 2130, 2134, 2343, 2520, 2985) THEN 'GSP'
           WHEN id IN (3177, 3179, 3181, 3229, 3232, 3235, 3236, 3239, 3240, 3242, 3244, 3245, 3246, 3459, 3455) THEN 'Live'
           WHEN id IN (2073, 2254, 2365, 2397, 2976) THEN 'Youtube'
           WHEN id = 2839 THEN 'CPA'
           WHEN id = 3436 THEN 'PKG'
           ELSE 'N/A'
           END               AS filter,
       CASE
           WHEN id IN (250, 252, 394, 395, 396, 397, 616, 617, 699, 700, 731, 732, 961, 962, 1137, 1138, 1141, 1142, 1169, 1170, 1883, 1884,
                       1885, 1886, 1927, 1935, 1936, 2408, 2807, 2814, 2839, 2986, 3122, 3141, 3221, 3223, 3242, 3243, 3244, 3245, 3246, 3247,
                       3248, 3249, 3250, 3291, 3394, 3404, 3405, 3435, 3454, 3455, 3458, 3459) THEN 'Bing'
           ELSE 'Google'
           END               AS partner,
       CASE
           WHEN id IN (1883, 1884, 1885, 1886, 742, 743, 879, 861, 493, 494) THEN 'EUR'
           ELSE 'GBP'
           END               AS currency,
       CASE
           WHEN id IN (493, 494, 742, 743, 861, 879) THEN 'GOOG001'
           WHEN id IN (12, 198, 200, 307, 350, 351, 368, 369, 374, 375, 571, 574, 604, 605, 669, 670, 705, 720, 736, 745, 781, 859,
                       864, 865, 900, 918, 919, 920, 921, 948, 966, 968, 977, 978, 979, 980, 1007, 1016, 1026, 1052, 1114, 1131, 1147, 1148,
                       1173, 1174, 1183, 1202, 1203, 1310, 1589, 1590, 1601, 1603, 1604, 1608, 1609, 1610, 1611, 1632, 1633, 1634, 1635, 1651,
                       1652, 1675, 1681, 1702, 1704, 1735, 1738, 1795, 1871, 1872, 1878, 1913, 1937, 1959, 1965, 2001, 2002, 2003, 2004, 2062, 2063,
                       2073, 2076, 2080, 2081, 2124, 2130, 2132, 2134, 2152, 2195, 2254, 2255, 2256, 2257, 2264, 2328, 2329, 2340, 2341, 2343, 2345, 2363,
                       2364, 2365, 2397, 2400, 2406, 2407, 2431, 2506, 2507, 2520, 2602, 2688, 2777, 2778, 2780, 2976, 2984, 2985, 3009, 3103, 3104,
                       3105, 3106, 3107, 3108, 3109, 3110, 3111, 3123, 3124, 3125, 3132, 3133, 3134, 3177, 3179, 3181, 3229, 3232, 3233, 3235, 3236,
                       3239, 3240, 3251, 3275, 3276, 3278, 3279, 3393, 3418, 3426, 3427, 3429, 3431, 3433, 3436) THEN 'GOOG002'
           WHEN id IN (1883, 1884, 1885, 1886) THEN 'BING002'
           ELSE 'BING001'
           END               AS supplier_code,
       CASE
           WHEN id IN (1883, 1884, 1885, 1886) THEN 'Bing Ads (EUR)'
           WHEN id IN (250, 252, 394, 395, 396, 397, 616, 617, 699, 700, 731, 732, 961, 962, 1137, 1138, 1141, 1142, 1169, 1170, 1927,
                       1935, 1936, 2807, 2814, 2839, 2986, 3122, 3141, 3221, 3223, 3242, 3243, 3244, 3245, 3246, 3247, 3248, 3249, 3250,
                       3291, 3394, 3404, 3405, 3435, 3454, 3455, 3458, 3459) THEN 'Bing Ads (GBP)'
           WHEN id IN (493, 494, 742, 743, 861, 879) THEN 'Google Ireland(EUR)'
           END               AS supplier
FROM se.data.se_affiliate sa;