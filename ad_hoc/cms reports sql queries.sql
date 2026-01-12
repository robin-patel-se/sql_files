=========== approvedMembersByDate===========
select count(*) as count
from membership
where date_confirmed > :startDate
  and date_confirmed <= :endDate
  and type = "FULL_ACCOUNT"
=========== reminderByDateSale===========
select count(*) as count, title, sale_id
from reminder
         inner join sale on sale.id = reminder.sale_id
where reminder.date_created > :startDate
  and reminder.date_created <= :endDate
group by sale_id;


=========== invitationByDate===========
select count(*) as count
from invitation
where date_sent > :startDate
  and date_sent <= :endDate;


=========== bookingStatusByDate===========
select status,
       count(*)                                                 as count,
       count(case when paypal_button_clicked = true then 1 end) as after_pay_button
from booking
where date_created > :startDate
  and date_created < :endDate
group by status;


=========== Total Members===========
select m.type, count(*) as members
from membership m
         join shiro_user u on u.membership_id = m.id
where m.date_confirmed < :endDate
group by m.type
=========== affiliateCount===========
SELECT a.name AS affiliate, a.url_string AS url, COUNT(u.id) AS members
FROM shiro_user u
         JOIN affiliate a ON u.affiliate_id = a.id
WHERE u.affiliate_id IS NOT NULL
  AND u.date_created > :startDate
  AND u.date_created <= :endDate
GROUP BY u.affiliate_id
=========== notificationStatistics===========
select type as `Notification Type`, count(*) as `Count`
FROM notification_statistic
where date_sent > :startDate
  and date_sent <= :endDate
group by type
order by count(*)
=========== telegraphAddresses===========


select b.date_created,
       u.username,
       p.telegraph_email,
       p.telegraph_phone,
       p.first_name,
       p.surname,
       b.phone_number,
       b.address1,
       b.address2,
       b.city,
       b.country,
       b.postcode
from booking b
         left join shiro_user u on u.id = b.user_id
         left join profile p on p.id = u.profile_id
where b.address1 is not null
  and b.address1 != ''
  and b.status = 'COMPLETE'
  and b.date_created > :startDate
  and b.date_created < :endDate

=========== favorite===========
select s.title as sale, s.id as saleId, count(f.id) as number
from favorite f
         join sale s
where f.sale_id = s.id
  and f.date_created > :startDate
  and f.date_created < :endDate
group by f.sale_id
order by number desc

=========== referralsByDate===========
SELECT u.username AS 'email', u.date_created AS 'sign up date', a.name AS 'affiliate name'
FROM shiro_user u
         INNER JOIN affiliate a ON a.id = u.affiliate_id
WHERE referrer_id IS NOT NULL
  AND u.date_created BETWEEN :startDate AND :endDate
ORDER BY u.date_created DESC

=========== saleSuppliers===========

select s.id as saleId, (case when sup.name is null then 'None' else sup.name end) as supplierName
from sale s
         left outer join supplier sup on s.supplier_id = sup.id
where s.date_created >= :startDate
  and s.date_created < :endDate
order by s.id


=========== saleCoordinates===========
SELECT id                                                                                      AS saleId,
       SUBSTRING(SUBSTRING(map_location, LOCATE('ll=', map_location) + 3), 1,
                 LOCATE('&amp', SUBSTRING(map_location, LOCATE('ll=', map_location) + 3)) - 1) AS coordinates
FROM sale
WHERE LOCATE('ll=', map_location) > 0
  AND date_created >= :startDate
  AND date_created < :endDate;


=========== saleAllocations===========
select s.id                                                           as saleId,
       o.id                                                           as offerId,
       ot.name                                                        as offerName,
       d.code                                                         as airportCode,
       s.base_currency                                                as currency,
       a.id                                                           as allocationId,
       a.start                                                        as allocationStart,
       a.end                                                          as allocationEnd,
       count(ai.id)                                                   as numberOfRooms,
       count(case when ai.state = 'AVAILABLE' then 1 else null end)   as available,
       count(case when ai.state = 'BOOKED' then 1 else null end)      as booked,
       count(case when ai.state = 'LOCKED' then 1 else null end)      as locked,
       count(case when ai.state = 'BLACKED_OUT' then 1 else null end) as blackout,
       a.rate                                                         as rate,
       a.rack_rate                                                    as rackRate,
       a.single_rate                                                  as singleRate
from sale s
         join offer o on o.sale_id = s.id
         join allocation a on a.offer_id = o.id
         left join allocation_items ais on ais.allocation_id = a.id
         left join allocation_item ai on ai.id = ais.allocation_item_id
         left join departure d on d.id = a.departure_id
         join offer_translation ot on ot.offer_id = o.id
where o.active = true
  and s.start > :startDate
  and s.start <= :endDate
  and ot.locale = 'en_GB'
  and (s.with_shared_allocations = false or s.type = 'HOTEL')
group by saleId, offerId, airportCode, currency, allocationId, allocationStart, allocationEnd, rate, rackRate,
         singleRate
UNION
select s.id                                                                        as saleId,
       o.id                                                                        as offerId,
       ot.name                                                                     as offerName,
       group_concat(DISTINCT d.code SEPARATOR ',')                                 as airportCode,
       s.base_currency                                                             as currency,
       '-'                                                                         as allocationId,
       a.start                                                                     as allocationStart,
       a.end                                                                       as allocationEnd,
       count(distinct ai.id)                                                       as numberOfRooms,
       count(distinct case when ai.state = 'AVAILABLE' then ai.id else null end)   as available,
       count(distinct case when ai.state = 'BOOKED' then ai.id else null end)      as booked,
       count(distinct case when ai.state = 'LOCKED' then ai.id else null end)      as locked,
       count(distinct case when ai.state = 'BLACKED_OUT' then ai.id else null end) as blackout,
       ''                                                                          as rate,
       ''                                                                          as rackRate,
       '-'                                                                         as singleRate
from sale s
         join offer o on o.sale_id = s.id
         join allocation a on a.offer_id = o.id
         left join allocation_items ais on ais.allocation_id = a.id
         left join allocation_item ai on ai.id = ais.allocation_item_id
         left join departure d on d.id = a.departure_id
         join offer_translation ot on ot.offer_id = o.id
where o.active = true
  and s.start > :startDate
  and s.start <= :endDate
  and ot.locale = 'en_GB'
  and (s.with_shared_allocations = true and s.type = 'PACKAGE')
group by saleId, offerId, currency, allocationStart, allocationEnd

=========== InvitationAcceptance===========
select i.date_sent                             as sent,
       if(i.registered, 'yes', 'no')           as accepted,
       if(i.registered, i.last_updated, 'n/a') as accepted_on,
       i.email                                 as sent_to,
       s.username                              as sent_by
from invitation i
         join shiro_user s on i.referrer_id = s.id
where date_sent >= :startDate
  and date_sent < :endDate;


=========== saleList===========
SELECT s.id,
       GROUP_CONCAT(DISTINCT st.title SEPARATOR ' | ')             title,
       GROUP_CONCAT(DISTINCT st.destination_name SEPARATOR ' | ')  destination_name,
       GROUP_CONCAT(DISTINCT cou.name SEPARATOR ' | ')             country,
       GROUP_CONCAT(DISTINCT cod.name SEPARATOR ' | ')             division,
       GROUP_CONCAT(DISTINCT cit.name SEPARATOR ' | ')             city,
       s.start,
       s.end,
       s.type,
       CASE
           WHEN s.repeated = 0 THEN 'New'
           ELSE 'Repeat'
           END                     AS                              'repeat',
       s.destination_type,
       CASE
           WHEN s.closest_airport_code IS NOT NULL THEN s.closest_airport_code
           END                     AS                              closest_airport,
       CASE
           WHEN cn.name IS NOT NULL THEN cn.name
           END                     AS                              company,
       GROUP_CONCAT(DISTINCT a.domain SEPARATOR ', ')              exclusive,
       GROUP_CONCAT(DISTINCT con.name SEPARATOR ' | ')             contractor,
       GROUP_CONCAT(DISTINCT jointcontractor.name SEPARATOR ' | ') joint_contractor,
       con.region                                                  contractor_region,
       GROUP_CONCAT(DISTINCT hpft.name SEPARATOR ' | ')            dp_territories,
       GROUP_CONCAT(DISTINCT t.name SEPARATOR ' | ')               territory_name,
       CASE
           WHEN cn.id IS NOT NULL THEN cn.id
           END                     AS                              company_id,
       sup.id                                                      supplier_id,
       GROUP_CONCAT(DISTINCT tag.name SEPARATOR ' , ')             tags,
       CASE
           WHEN s.instant AND ! s.smart_stay THEN
               'impulse'
           WHEN s.smart_stay THEN
               'smart stay'
           ELSE
               'flash'
           END                     AS                              provider_name,
       s.salesforce_opportunity_id AS                              'sf_id',
       s.zero_deposit              AS                              'zero_deposit',
       s.active                    AS                              'active',
       s.is_overnight_flight       AS                              'overnight_flight',
       ''                                                          is_multi_destination
FROM sale AS s
         LEFT JOIN
     sale_translation st
     ON st.sale_id = s.id
         LEFT JOIN
     sale_company sc ON sc.sale_id = s.id
         LEFT JOIN
     company cn ON cn.id = sc.company_id
         LEFT JOIN
     sale_affiliate sa ON sa.sale_affiliates_id = s.id
         LEFT JOIN
     location_info li ON s.location_info_id = li.id
         LEFT JOIN
     country cou ON li.country_id = cou.id
         LEFT JOIN
     country_division cod ON li.division_id = cod.id
         LEFT JOIN
     city cit ON li.city_id = cit.id
         LEFT JOIN
     affiliate a ON sa.affiliate_id = a.id
         LEFT JOIN
     contractor con ON s.contractor_id = con.id
         LEFT JOIN
     contractor jointcontractor ON (s.joint_contractor_id) = jointcontractor.id
         LEFT JOIN
     sale_flight_config sfc ON sfc.sale_id = s.id
         AND sfc.is_able_to_sell_flights = TRUE
         LEFT JOIN
     sale_territory stn ON stn.sale_id = s.id
         LEFT JOIN
     territory t ON t.id = stn.territory_id
         LEFT JOIN
     territory hpft ON hpft.id = sfc.territory_id
         LEFT JOIN
     supplier sup ON (sup.id = s.supplier_id)
         LEFT JOIN
     tag_links tl ON tl.tag_ref = s.id AND tl.type = 'sale'
         LEFT JOIN
     tags tag ON tag.id = tl.tag_id
GROUP BY s.id

UNION ALL

SELECT CONCAT('A', bs.id),
       GROUP_CONCAT(DISTINCT bst.title
                    SEPARATOR ' | '),
       GROUP_CONCAT(DISTINCT bst.destination_name
                    SEPARATOR ' | '),
       GROUP_CONCAT(DISTINCT (CASE
                                  WHEN bs.class = 'com.flashsales.sale.IhpSale' THEN icou.name
                                  ELSE CASE
                                           WHEN bs.class = 'com.flashsales.sale.HotelSale' THEN hcou.name
                                           ELSE wcou.name
                                      END
           END)
                    SEPARATOR ' | '),
       GROUP_CONCAT(DISTINCT (CASE
                                  WHEN bs.class = 'com.flashsales.sale.IhpSale' THEN icod.name
                                  ELSE CASE
                                           WHEN bs.class = 'com.flashsales.sale.HotelSale' THEN hcod.name
                                           ELSE wcod.name
                                      END
           END)
                    SEPARATOR ' | '),
       GROUP_CONCAT(DISTINCT (CASE
                                  WHEN bs.class = 'com.flashsales.sale.IhpSale' THEN icit.name
                                  ELSE CASE
                                           WHEN bs.class = 'com.flashsales.sale.HotelSale' THEN hcit.name
                                           ELSE wcit.name
                                      END
           END)
                    SEPARATOR ' | '),
       bs.start,
       bs.end,
       CASE
           WHEN bs.class IN ('com.flashsales.sale.IhpSale', 'com.flashsales.sale.ConnectedWebRedirectSale')
               THEN 'PACKAGE'
           ELSE 'HOTEL'
           END    END,
       'New',
       bs.destination_type,
       h.default_preferred_airport_code,
       CASE
           WHEN hcn.name IS NOT NULL THEN hcn.name
           WHEN icn.name IS NOT NULL THEN icn.name
           WHEN wcn.name IS NOT NULL THEN wcn.name
           END,
       GROUP_CONCAT(DISTINCT a.domain
                    SEPARATOR ', '),
       GROUP_CONCAT(DISTINCT con.name
                    SEPARATOR ' | '),
       GROUP_CONCAT(DISTINCT jointcontractor.name
                    SEPARATOR ' | '),
       con.region,
       CASE
           WHEN
               bs.class = 'com.flashsales.sale.IhpSale'
               THEN
               GROUP_CONCAT(DISTINCT t.name
                            SEPARATOR ' | ')
           WHEN
                   bs.class = 'com.flashsales.sale.HotelSale' and bs.has_flights_available = 1
               THEN
               GROUP_CONCAT(DISTINCT t.name
                            SEPARATOR ' | ')
           ELSE ''
           END,
       GROUP_CONCAT(DISTINCT t.name
                    SEPARATOR ' | '),
       CASE
           WHEN hcn.id IS NOT NULL THEN hcn.id
           WHEN icn.id IS NOT NULL THEN icn.id
           WHEN wcn.id IS NOT NULL THEN wcn.id
           END AS company_id,
       CASE
           WHEN
               bs.class = 'com.flashsales.sale.IhpSale'
               THEN
               CASE
                   WHEN t.name = 'UK' THEN sup.id
                   ELSE eusup.id
                   END
           ELSE CASE
                    WHEN bs.class = 'com.flashsales.sale.ConnectedWebRedirectSale' THEN wrdsup.id
                    ELSE ''
               END
           END,
       GROUP_CONCAT(DISTINCT tag.name
                    SEPARATOR ' , '),
       CASE
           WHEN bs.class = 'com.flashsales.sale.ConnectedWebRedirectSale' THEN 'Travelbird'
           ELSE ''
           END,
       bs.salesforce_opportunity_id,
       NULL,
       bs.active,
       NULL,
       CASE
           WHEN
                   (SELECT DISTINCT COUNT(id)
                    FROM ihp_sale_company
                    WHERE ihp_sale_id = bs.id) > 1
               THEN
               'true'
           ELSE 'false'
           END
FROM base_sale AS bs
         LEFT JOIN
     base_sale_translation bst ON bst.sale_id = bs.id
         LEFT JOIN
     hotel_sale_offer hso ON hso.hotel_sale_id = bs.id
         LEFT JOIN
     base_offer bo ON bo.id = hso.hotel_offer_id
         LEFT JOIN
     base_offer_product bop ON bop.base_offer_products_id = bo.id
         LEFT JOIN
     product p ON p.id = bop.product_id
         LEFT JOIN
     hotel h ON h.id = p.hotel_id
         LEFT JOIN
     ihp_sale_company isc ON isc.ihp_sale_id = bs.id
         LEFT JOIN
     company hcn ON hcn.id = h.company_id
         LEFT JOIN
     company icn ON icn.id = isc.company_id
         LEFT JOIN
     base_sale_affiliate bsa ON bsa.base_sale_affiliates_id = bs.id
         LEFT JOIN
     location_info hli ON h.location_info_id = hli.id
         LEFT JOIN
     country hcou ON hli.country_id = hcou.id
         LEFT JOIN
     country_division hcod ON hli.division_id = hcod.id
         LEFT JOIN
     city hcit ON hli.city_id = hcit.id
         LEFT JOIN
     web_redirect wrd ON bs.web_redirect_id = wrd.id
         LEFT JOIN
     location_info wli ON wrd.location_info_id = wli.id
         LEFT JOIN
     country wcou ON wli.country_id = wcou.id
         LEFT JOIN
     country_division wcod ON wli.division_id = wcod.id
         LEFT JOIN
     city wcit ON wli.city_id = wcit.id
         LEFT JOIN
     web_redirect_company wrdc ON wrdc.web_redirect_companies_id = wrd.id
         LEFT JOIN
     company wcn ON wcn.id = wrdc.company_id
         LEFT JOIN
     in_house_package ihp ON bs.ihp_id = ihp.id
         LEFT JOIN
     location_info ili ON ihp.location_info_id = ili.id
         LEFT JOIN
     country icou ON ili.country_id = icou.id
         LEFT JOIN
     country_division icod ON ili.division_id = icod.id
         LEFT JOIN
     city icit ON ili.city_id = icit.id
         LEFT JOIN
     affiliate a ON bsa.affiliate_id = a.id
         LEFT JOIN
     contractor con ON bs.contractor_id = con.id
         LEFT JOIN
     contractor jointcontractor ON bs.joint_contractor_id = jointcontractor.id
         LEFT JOIN
     territory t ON t.id = bs.territory_id
         LEFT JOIN
     tag_links tl ON tl.tag_ref = bs.id
         LEFT JOIN
     tags tag ON tag.id = tl.tag_id
         LEFT JOIN
     supplier sup ON sup.salesforce_account_id = '001w000001cSt6u'
         LEFT JOIN
     supplier eusup ON eusup.salesforce_account_id = '0011r00002IYDUe'
         LEFT JOIN
     supplier wrdsup ON wrdsup.id = wrd.supplier_id
GROUP BY bs.id;


=========== esiOptInAffiliates===========
select u.username as email, u.date_created as joinDate, a.name as affiliate
from affiliate a
         inner join shiro_user u on u.affiliate_id = a.id
         inner join profile p on p.id = u.profile_id
where a.id in (238, 178, 229)
  and p.receive_weekly_offers = true
  and u.date_created >= :startDate
order by u.date_created desc


=========== activationRequests===========
select ar.email,
       af.name,
       ar.date_created       as `request created`,
       ar.date_confirmed     as `account confirmed`,
       ar.date_reminder_sent as `request reminder sent`,
       ar.ip
from activation_request ar
         join affiliate af on af.id = ar.affiliate_id
where ar.date_created > :startDate
  and ar.date_created < :endDate
order by ar.date_created

=========== signupsByPostcode===========
select upper(replace(p.postcode, ' ', '')) as 'postcode', count(1) as 'signups_number'
from shiro_user u
         join profile p ON u.profile_id = p.id
         join affiliate a on u.affiliate_id = a.id
where u.date_created >= :startDate
  and u.date_created < :endDate
  and a.id in (24, 200, 252, 366)
group by upper(replace(p.postcode, ' ', ''));


=========== telegraphOptInAffiliates===========
select u.username as email, u.date_created as joinDate, a.name as affiliate
from affiliate a
         inner join shiro_user u on u.affiliate_id = a.id
         inner join profile p on p.id = u.profile_id
where a.id in
      (115, 131, 132, 133, 134, 135, 136, 137, 141, 142, 143, 144, 145, 146, 147, 158, 164, 222, 234, 255, 260, 264,
       295, 320, 343, 415, 502, 522, 523)
  and p.receive_weekly_offers = true
  and u.date_created >= :startDate
order by u.date_created desc

=========== voucherBookings===========
select CAST(voucher.date_created AS DATE)                                    as 'Date Purchased',
       CAST(voucher.date_created AS TIME)                                    as 'Time Purchased',
       voucher.code                                                          as 'Code',
       CONCAT(gifter_profile.first_name, ' ', gifter_profile.surname)        as 'Customer Name',
       gifter_user.username                                                  as 'Customer Email',
       CONCAT((payment.amount - payment.surcharge), ' ', territory.currency) as 'Amount Purchased',
       voucher.status                                                        as 'Status',
       voucher.unique_transaction_reference                                  as 'Unique ID',
       giftee_user.username                                                  as 'Redeemed By'
from voucher
         inner join shiro_user gifter_user on voucher.gifter_id = gifter_user.id
         left join shiro_user giftee_user on voucher.giftee_id = giftee_user.id
         inner join profile gifter_profile on gifter_user.profile_id = gifter_profile.id
         inner join affiliate on gifter_user.affiliate_id = affiliate.id
         inner join territory on affiliate.territory_id = territory.id
         inner join payment on voucher.payment_id = payment.id
group by voucher.id;
=========== bookingPayments===========
select b.id, u.username, b.status, b.currency, p.type, b.date_created
from booking b
         left join shiro_user u on u.id = b.user_id
         left join payment p on p.id = b.payment_id
where b.date_created > :startDate
  and b.date_created <= :endDate
  and b.paypal_button_clicked = 1;
=========== companyList===========
SELECT co.id                                                                                    company_id,
       ''                                                                                       supplier_id,
       CASE WHEN co.salesforce_account_id is not null THEN co.salesforce_account_id ELSE '' END company_sf_id,
       ''                                                                                       supplier_sf_id,
       CASE WHEN co.country is not null THEN co.country ELSE '' END                             country
FROM company co


=========== currentSaleAllocations===========
select s.id                                                           as saleId,
       o.id                                                           as offerId,
       ot.name                                                        as offerName,
       d.code                                                         as airportCode,
       s.base_currency                                                as currency,
       a.id                                                           as allocationId,
       a.start                                                        as allocationStart,
       a.end                                                          as allocationEnd,
       count(ai.id)                                                   as numberOfRooms,
       count(case when ai.state = 'AVAILABLE' then 1 else null end)   as available,
       count(case when ai.state = 'BOOKED' then 1 else null end)      as booked,
       count(case when ai.state = 'LOCKED' then 1 else null end)      as locked,
       count(case when ai.state = 'BLACKED_OUT' then 1 else null end) as blackout,
       a.rate                                                         as rate,
       a.rack_rate                                                    as rackRate,
       a.single_rate                                                  as singleRate,
       a.child_rate                                                   as childRate,
       a.infant_rate                                                  as infantRate,
       a.min_number_of_nights                                         as minNumberOfNights
from sale s
         join offer o on o.sale_id = s.id
         join allocation a on a.offer_id = o.id
         left join allocation_items ais on ais.allocation_id = a.id
         left join allocation_item ai on ai.id = ais.allocation_item_id
         left join departure d on d.id = a.departure_id
         join offer_translation ot on ot.offer_id = o.id
where o.active = true
  and (
        (s.start > :startDate and s.start <= :endDate)
        or (s.start < :startDate and s.end > :endDate)
        or (s.end > :startDate and s.end <= :endDate)
    )
  and ot.locale = 'en_GB'
  and (s.with_shared_allocations = false or s.type = 'HOTEL')
group by saleId, offerId, airportCode, currency, allocationId, allocationStart, allocationEnd, rate, rackRate,
         singleRate
UNION
select s.id                                                                        as saleId,
       o.id                                                                        as offerId,
       ot.name                                                                     as offerName,
       group_concat(DISTINCT d.code SEPARATOR ',')                                 as airportCode,
       s.base_currency                                                             as currency,
       a.id                                                                        as allocationId,
       a.start                                                                     as allocationStart,
       a.end                                                                       as allocationEnd,
       count(distinct ai.id)                                                       as numberOfRooms,
       count(distinct case when ai.state = 'AVAILABLE' then ai.id else null end)   as available,
       count(distinct case when ai.state = 'BOOKED' then ai.id else null end)      as booked,
       count(distinct case when ai.state = 'LOCKED' then ai.id else null end)      as locked,
       count(distinct case when ai.state = 'BLACKED_OUT' then ai.id else null end) as blackout,
       ''                                                                          as rate,
       ''                                                                          as rackRate,
       '-'                                                                         as singleRate,
       ''                                                                          as childRate,
       ''                                                                          as infantRate,
       ''                                                                          as minNumberOfNights
from sale s
         join offer o on o.sale_id = s.id
         join allocation a on a.offer_id = o.id
         left join allocation_items ais on ais.allocation_id = a.id
         left join allocation_item ai on ai.id = ais.allocation_item_id
         left join departure d on d.id = a.departure_id
         join offer_translation ot on ot.offer_id = o.id
where o.active = true
  and (
        (s.start > :startDate and s.start <= :endDate)
        or (s.start < :startDate and s.end > :endDate)
        or (s.end > :startDate and s.end <= :endDate)
    )
  and ot.locale = 'en_GB'
  and (s.with_shared_allocations = true and s.type = 'PACKAGE')
group by saleId, offerId, currency, allocationStart, allocationEnd =========== liveSalesByTerritory===========
SELECT distinct s.id as 'saleId'
FROM sale s
         INNER JOIN sale_territory st ON st.sale_id = s.id
         INNER JOIN territory t ON t.id = st.territory_id
WHERE s.active = 1
  AND s.start <= :startDate
  AND s.end > :endDate
  AND t.name = :territoryName
ORDER BY s.id ASC;
=========== userEmailsHashedSha256===========
select date_created as joined, sha2(username, 256) as hash
from shiro_user
where date_created > :startDate
  and date_created <= :endDate;
=========== emailDelivery===========
select distinct te.additional_info         as 'Booking id',
                s.id                       as 'Sale id',
                te.type                       'Email type',
                te.to_address              as 'Recipient',
                te.status                  as 'Status',
                te.date_created            as 'Date created',
                te.date_queued_at_provider as 'When queued at provider',
                te.date_delivered          as 'When sent to recipient',
                te.retries                 as 'Retries number',
                te.fail_reason             as 'Fail reason'
from triggered_email te
         left join booking_allocations ba on te.additional_info = ba.booking_allocations_id
         left join allocation al on ba.allocation_id = al.id
         left join offer o on o.id = al.offer_id
         left join sale s on o.sale_id = s.id
where te.date_created > :startDate
  and te.date_created <= :endDate =========== duplicateBookers===========
SELECT bg.*,
       su.username,
       a.name as 'Affiliate Name',
       t.name as 'Territory Name'
FROM (SELECT COUNT(id) AS count,
             user_id
      FROM booking b
      WHERE status = 'COMPLETE'
        AND b.affiliate_user_id IS NULL
        AND b.date_created > :startDate
        AND b.date_created < :endDate
      GROUP BY b.user_id) as bg
         join shiro_user su on su.id = bg.user_id
         join affiliate a on a.id = su.affiliate_id
         join territory t on t.id = a.territory_id
where bg.count > 1;

=========== saleListByLastUpdatedDate===========
SELECT s.id,
       group_concat(DISTINCT st.title SEPARATOR ' | ')                                 title,
       group_concat(DISTINCT st.destination_name SEPARATOR ' | ')                      destination_name,
       group_concat(DISTINCT cou.name SEPARATOR ' | ')                                 country,
       group_concat(DISTINCT cod.name SEPARATOR ' | ')                                 division,
       group_concat(DISTINCT cit.name SEPARATOR ' | ')                                 city,
       s.start,
       s.end,
       s.type,
       CASE WHEN s.repeated = 0 THEN 'New' ELSE 'Repeat' END                        AS 'repeat',
       s.destination_type,
       CASE WHEN s.closest_airport_code is not null THEN s.closest_airport_code END as closest_airport,
       CASE WHEN cn.name is not null THEN cn.name END                               as company,
       group_concat(DISTINCT a.domain SEPARATOR ', ')                                  exclusive,
       group_concat(DISTINCTcon.name SEPARATOR
                    ' | ')                                                             contractor,
       group_concat(DISTINCTjointcontractor.name SEPARATOR
                    ' | ')                                                             joint_contractor,
       con.region                                                                      contractor_region,
       group_concat(DISTINCThpft.name SEPARATOR
                    ' | ')                                                             dp_territories,
       group_concat(DISTINCTt.name SEPARATOR
                    ' | ')                                                             territory_name,
       CASE WHEN cn.id is not null THEN cn.id END                                   as company_id,
       sup.id                                                                          supplier_id,
       group_concat(DISTINCTtag.name SEPARATOR
                    ' , ')                                                             tags,
       CASE
           WHEN s.instant and ! s.smart_stay THEN
               'impulse'
           WHEN s.smart_stay THEN
               'smart stay'
           ELSE
               'flash' END                                                          as provider_name,
       s.salesforce_opportunity_id                                                  as
                                                                                       'sf_id',
       s.zero_deposit                                                               as
                                                                                       'zero_deposit',
       s.active                                                                     as
                                                                                       'active',
       s.is_overnight_flight                                                        as
                                                                                       'overnight_flight',
       ''                                                                              is_multi_destination
FROM sale AS s
         LEFT JOIN sale_translation st
                   ON st.sale_id = s.id
         LEFT JOIN sale_company sc ON sc.sale_id = s.id
         LEFT JOIN company cn ON cn.id = sc.company_id
         LEFT JOIN sale_affiliate sa ON sa.sale_affiliates_id = s.id
         LEFT JOIN location_info li ON s.location_info_id = li.id
         LEFT JOIN country cou ON li.country_id = cou.id
         LEFT JOIN country_division cod ON li.division_id = cod.id
         LEFT JOIN city cit ON li.city_id = cit.id
         LEFT JOIN affiliate a ON sa.affiliate_id = a.id
         LEFT JOIN contractor con ON s.contractor_id = con.id
         LEFT JOIN contractor jointcontractor ON (s.joint_contractor_id) = jointcontractor.id
         LEFT JOIN sale_flight_config sfc ON sfc.sale_id = s.id and sfc.is_able_to_sell_flights = TRUE
         LEFT JOIN sale_territory stn ON stn.sale_id = s.id
         LEFT JOIN territory t ON t.id = stn.territory_id
         LEFT JOIN territory hpft ON hpft.id = sfc.territory_id
         LEFT JOIN supplier sup ON (sup.id = s.supplier_id)
         LEFT JOIN tag_links tl on tl.tag_ref = s.id and tl.type = 'sale'
         LEFT JOIN tags tag on tag.id = tl.tag_id
WHERE s.last_updated >= :startDate
  AND s.last_updated <= :endDate
GROUP BY s.id
UNION ALL
SELECT concat('A', bs.id),
       group_concat(DISTINCT bst.title SEPARATOR ' | '),
       group_concat(DISTINCT bst.destination_name SEPARATOR ' | '),
       group_concat(DISTINCT (CASE
                                  WHEN bs.class = 'com.flashsales.sale.IhpSale' THEN icou.name
                                  ELSE CASE
                                           WHEN bs.class = 'com.flashsales.sale.HotelSale' THEN hcou.name
                                           ELSE wcou.name END END) SEPARATOR ' | '),
       group_concat(DISTINCT (CASE
                                  WHEN bs.class = 'com.flashsales.sale.IhpSale' THEN icod.name
                                  ELSE CASE
                                           WHEN bs.class = 'com.flashsales.sale.HotelSale' THEN hcod.name
                                           ELSE wcod.name END END) SEPARATOR ' | '),
       group_concat(DISTINCT (CASE
                                  WHEN bs.class = 'com.flashsales.sale.IhpSale' THEN icit.name
                                  ELSE CASE
                                           WHEN bs.class = 'com.flashsales.sale.HotelSale' THEN hcit.name
                                           ELSE wcit.name END END) SEPARATOR ' | '),
       bs.start,
       bs.end,
       CASE
           WHEN bs.class IN ('com.flashsales.sale.IhpSale', 'com.flashsales.sale.ConnectedWebRedirectSale')
               THEN 'PACKAGE'
           ELSE 'HOTEL' END                           END,
       'New',
       bs.destination_type,
       '',
       CASE
           WHEN hcn.name is not null THEN hcn.name
           WHEN icn.name is not null THEN icn.name
           WHEN wcn.name is not null THEN wcn.name END,
       group_concat(DISTINCT a.domain SEPARATOR ', '),
       group_concat(DISTINCT con.name SEPARATOR ' | '),
       group_concat(DISTINCT jointcontractor.name SEPARATOR ' | '),
       con.region,
       CASE
           WHEN bs.class = 'com.flashsales.sale.IhpSale' THEN group_concat(DISTINCT t.name SEPARATOR ' | ')
           ELSE '' END,
       group_concat(DISTINCT t.name SEPARATOR ' | '),
       CASE
           WHEN hcn.id is not null THEN hcn.id
           WHEN icn.id is not null THEN icn.id
           WHEN wcn.id is not null THEN wcn.id END as company_id,
       CASE
           WHEN bs.class = 'com.flashsales.sale.IhpSale' THEN sup.id
           ELSE CASE WHEN bs.class = 'com.flashsales.sale.ConnectedWebRedirectSale' THEN wrdsup.id ELSE '' END END,
       group_concat(DISTINCT tag.name SEPARATOR ' , '),
       CASE WHEN bs.class = 'com.flashsales.sale.ConnectedWebRedirectSale' THEN 'Travelbird' ELSE '' END,
       bs.salesforce_opportunity_id,
       null,
       bs.active,
       null,
       CASE
           WHEN (SELECT DISTINCT COUNT(id) FROM ihp_sale_company WHERE ihp_sale_id = bs.id) > 1 THEN 'true'
           ELSE 'false' END
FROM base_sale AS bs
         LEFT JOIN base_sale_translation bst ON bst.sale_id = bs.id
         LEFT JOIN hotel_sale_offer hso ON hso.hotel_sale_id = bs.id
         LEFT JOIN base_offer bo ON bo.id = hso.hotel_offer_id
         LEFT JOIN base_offer_product bop ON bop.base_offer_products_id = bo.id
         LEFT JOIN product p ON p.id = bop.product_id
         LEFT JOIN hotel h ON h.id = p.hotel_id
         LEFT JOIN ihp_sale_company isc ON isc.ihp_sale_id = bs.id
         LEFT JOIN company hcn ON hcn.id = h.company_id
         LEFT JOIN company icn ON icn.id = isc.company_id
         LEFT JOIN base_sale_affiliate bsa ON bsa.base_sale_affiliates_id = bs.id
         LEFT JOIN location_info hli ON h.location_info_id = hli.id
         LEFT JOIN country hcou ON hli.country_id = hcou.id
         LEFT JOIN country_division hcod ON hli.division_id = hcod.id
         LEFT JOIN city hcit ON hli.city_id = hcit.id
         LEFT JOIN web_redirect wrd ON bs.web_redirect_id = wrd.id
         LEFT JOIN location_info wli ON wrd.location_info_id = wli.id
         LEFT JOIN country wcou ON wli.country_id = wcou.id
         LEFT JOIN country_division wcod ON wli.division_id = wcod.id
         LEFT JOIN city wcit ON wli.city_id = wcit.id
         LEFT JOIN web_redirect_company wrdc ON wrdc.web_redirect_companies_id = wrd.id
         LEFT JOIN company wcn ON wcn.id = wrdc.company_id
         LEFT JOIN in_house_package ihp ON bs.ihp_id = ihp.id
         LEFT JOIN location_info ili ON ihp.location_info_id = ili.id
         LEFT JOIN country icou ON ili.country_id = icou.id
         LEFT JOIN country_division icod ON ili.division_id = icod.id
         LEFT JOIN city icit ON ili.city_id = icit.id
         LEFT JOIN affiliate a ON bsa.affiliate_id = a.id
         LEFT JOIN contractor con ON bs.contractor_id = con.id
         LEFT JOIN contractor jointcontractor ON bs.joint_contractor_id = jointcontractor.id
         LEFT JOIN territory t ON t.id = bs.territory_id
         LEFT JOIN tag_links tl on tl.tag_ref = bs.id
         LEFT JOIN tags tag on tag.id = tl.tag_id
         LEFT JOIN supplier sup ON sup.salesforce_account_id = '001w000001cSt6u'
         LEFT JOIN supplier wrdsup ON wrdsup.id = wrd.supplier_id
WHERE bs.last_updated >= :startDate
  AND bs.last_updated <= :endDate
GROUP BY bs.id;
=========== creditsCreatedByAgents===========
SELECT *
FROM (
         SELECT c.date_created,
                SUBSTRING(c.reason, LOCATE('[Actioned by: ', c.reason) + 14,
                          LENGTH(c.reason) - (LOCATE('[Actioned by: ', c.reason) + 14)) AS requesting_user,
                c.amount,
                c.currency,
                u.username                                                              AS credited_user,
                c.type,
                SUBSTRING(c.reason, 1, LOCATE('Actioned by:', c.reason) - 3)            AS reason
         FROM secretescapes.credit c
                  INNER JOIN
              shiro_user u ON u.billing_id = c.billing_id
         WHERE c.date_created >= :startDate
           AND c.date_created < :endDate
           AND c.reason LIKE '%[Actioned by:%'
           AND c.reason NOT LIKE '%[Actioned by: null]'
           AND c.reason NOT LIKE '%[Actioned by: JOBS]'
           AND c.reason NOT LIKE 'Converted after territory change%'
           AND c.reason NOT LIKE '%[Actioned by: ]'
     ) AS credits
WHERE credits.requesting_user LIKE '%@secretescapes.%'
   OR credits.requesting_user LIKE '%sitel.net'
   OR credits.requesting_user LIKE '%sitel.com'
