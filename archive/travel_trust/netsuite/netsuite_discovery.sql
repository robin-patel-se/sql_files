SELECT *
FROM hygiene_vault_mvp.cms_mysql.booking b;


SELECT bs.record__o,
       bs.record__o:vccReference::VARCHAR
FROM hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary bs
WHERE
  bs.record__o:vccReference::VARCHAR IS NOT NULL;

SELECT DISTINCT sale_type FROM hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary bs;

{
  "_id": 35591400,
  "adults": 2,
  "affiliate": "Yahoo Brand PPC",
  "affiliateId": 252,
  "allocationRate": 35800,
  "atolFee": 500,
  "backfillGrossCommissionInSupplierCurrency": 0,
  "baggagePrice": 0,
  "baggagePriceInSupplierCurrency": 0,
  "bookingClass": "com.flashsales.sale.Booking",
  "bookingFee": 1500,
  "bookingFeeNetRate": 1250,
  "bookingStatus": "COMPLETE",
  "bookingType": "BOOKING",
  "bundleId": "35591400",
  "checkIn": {
    "$date": 1540512000000
  },
  "checkOut": {
    "$date": 1540771200000
  },
  "children": 0,
  "city": "St Helier",
  "commissionExVat": 2230,
  "commissionExVatInSupplierCurrency": 2230,
  "company": "Revere Hotel*",
  "contractor": "Russell James",
  "country": "England",
  "county": "Jersey",
  "creditAmountDeductibleFromCommission": 0,
  "creditsUsed": 0,
  "currency": "GBP",
  "currentUserId": "7234460",
  "customerEmail": "brendahurst@btinternet.com",
  "customerId": 7234460,
  "customerPayment": 38304,
  "customerTotalPrice": 38304,
  "dataSchemeVersion": 15,
  "dateTimeBooked": {
    "$date": 1533997613000
  },
  "departureAirportCode": "LGW",
  "departureAirportName": "London Gatwick",
  "destinationName": "The Revere Hotel, Channel Islands",
  "destinationType": "UK",
  "division": "Isle Of Wight",
  "firstName": "Brenda",
  "flashGrossCommissionInSupplierCurrency": 2676,
  "flightAmount": 14004,
  "flightAmountInSupplierCurrency": 14004,
  "flightCarrier": "Easyjet",
  "flightCommission": 0,
  "flightCommissionInSupplierCurrency": 0,
  "flightInboundArrivalDate": {
    "$date": 1540848000000
  },
  "flightOnlyPrice": 14004,
  "flightOnlyPriceInSupplierCurrency": 14004,
  "flightOutboundDepartureDate": {
    "$date": 1540574100000
  },
  "flightVatOnCommission": 0,
  "flightVatOnCommissionInSupplierCurrency": 0,
  "grossBookingValue": 22300,
  "grossBookingValueInSupplierCurrency": 22300,
  "grossCommission": 2676,
  "grossProfit": 3480,
  "hasFlights": true,
  "impulse": "n",
  "infants": 0,
  "isDeposit": false,
  "lastName": "Hurst",
  "lastUpdated": {
    "$date": 1533997613000
  },
  "noNights": 3,
  "nonCashCreditsUsed": 0,
  "notes": "n",
  "numberOfBackfilledNights": 0,
  "numberOfBags": 0,
  "numberOfFlashNights": 1,
  "offerId": 708564,
  "offerName": "Standard Double or Twin room, three nights (one dinner, B&B) - with private airport transfers",
  "originalAcquiringAffiliate": "Yahoo Brand PPC",
  "originalAffiliateId": 252,
  "pax": "Brenda Hurst",
  "paymentSurcharge": 0,
  "paymentSurchargeNetRate": 0,
  "paymentType": "CREDIT",
  "platformName": "MOBILE_WEB",
  "postCode": "KT13 8EQ",
  "providerName": "flash",
  "rateToGbp": 100000,
  "rooms": 1,
  "saleBaseCurrency": "GBP",
  "saleClosestAirportCode": "JER",
  "saleDimension": "IHP - dynamic",
  "saleEndDate": {
    "$date": 1534204740000
  },
  "saleId": 73245,
  "saleName": "Charming Jersey break at a characterful hotel",
  "saleRateToGbp": 100000,
  "saleStartDate": {
    "$date": 1530835260000
  },
  "supplier": "Secret Escapes Limited",
  "territory": "UK",
  "topDiscount": 35,
  "totalCustomTax": 0,
  "totalCustomTaxInSupplierCurrency": 0,
  "totalNetRate": 19624,
  "totalPriceInSupplierCurrency": 22300,
  "totalRoomNights": 3,
  "totalSellRate": 22300,
  "transactionId": "73245-708564-35591400",
  "type": "PACKAGE",
  "uniqueTransactionReference": "1cbdMQamV58b-V4ZGxzV",
  "user": {
    "address1": "2 The Willows",
    "address2": "",
    "city": "Weybridge",
    "country": "GB",
    "firstName": "Brenda",
    "homePhone": "",
    "lastName": "Hurst",
    "mobilePhone": "00447939047344",
    "postcode": "KT13 8EQ",
    "region": "",
    "title": "Mrs"
  },
  "userJoinDate": {
    "$date": 1384002786000
  },
  "vatOnBookingFee": 250,
  "vatOnCommission": 446,
  "vatOnCommissionInSupplierCurrency": 446,
  "vatOnPaymentSurcharge": 0
}

SELECT * FROM hygiene_snapshot_vault_mvp.cms_mysql.reservation r;

SELECT * FROM data_vault_mvp.dwh.booking_cancellation bc;

SELECT * FROM se.data.se_booking sb;

SELECT * FROM se.bi.daily_spv_weight dsw where dsw.territory = 'UK' AND dsw.spvs > 0;


SELECT * FROM hygiene_snapshot_vault_mvp.svb.svb_statement ss

--number of sales active
SELECT COUNT(*) FROM se.data.se_sale_attributes ssa WHERE ssa.sale_active;

--prod
SELECT * FROM se.data.se_calendar sc;

--dev
SELECT * FROM se_dev_robin.data.se_calendar sc;