db.getCollection('events').find({c: {$gte: ISODate("2018-05-20T00:00:00.000Z"), $lt: ISODate("2018-05-30T00:00:00.000Z")},
                                 t: {$in: ["hpf/baggage"]}})
db.getCollection('bookingSummaryReport').find({dateTimeBooked: {$gte: ISODate("2018-05-20T00:00:00.000Z"), $lt: ISODate("2018-05-21T00:00:00.000Z")}})
db.getCollection('bookingSummaryReport').find({transactionId: 'A4572-3994-467453'})