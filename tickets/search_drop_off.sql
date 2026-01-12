-- User search conversion drop off


-- DE 2023 - 2025,
/*
When search by traveller went live user search to book conversion went down.
There was a concern that search by traveller has had a negative impact on conversion.

Most pronounced in DE

Spend levels are more like 2023 than 2024. Reference point

Concentrate YoY but bring in 2023

-- Search by traveller went live

SBT deck: https://docs.google.com/presentation/d/1yqew_-GBz_YmaY9oU7ajmurlPcZe0bQRPuwGhIGKdu0/edit?slide=id.g3a0de88fbe3_0_13#slide=id.g3a0de88fbe3_0_13

When shared SBT go live data, it looks like SBT has made a gap between user search cvr

However when looking further back in the year it was observed that this gap started in February

Biggest pronounced gap is in march

There are sometimes in year where the conversion actually gets really close to 2024

Does traffic align to marketing? rather than trying to associate marketing activity to search

We don't think its search by traveller because we first witnessed deviation in Feb 2025 and since early November 2025
search cvr is very aligned to 2024.

We can investigate based on a session that has made a user search AND made a booking (don't need to worry about
sale id matching)

App does not show the same user cvr trend

https://eu-west-1a.online.tableau.com/#/site/secretescapes/views/ProductHealthMetricsWIP_17570845433400/Search?:iid=1
CVR User Search->Book (Over Time)

BFV behaviour has changed since Flamingo, occupancy is now possible to filter on the sale page, as a result we have
a decline in BFVs which makes BFV to Booking CVR look stronger than last year.

User search to SPV conversion how has that changed? this would be an indicator into the relevance of search results

Product releases spreadsheet: https://docs.google.com/spreadsheets/d/1dFe78nKH47gnUGesatjOuCMm4ceO23titp-OMrE0fbs/edit?gid=0#gid=0

Things that might be interesting to investigate
- Search results reporting - how has number of results changed yoy
- Deal count - amount of sales
- Marketing spend by graphs
- Search to book by search term
- Availability
 */

SELECT * FROM latest_vault.cms_mysql.divisi