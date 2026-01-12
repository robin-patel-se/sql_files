WITH parse_js AS (
    SELECT PARSE_JSON(
'[
  {
    "elements": [
      "recommended for you"
    ],
    "sales": [
      {
        "element": "recommended for you",
        "sale_id": "A12144"
      },
      {
        "element": "recommended for you",
        "sale_id": "A5826"
      },
      {
        "element": "recommended for you",
        "sale_id": "A10026"
      },
      {
        "element": "recommended for you",
        "sale_id": "A16955"
      },
      {
        "element": "recommended for you",
        "sale_id": "A4904"
      },
      {
        "element": "recommended for you",
        "sale_id": "A4509"
      },
      {
        "element": "recommended for you",
        "sale_id": "A10640"
      },
      {
        "element": "recommended for you",
        "sale_id": "A9741"
      },
      {
        "element": "recommended for you",
        "sale_id": "A11643"
      }
    ]
  }
]') AS js
)
SELECT js,
       element_sids.*,
       element_sids.value:sale_id,
       element_sids.value:element
--        elements.value::VARCHAR
FROM parse_js,
LATERAL FLATTEN(INPUT => js[0]['sales'], OUTER => TRUE) element_sids
-- LATERAL FLATTEN(INPUT => js[0]['elements'], OUTER => TRUE) elements
