SELECT *
FROM members
  INNER JOIN (
    SELECT member_id, MAX(extracted_at) AS maxsign FROM members GROUP BY member_id
  ) most_recent ON members.member_ID = most_recent.member_ID
  and members.extracted_at = most_recent.maxsign
limit 1000