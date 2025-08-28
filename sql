1. Row count difference
SELECT 'MtmHistMembers' AS table_name, COUNT(*) AS row_count
FROM MtmHistMembers
UNION ALL
SELECT 'MtmHistMembersnew', COUNT(*)
FROM MtmHistMembersnew;


This shows if one table has more rows than the other.

🔹 2. Distinct MemberID difference
SELECT 'MtmHistMembers' AS table_name, COUNT(DISTINCT MemberID) AS unique_members
FROM MtmHistMembers
UNION ALL
SELECT 'MtmHistMembersnew', COUNT(DISTINCT MemberID)
FROM MtmHistMembersnew;


This checks how many unique members exist in each.

🔹 3. Rows in one table but not the other

(works like a full outer join to highlight differences)

SELECT a.MemberID, a.MemEffDt, a.MemTermDt, a.ContractNum,
       b.MemberID AS MemberID_new, b.MemEffDt AS MemEffDt_new, b.MemTermDt AS MemTermDt_new, b.ContractNum AS ContractNum_new
FROM MtmHistMembers a
FULL OUTER JOIN MtmHistMembersnew b
  ON a.MemberID = b.MemberID
     AND a.MemEffDt = b.MemEffDt
     AND a.MemTermDt = b.MemTermDt
     AND a.ContractNum = b.ContractNum
WHERE a.MemberID IS NULL 
   OR b.MemberID IS NULL;


This will return rows that don’t match between the two tables.

🔹 4. Quick difference count only
SELECT COUNT(*) AS diff_count
FROM (
    SELECT * FROM MtmHistMembers
    MINUS
    SELECT * FROM MtmHistMembersnew
    UNION ALL
    SELECT * FROM MtmHistMembersnew
    MINUS
    SELECT * FROM MtmHistMembers
) t;


This gives you just the number of mismatched rows.

👉 Do you want me to write it in a way that only tells you counts of differences (fast check), or do you want the actual mismatched rows listed so you can review them?
