1. Row count difference
SELECT 'MtmHistMembers' AS table_name, COUNT(*) AS row_count
FROM MtmHistMembers
UNION ALL
SELECT 'MtmHistMembersnew1', COUNT(*)
FROM MtmHistMembersnew1;


This shows if one table has more rows than the other.

🔹 2. Distinct MemberID difference
SELECT 'MtmHistMembers' AS table_name, COUNT(DISTINCT MemberID) AS unique_members
FROM MtmHistMembers
UNION ALL
SELECT 'MtmHistMembersnew1', COUNT(DISTINCT MemberID)
FROM MtmHistMembersnew1;


This checks how many unique members exist in each.

🔹 3. Rows in one table but not the other

(works like a full outer join to highlight differences)

SELECT a.MemberID, a.MemEffDt, a.MemTermDt, a.ContractNum,
       b.MemberID AS MemberID_new, b.MemEffDt AS MemEffDt_new, b.MemTermDt AS MemTermDt_new, b.ContractNum AS ContractNum_new
FROM MtmHistMembers a
FULL OUTER JOIN MtmHistMembersnew1 b
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
    SELECT * FROM MtmHistMembersnew1
    UNION ALL
    SELECT * FROM MtmHistMembersnew1
    MINUS
    SELECT * FROM MtmHistMembers
) t;


This gives you just the number of mismatched rows.

👉 Do you want me to write it in a way that only tells you counts of differences (fast check), or do you want the actual mismatched rows listed so you can review them?

=============

1. Future Eligibility (ParticipantEffectiveDate > today)

The new code excludes members with future eligibility.
So to compare:

-- Members with future eligibility in OLD table
SELECT COUNT(*) AS old_count
FROM MtmHistMembers
WHERE MemberID IN (
   SELECT MemberID
   FROM HSViews.ESI_EligibilitySpan
   WHERE CAST(ParticipantEffectiveDate AS DATE FORMAT 'YYYYMMDD') > CURRENT_DATE
);

-- Members with future eligibility in NEW table
SELECT COUNT(*) AS new_count
FROM MtmHistMembersnew1
WHERE MemberID IN (
   SELECT MemberID
   FROM HSViews.ESI_EligibilitySpan
   WHERE CAST(ParticipantEffectiveDate AS DATE FORMAT 'YYYYMMDD') > CURRENT_DATE
);


👉 Expectation: old_count > 0 and new_count = 0.
That proves the new filter is working.

🔹 2. EGWP Dual Contracts (S5617 + H)

The new code removes H contracts if the member also has S5617.
So to compare:

-- Dual contract members in OLD table
SELECT COUNT(DISTINCT MemberID) AS old_duals
FROM MtmHistMembers m
WHERE EXISTS (
   SELECT 1 FROM ExportFeed.V_SDO_MTM_ELIGIBILITY_QUARTERLY_EXTRACT e
   WHERE OReplace(e.MemberID,'*','') = OReplace(m.MemberID,'*','')
     AND e.AccountID LIKE '55617%'
)
AND EXISTS (
   SELECT 1 FROM ExportFeed.V_SDO_MTM_ELIGIBILITY_QUARTERLY_EXTRACT e
   WHERE OReplace(e.MemberID,'*','') = OReplace(m.MemberID,'*','')
     AND e.AccountID LIKE 'H%'
);

-- Dual contract members in NEW table
SELECT COUNT(DISTINCT MemberID) AS new_duals
FROM MtmHistMembersnew1 m
WHERE EXISTS (
   SELECT 1 FROM ExportFeed.V_SDO_MTM_ELIGIBILITY_QUARTERLY_EXTRACT e
   WHERE OReplace(e.MemberID,'*','') = OReplace(m.MemberID,'*','')
     AND e.AccountID LIKE '55617%'
)
AND EXISTS (
   SELECT 1 FROM ExportFeed.V_SDO_MTM_ELIGIBILITY_QUARTERLY_EXTRACT e
   WHERE OReplace(e.MemberID,'*','') = OReplace(m.MemberID,'*','')
     AND e.AccountID LIKE 'H%'
);


👉 Expectation: old_duals > new_duals (in some cases new may be 0).

🔹 3. Side-by-Side Difference

To see the actual rows removed by new rules:

-- Rows in old not in new
SELECT *
FROM MtmHistMembers
MINUS
SELECT *
FROM MtmHistMembersnew1;

-- Rows in new not in old (should be none or very few)
SELECT *
FROM MtmHistMembersnew1
MINUS
SELECT *
FROM MtmHistMembers;
