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



🔹 1. Future Eligibility Check (ESI_EligibilitySpan)

The code is excluding members who have future eligibility spans.

👉 To validate:

-- Find members that WOULD be excluded
SELECT MemberID, ParticipantEffectiveDate
FROM HSViews.ESI_EligibilitySpan
WHERE ParticipantEffectiveDate > CURRENT_DATE
ORDER BY ParticipantEffectiveDate;


These rows are the ones the new logic filters out.

You should see that they don’t appear in MtmHistMembersNEW but may appear in the old MtmHistMembers.

✅ Compare counts:

SELECT COUNT(*) FROM MtmHistMembers
WHERE MemberID IN (
   SELECT MemberID FROM HSViews.ESI_EligibilitySpan
   WHERE ParticipantEffectiveDate > CURRENT_DATE
);

SELECT COUNT(*) FROM MtmHistMembersNEW
WHERE MemberID IN (
   SELECT MemberID FROM HSViews.ESI_EligibilitySpan
   WHERE ParticipantEffectiveDate > CURRENT_DATE
);


→ In the new version, this should return 0.

🔹 2. EGWP Dual Contract Check (S5617 vs H)

The code removes EGWP members who have both an S5617 contract and an H contract, keeping only the S5617.

👉 To validate:

-- Identify members with both contracts
SELECT MemberID, AccountID
FROM ExportFeed.V_SDO_MTM_ELIGIBILITY_QUARTERLY_EXTRACT
WHERE AccountID LIKE '55617%'  -- S5617 contracts
   OR AccountID LIKE 'H%'      -- H contracts
GROUP BY MemberID, AccountID
HAVING COUNT(DISTINCT AccountID) > 1
ORDER BY MemberID;


These are the members the new logic targets.

In MtmHistMembers (old), you might see them listed under both contracts.

In MtmHistMembersNEW (new), you should only see the S5617 contract rows.

✅ Compare counts:

-- How many dual contract members exist in each version
SELECT COUNT(DISTINCT MemberID)
FROM MtmHistMembers
WHERE AccountID LIKE 'H%';

SELECT COUNT(DISTINCT MemberID)
FROM MtmHistMembersNEW
WHERE AccountID LIKE 'H%';


→ In the new version, the second query should be lower or zero because those “H” rows are removed.

📌 Summary of What to Check

Future eligibility → Members with ParticipantEffectiveDate > Current_Date should be missing from the new table.

Dual EGWP contracts → Members with both S5617 and H contracts should only show the S5617 contract in the new table.

Do you want me to build you a side-by-side comparison query that shows exactly which members were dropped/changed between MtmHistMembers and MtmHistMembersNEW for these two new rules? That way you don’t have to run multiple queries separately.

ChatGPT can make mistakes. Check important info.
