Here’s how to debug in steps
1. Check if hist alone has rows
SELECT COUNT(*) AS cnt_hist
FROM EXPORT_FEED.V_SDO_MTM_ELIGIBILITY_QUARTERLY_EXTRACT;

2. Check if c alone has rows with those reply codes
SELECT COUNT(*) AS cnt_c
FROM Reporting_V.SDO_CMS_Trnsctn_rply c
WHERE c.TRNSCTN_REPLY_CD IN ('036','090');

3. Check if the join is the problem

Try both joins — in many TRR setups, MedicareID is the true link, not MemberID.

-- A) your current join
SELECT COUNT(*) AS cnt_join_memberid
FROM Reporting_V.SDO_CMS_Trnsctn_rply c
JOIN EXPORT_FEED.V_SDO_MTM_ELIGIBILITY_QUARTERLY_EXTRACT hist
  ON OREPLACE(hist.MemberID,'*','') = OREPLACE(c.MBR_ID,'*','')
WHERE c.TRNSCTN_REPLY_CD IN ('036','090');

-- B) alternate join on MedicareID
SELECT COUNT(*) AS cnt_join_medicareid
FROM Reporting_V.SDO_CMS_Trnsctn_rply c
JOIN EXPORT_FEED.V_SDO_MTM_ELIGIBILITY_QUARTERLY_EXTRACT hist
  ON OREPLACE(hist.MedicareID,'*','') = OREPLACE(c.MBR_ID,'*','')
WHERE c.TRNSCTN_REPLY_CD IN ('036','090');


👉 Whichever gives you a non-zero result is the right join key.

4. Relax the date filter

Instead of restricting to this calendar year right away, first test without date filtering:

SELECT TOP 20 hist.MemberID, c.MBR_ID, hist.AccountID, c.TRNSCTN_REPLY_CD
FROM Reporting_V.SDO_CMS_Trnsctn_rply c
JOIN EXPORT_FEED.V_SDO_MTM_ELIGIBILITY_QUARTERLY_EXTRACT hist
  ON OREPLACE(hist.MedicareID,'*','') = OREPLACE(c.MBR_ID,'*','')
WHERE c.TRNSCTN_REPLY_CD IN ('036','090');


If you see rows here, then add the LoadDateKey calendar-year filter back in.
