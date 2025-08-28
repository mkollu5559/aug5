CREATE VOLATILE TABLE hist_mbrs_cms_trnsctn_new AS
SELECT
    hist.MemberID,
    /* SupportingData is the true source for term/death date */
    CAST(b.SupportingData AS DATE FORMAT 'YYYYMMDD') AS TERM_DT,
    hist.AccountID,
    hist.MMEMTMMENROLLDT
FROM Reporting_V.SDO_CMS_Trnsctn_rply     c
JOIN EXPORT_FEED.V_SDO_MTM_ELIGIBILITY_QUARTERLY_EXTRACT hist
  ON OREPLACE(hist.MemberID,'*','') = OREPLACE(c.MBR_ID,'*','')

/* <-- add this join to access SupportingData */
LEFT JOIN CMS_Core.V_CMS_Trans_ReplyData_Dim b
  /* pick the best key you have; these two are common */
  ON OREPLACE(b.MedicareID,'*','') = OREPLACE(hist.MemberID,'*','')
  /* and/or tie to the transaction in c if available */
  -- AND b.TransactionReplyCode = c.TRNSCTN_REPLY_CD
  -- AND b.TransactionTypeCode  = c.TRNSCTN_TYPE_CD

WHERE c.TRNSCTN_REPLY_CD IN ('036','090')      -- your existing filter
  AND c.EFF_DT = CAST(CURRENT_DATE AS DATE FORMAT 'YYYYMMDD')
  AND c.TRNSCTN_REPLY_CD <> '091'
WITH DATA
ON COMMIT PRESERVE ROWS;
