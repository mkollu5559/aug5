DROP TABLE hist_mbrs_cms_trnsctn_new;

CREATE VOLATILE TABLE hist_mbrs_cms_trnsctn_new AS
(
    SELECT
        hist.MemberID,
        /* SupportingData is in the DIM view, not in c */
        CAST(b.SupportingData AS DATE FORMAT 'YYYYMMDD') AS TERM_DT,
        hist.AccountID,
        hist.MMEMTMMENROLLDT
    FROM Reporting_V.SDO_CMS_Trnsctn_rply c
    JOIN EXPORT_FEED.V_SDO_MTM_ELIGIBILITY_QUARTERLY_EXTRACT hist
      ON OREPLACE(hist.MemberID,'*','') = OREPLACE(c.MBR_ID,'*','')
    /* Join the DIM view to access SupportingData */
    LEFT JOIN CMS_Core.V_CMS_Trans_ReplyData_Dim b
      ON OREPLACE(b.MedicareID,'*','') = OREPLACE(hist.MemberID,'*','')
     /* (optional) tighten the join to exact transaction if you have these cols) */
     -- AND b.TransactionReplyCode = c.TRNSCTN_REPLY_CD
     -- AND b.TransactionTypeCode  = c.TRNSCTN_TYPE_CD

    WHERE c.TRNSCTN_REPLY_CD IN ('036','090')
      AND c.TRNSCTN_REPLY_CD <> '091'
      /* If c.EFF_DT is a DATE, this is fine: */
      AND c.EFF_DT = CURRENT_DATE
      /* If c.EFF_DT is CHAR/INT YYYYMMDD, use this instead:
         AND c.EFF_DT = CAST(CURRENT_DATE AS DATE FORMAT 'YYYYMMDD')
      */
) WITH DATA
ON COMMIT PRESERVE ROWS;
