DROP TABLE hist_mbrs_cms_trnsctn_new2;

CREATE VOLATILE TABLE hist_mbrs_cms_trnsctn_new2 AS
(
  SELECT
      hist.MemberID,
      CAST(b.SupportingData AS DATE FORMAT 'YYYYMMDD') AS TERM_DT,
      hist.AccountID,
      hist.MMEMTMMENROLLDT
  FROM Reporting_V.SDO_CMS_Trnsctn_rply c
  JOIN EXPORT_FEED.V_SDO_MTM_ELIGIBILITY_QUARTERLY_EXTRACT hist
    ON OREPLACE(hist.MemberID,'*','') = OREPLACE(c.MBR_ID,'*','')
  LEFT JOIN CMS_Core.V_CMS_Trans_ReplyData_Dim b
    ON OREPLACE(b.MedicareID,'*','') = OREPLACE(hist.MemberID,'*','')
  WHERE c.TRNSCTN_REPLY_CD IN ('036','090')
    AND c.TRNSCTN_REPLY_CD <> '091'
    /* calendar-year filter from your doc */
    AND CAST(hist.LoadDateKey AS DATE FORMAT 'YYYYMMDD')
        > CAST(EXTRACT(YEAR FROM CURRENT_DATE)||'0101' AS DATE FORMAT 'YYYYMMDD')
) WITH DATA
ON COMMIT PRESERVE ROWS;
