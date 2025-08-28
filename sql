hist_mbrs_cms_trnsctn = """create volatile table hist_mbrs_cms_trnsctn as(
select
hist.MemberID,
CAST(b.SupportingData AS DATE FORMAT 'YYYYMMDD') as TERM_DT,
hist.AccountID,
hist.MEMMTMENROLLDT
FROM """ +  DatabaseNameReporting +"""_V.SDO_CMS_Trnsctn_rply c
INNER JOIN """ + DatabaseNameExportFeed +"""_V.SDO_MTM_ELIGIBILITY_QUARTERLY_EXTRACT hist
  ON OReplace(hist.MemberID,'*','') = OReplace(c.MBR_ID,'*','')
LEFT JOIN CMS_Core_V.CMS_Trans_ReplyData_Dim b
  ON b.MedicareID = c.MBR_ID
WHERE c.TRNSCTN_REPLY_CD IN ('036','090')
  AND c.TRNSCTN_REPLY_CD <> '091')with data on commit preserve rows;"""
