#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
/************************************************************************************* 
Script:     mtm_eligibility_disenrollment.py
Author:     (updated per manual MTM Monthly Disenrollment Process)
Copyright:  HealthSpring
DATE:       04/10/2024 (orig) ; Updated: now
Purpose:    Create monthly disenrollment extract for current calendar year:
            - Deceased members (TRR SupportingData) w/ re-enroll after DOD excluded
            - Disenrolled members (OSS first, then ESI), excluding future eligibility
            - Contract changers (OSS first, then ESI), excluding future eligibility
            - EGWP dual contracts (S5617 + H) => keep S5617 only (not a contract change)
Output:     EXPORT_FEED_T.SDO_MTM_ELIGIBILITY_DISENROLLMENT_EXTRACT
History:    Date            Author                  Action
            04/10/2024      Freeman, Katerina       Initial Version
            now             (you)                    Align with manual steps & findings
/*************************************************************************************
"""

import os
import sys
import traceback
import datetime
import configparser
import oss_etl_library as oss  # provided lib

# Process identity
scriptName = os.path.basename(__file__)
location = os.path.dirname(__file__)
processName = os.path.splitext(os.path.basename(__file__))[0]
statusFileName = os.path.join(location, processName + "_Error.txt")

# Connection
my_connection = oss.DatabaseConnection.from_config_file(
    config_file="/phython${env.scripts}/python/setup.ini",
    section="${env.scripts}",
)

# Params
config = configparser.ConfigParser()
config.read(
    "/phython${env.scripts}/python/paramfiles/export_feed_t/mtm_eligibility_disenrollment.prm"
)

DatabaseNameReporting = config.get("Process", "DatabaseNameReporting")  # e.g., REPORTING
DatabaseNameHSViews   = config.get("Process", "DatabaseNameHSViews")    # e.g., HSViews
DatabaseNameExportFeed= config.get("Process", "DatabaseNameExportFeed") # e.g., EXPORT_FEED
DatabaseNameHSIDS     = config.get("Process", "DatabaseNameHSIDS")      # if needed

# ------------------------------------------------------------------------------------
# Helper snippets
# ------------------------------------------------------------------------------------

# Year start (YYYY0101) as DATE using Teradata casting style
YEAR_START_FILTER = (
    "CAST(EXTRACT(YEAR FROM CURRENT_DATE)||'0101' AS DATE FORMAT 'YYYYMMDD')"
)

# Convenience schema suffixes used in your environment
V = "_V"
T = "_T"

# ------------------------------------------------------------------------------------
# Step 1. Baseline YTD MTM membership for current calendar year (manual doc: T1 + base)
# ------------------------------------------------------------------------------------
T1 = f"""
CREATE MULTISET VOLATILE TABLE T1 AS
(
  SELECT DISTINCT
      a.CardHoldID            AS MemberID,
      a.AccountID,
      MIN(a.MemMTMEnrollDt)   AS MemMTMEnrollDt
  FROM {DatabaseNameExportFeed}{V}.SDO_MTM_ELIGIBILITY_QUARTERLY_HIST a
  WHERE CAST(a.LoadDateKey AS DATE FORMAT 'YYYYMMDD') > {YEAR_START_FILTER}
  GROUP BY 1,2
)
WITH DATA
PRIMARY INDEX (MemberID)
ON COMMIT PRESERVE ROWS;
"""

MTMMonthlyDisenrollment = """
CREATE MULTISET VOLATILE TABLE MTMMonthlyDisenrollment AS
(
  SELECT DISTINCT
      T1.MemberID,
      T1.AccountID,
      MIN(T1.MemMTMEnrollDt)                    AS MemMTMEnrollDt,
      CAST(NULL AS INT)                         AS DisenrollmentDate2019,
      CAST(NULL AS VARCHAR(50))                 AS DisenrollmentReason2019,
      CAST(NULL AS VARCHAR(2))                  AS OptOutCode2019
  FROM T1
  GROUP BY 1,2
)
WITH DATA
PRIMARY INDEX (MemberID)
ON COMMIT PRESERVE ROWS;
"""

# ------------------------------------------------------------------------------------
# Build OSS coverage slice for all baseline members (manual: MTM_OSS)
# ------------------------------------------------------------------------------------
MTM_OSS = f"""
CREATE MULTISET VOLATILE TABLE MTM_OSS AS
(
  SELECT DISTINCT
      CVRG.Mbr_ID,
      CVRG.Cntrct_Num,
      CAST(CVRG.Cvrg_Start_DT AS INT) AS Cvrg_Start_Dt,
      CAST(CVRG.Cvrg_End_Dt   AS INT) AS Cvrg_End_Dt
  FROM {DatabaseNameReporting}{V}.CDO_MBR_ENRLMT_CVRG CVRG
  WHERE OREPLACE(CVRG.Mbr_ID,'*','') IN (SELECT DISTINCT OREPLACE(MemberID,'*','') FROM T1)
)
WITH DATA
PRIMARY INDEX (Mbr_ID)
ON COMMIT PRESERVE ROWS;
"""

# ------------------------------------------------------------------------------------
# Step 2. Deceased members via TRR (manual: DODDaily, Enrlmt map, DODDailyFinal, exclude re-enroll)
# ------------------------------------------------------------------------------------
# Pull TRR DIM once; SupportingData is the DOD we report
DODDaily = """
CREATE MULTISET VOLATILE TABLE DODDaily AS
(
  SELECT DISTINCT
      b.MedicareID,
      b.FileTypeCode,
      CAST(NULL AS VARCHAR(20))      AS MemberID,                -- to be backfilled
      b.TransactionReplyCode         AS TRC,
      COALESCE(b.EffectiveDateKey, 29991231) AS EffectiveDateKey,
      CAST(b.TransactionDateKey - 19000000 AS DATE) AS TransactionDateKey,
      b.ProcessingTimeStamp,
      b.SupportingData,
      b.TransactionTypeCode
  FROM CMS_Core_V.CMS_Trans_ReplyData_Dim b
  WHERE b.TransactionReplyCode IN ('036','090','091','092')
     OR (b.TransactionReplyCode = '287' AND b.TransactionTypeCode = '81')
)
WITH DATA
PRIMARY INDEX (MedicareID)
ON COMMIT PRESERVE ROWS;
"""

# map MedicareID -> MemberID (latest)
ENRLMT_MAP = f"""
CREATE MULTISET VOLATILE TABLE Enrlmt AS
(
  SELECT DISTINCT e.MemberId, e.MedicareID
  FROM {DatabaseNameReporting}{V}.SDO_GBSA_ENRLMT e
  WHERE e.MedicareID IN (SELECT MedicareID FROM DODDaily)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY e.MedicareID ORDER BY e.Reportdate DESC) = 1
)
WITH DATA
PRIMARY INDEX (MemberId)
ON COMMIT PRESERVE ROWS;
"""

# backfill MemberID onto DODDaily
DODDaily_Update = """
UPDATE DODDaily
FROM Enrlmt a
SET MemberID = a.MemberID
WHERE DODDaily.MedicareID = a.MedicareID;
"""

# choose final DOD record per member (exclude 091/287)
DODDailyFinal = """
CREATE MULTISET VOLATILE TABLE DODDailyFinal AS
(
  SELECT *
  FROM DODDaily
  QUALIFY ROW_NUMBER() OVER
  (
    PARTITION BY MemberID
    ORDER BY TransactionDateKey DESC,
             ProcessingTimeStamp DESC,
             COALESCE(SupportingData,'29991231') ASC,
             TRC DESC,
             CASE WHEN FileTypeCode='Daily' THEN 2
                  WHEN FileTypeCode='Weekly' THEN 1
                  ELSE 3 END ASC
  ) = 1
  AND TRC NOT IN ('091','287')
)
WITH DATA
PRIMARY INDEX (MemberID)
ON COMMIT PRESERVE ROWS;
"""

# find re-enrolled AFTER DOD (287/81 → later 011)
ENROLL_AFTER_DOD = """
CREATE MULTISET VOLATILE TABLE Enroll AS
(
  SELECT DISTINCT
      b.MedicareID,
      b.FileTypeCode,
      CAST(NULL AS VARCHAR(20)) AS MemberID,
      b.TransactionReplyCode    AS TRC,
      COALESCE(b.EffectiveDateKey, 29991231) AS EffectiveDateKey,
      CAST(b.TransactionDateKey - 19000000 AS DATE) AS TransactionDateKey,
      b.ProcessingTimeStamp,
      b.SupportingData,
      b.TransactionTypeCode
  FROM CMS_Core_V.CMS_Trans_ReplyData_Dim b
  JOIN DODDailyFinal d
    ON b.MedicareID = d.MedicareID
  WHERE b.EffectiveDateKey > d.EffectiveDateKey
    AND b.TransactionReplyCode = '011'
)
WITH DATA
PRIMARY INDEX (MedicareID)
ON COMMIT PRESERVE ROWS;
"""

ENROLL_MAP_UPDATE = """
UPDATE Enroll
FROM Enrlmt a
SET MemberID = a.MemberID
WHERE Enroll.MedicareID = a.MedicareID;
"""

# remove those who re-enrolled after death
DODDailyFinal_Pruned = """
DELETE FROM DODDailyFinal
WHERE MedicareID IN (SELECT MedicareID FROM Enroll);
"""

# ------------------------------------------------------------------------------------
# Deceased: write DisenrollmentDate2019 using DOD; exclude future eligibility
# (ESI first for non-EGWP account mapping; OSS for EGWP shape)
# ------------------------------------------------------------------------------------
DECEASED_UPDATE_ESI = f"""
UPDATE MTMMonthlyDisenrollment AS base
FROM DODDailyFinal a
SET DisenrollmentDate2019 =
(
  SELECT a.SupportingData
)
WHERE OREPLACE(a.MemberID,'*','') = OREPLACE(base.MemberID,'*','')
  AND a.SupportingData <=
      (
        SELECT MAX(b.ParticipantExpirationDate)
        FROM {DatabaseNameHSViews}.ESI_EligibilitySpan b
        WHERE base.AccountID =
              CASE WHEN LEFT(b.DemographicLevel2,1)='F'
                   THEN SUBSTR(b.DemographicLevel2,2,5)
                   ELSE SUBSTR(b.DemographicLevel2,1,5)
              END
          AND OREPLACE(base.MemberID,'*','') = OREPLACE(b.MemberID,'*','')
      )
  AND base.DisenrollmentDate2019 IS NULL;
"""

DECEASED_UPDATE_OSS = """
UPDATE MTMMonthlyDisenrollment AS base
FROM DODDailyFinal a
SET DisenrollmentDate2019 =
(
  SELECT a.SupportingData
)
WHERE OREPLACE(a.MemberID,'*','') = OREPLACE(base.MemberID,'*','')
  AND a.SupportingData <=
      (
        SELECT MAX(b.Cvrg_End_Dt)
        FROM MTM_OSS b
        WHERE base.AccountID = b.Cntrct_Num
          AND OREPLACE(base.MemberID,'*','') = OREPLACE(b.Mbr_ID,'*','')
      )
  AND base.DisenrollmentDate2019 IS NULL;
"""

DECEASED_REASON = """
UPDATE MTMMonthlyDisenrollment
SET DisenrollmentReason2019 = 'Deceased'
WHERE DisenrollmentDate2019 IS NOT NULL
  AND DisenrollmentReason2019 IS NULL;
"""

# ------------------------------------------------------------------------------------
# Step 3. Disenrolled (OSS first, then ESI), excluding future eligibility
# ------------------------------------------------------------------------------------
DISENROLLED_OSS = """
UPDATE MTMMonthlyDisenrollment AS base
SET DisenrollmentDate2019 =
(
  SELECT MAX(a.Cvrg_End_Dt)
  FROM MTM_OSS a
  WHERE OREPLACE(a.Mbr_ID,'*','') = OREPLACE(base.MemberID,'*','')
    AND a.Cntrct_Num = base.AccountID
    AND a.Cvrg_End_Dt < CAST(CURRENT_DATE AS INT)+19000000
    AND NOT EXISTS
    (
      SELECT 1
      FROM MTM_OSS b
      WHERE OREPLACE(a.Mbr_ID,'*','') = OREPLACE(b.Mbr_ID,'*','')
        AND b.Cvrg_End_Dt >= CAST(CURRENT_DATE AS INT)+19000000
        AND b.CVRG_Start_Dt <  CAST(EXTRACT(YEAR FROM CURRENT_DATE)||'1231' AS INT)
    )
)
WHERE DisenrollmentDate2019 IS NULL;
"""

DISENROLLED_ESI = f"""
UPDATE MTMMonthlyDisenrollment AS base
SET DisenrollmentDate2019 =
(
  SELECT MAX(a.ParticipantExpirationDate)
  FROM {DatabaseNameHSViews}.ESI_EligibilitySpan a
  WHERE OREPLACE(a.MemberID,'*','') = OREPLACE(base.MemberID,'*','')
    AND base.AccountID =
         CASE WHEN LEFT(a.DemographicLevel2,1)='F'
              THEN SUBSTR(a.DemographicLevel2,2,5)
              ELSE SUBSTR(a.DemographicLevel2,1,5)
         END
    AND a.ParticipantExpirationDate < CAST(CURRENT_DATE AS INT)+19000000
    AND NOT EXISTS
    (
      SELECT 1
      FROM {DatabaseNameHSViews}.ESI_EligibilitySpan b
      WHERE OREPLACE(a.MemberID,'*','') = OREPLACE(b.MemberID,'*','')
        AND b.ParticipantExpirationDate >= CAST(CURRENT_DATE AS INT)+19000000
        AND b.ParticipantEffectiveDate < CAST(EXTRACT(YEAR FROM CURRENT_DATE)||'1231' AS INT)
    )
)
WHERE DisenrollmentDate2019 IS NULL;
"""

DISENROLLED_REASON = """
UPDATE MTMMonthlyDisenrollment
SET DisenrollmentReason2019 = 'Disenrolled'
WHERE DisenrollmentDate2019 IS NOT NULL
  AND DisenrollmentReason2019 IS NULL;
"""

# ------------------------------------------------------------------------------------
# Step 4. Contract changers (OSS first, then ESI); EGWP dual S5617+H rule applied
# ------------------------------------------------------------------------------------
CONTRACT_CHANGE_OSS = """
UPDATE MTMMonthlyDisenrollment AS base
SET DisenrollmentDate2019 =
(
  SELECT MAX(a.Cvrg_End_Dt)
  FROM MTM_OSS a
  WHERE OREPLACE(a.Mbr_ID,'*','') = OREPLACE(base.MemberID,'*','')
    AND a.Cntrct_Num = base.AccountID
    AND a.Cvrg_End_Dt < CAST(CURRENT_DATE AS INT)+19000000
    AND a.Cvrg_End_Dt < CAST(EXTRACT(YEAR FROM CURRENT_DATE)||'1231' AS INT)
    AND NOT EXISTS
    (
      SELECT 1
      FROM MTM_OSS b
      WHERE OREPLACE(a.Mbr_ID,'*','') = OREPLACE(b.Mbr_ID,'*','')
        AND a.Cntrct_Num = b.Cntrct_Num
        AND b.Cvrg_End_Dt >= CAST(CURRENT_DATE AS INT)+19000000
        AND b.CVRG_Start_Dt < CAST(EXTRACT(YEAR FROM CURRENT_DATE)||'1231' AS INT)
    )
)
WHERE DisenrollmentDate2019 IS NULL;
"""

CONTRACT_CHANGE_ESI = f"""
UPDATE MTMMonthlyDisenrollment AS base
SET DisenrollmentDate2019 =
(
  SELECT MAX(a.ParticipantExpirationDate)
  FROM {DatabaseNameHSViews}.ESI_EligibilitySpan a
  WHERE OREPLACE(a.MemberID,'*','') = OREPLACE(base.MemberID,'*','')
    AND base.AccountID =
         CASE WHEN LEFT(a.DemographicLevel2,1)='F'
              THEN SUBSTR(a.DemographicLevel2,2,5)
              ELSE SUBSTR(a.DemographicLevel2,1,5)
         END
    AND a.ParticipantExpirationDate < CAST(CURRENT_DATE AS INT)+19000000
    AND a.ParticipantExpirationDate < CAST(EXTRACT(YEAR FROM CURRENT_DATE)||'1231' AS INT)
    AND NOT EXISTS
    (
      SELECT 1
      FROM {DatabaseNameHSViews}.ESI_EligibilitySpan b
      WHERE OREPLACE(a.MemberID,'*','') = OREPLACE(b.MemberID,'*','')
        AND CASE WHEN LEFT(a.DemographicLevel2,1)='F'
                 THEN SUBSTR(a.DemographicLevel2,2,5)
                 ELSE SUBSTR(a.DemographicLevel2,1,5)
            END
          = CASE WHEN LEFT(b.DemographicLevel2,1)='F'
                 THEN SUBSTR(b.DemographicLevel2,2,5)
                 ELSE SUBSTR(b.DemographicLevel2,1,5)
            END
        AND b.ParticipantExpirationDate >= CAST(CURRENT_DATE AS INT)+19000000
        AND b.ParticipantEffectiveDate  < CAST(EXTRACT(YEAR FROM CURRENT_DATE)||'1231' AS INT)
    )
)
WHERE DisenrollmentDate2019 IS NULL;
"""

# EGWP dual contract rule: if member has both S5617 and H in MTM data, do NOT treat H as change
EGWP_FILTER = f"""
/* Remove false contract-change cases where member has both S5617 and H contracts;
   they should remain S5617 only, not flagged as a change. */
UPDATE MTMMonthlyDisenrollment AS base
SET DisenrollmentDate2019 = NULL,
    DisenrollmentReason2019 = NULL
WHERE base.DisenrollmentReason2019 IS NULL
  AND EXISTS
  (
    SELECT 1
    FROM {DatabaseNameExportFeed}{V}.SDO_MTM_ELIGIBILITY_QUARTERLY_EXTRACT egwp
    WHERE OREPLACE(egwp.MemberID,'*','') = OREPLACE(base.MemberID,'*','')
      AND egwp.AccountID LIKE '55617%'
  )
  AND base.AccountID LIKE 'H%';
"""

CONTRACT_REASON = """
UPDATE MTMMonthlyDisenrollment
SET DisenrollmentReason2019 = 'Contract Change'
WHERE DisenrollmentDate2019 IS NOT NULL
  AND DisenrollmentReason2019 IS NULL;
"""

# ------------------------------------------------------------------------------------
# Step 5. OptOut mapping
# ------------------------------------------------------------------------------------
OPTOUT = """
UPDATE MTMMonthlyDisenrollment
SET OptOutCode2019 =
(
  SELECT CASE
           WHEN DisenrollmentReason2019 = 'Deceased' THEN '01'
           WHEN DisenrollmentReason2019 IN ('Disenrolled','Contract Change') THEN '02'
         END
)
WHERE DisenrollmentDate2019 IS NOT NULL;
"""

# ------------------------------------------------------------------------------------
# Final file/table for export (matches your previous extract schema)
# ------------------------------------------------------------------------------------
DISENROLLMENT_FILE = """
CREATE MULTISET VOLATILE TABLE DisenrollmentFile2019 AS
(
  SELECT
      a.MemberID,
      a.AccountID                     AS ContractID,
      a.OptOutCode2019                AS OptOutCode,
      a.DisenrollmentDate2019         AS DisenrollmentDate,
      MAX(a.MemMTMEnrollDt)           AS MemMTMEnrollDt
  FROM MTMMonthlyDisenrollment a
  WHERE a.DisenrollmentDate2019 IS NOT NULL
  GROUP BY 1,2,3,4
)
WITH DATA
PRIMARY INDEX (MemberID)
ON COMMIT PRESERVE ROWS;
"""

# Truncate target, then insert formatted rows
TRUNCATE_TARGET = f"DELETE FROM {DatabaseNameExportFeed}{T}.SDO_MTM_ELIGIBILITY_DISENROLLMENT_EXTRACT ALL;"

INSERT_EXTRACT = f"""
INSERT INTO {DatabaseNameExportFeed}{T}.SDO_MTM_ELIGIBILITY_DISENROLLMENT_EXTRACT
(
  MemberID,
  ContractID,
  OptOutCode,
  DisenrollmentDate,
  MemMTMEnrollDt,
  HS_Loadtimestamp,
  HS_LastUpdateTimestamp
)
SELECT
  CAST(MemberID AS VARCHAR(20)),
  ContractID,
  OptOutCode,
  /* convert yyyymmdd -> mm/dd/yyyy */
  SUBSTR(TRIM(CAST(DisenrollmentDate AS VARCHAR(8))),5,2) || '/' ||
  SUBSTR(TRIM(CAST(DisenrollmentDate AS VARCHAR(8))),7,2) || '/' ||
  SUBSTR(TRIM(CAST(DisenrollmentDate AS VARCHAR(8))),1,4)  AS DisenrollmentDate,
  /* MemMTMEnrollDt expected mm/dd/yyyy from source; if not, convert as needed */
  MemMTMEnrollDt,
  CURRENT_TIMESTAMP,
  CURRENT_TIMESTAMP
FROM DisenrollmentFile2019;
"""

# Also mirror into HIST table as you did before
INSERT_HIST = f"""
INSERT INTO {DatabaseNameExportFeed}{T}.SDO_MTM_ELIGIBILITY_DISENROLLMENT_HIST
(
  MemberID,
  ContractID,
  OptOutCode,
  DisenrollmentDate,
  MemMTMEnrollDt,
  HS_Loadtimestamp,
  HS_LastUpdateTimestamp
)
SELECT
  MemberID,
  ContractID,
  OptOutCode,
  DisenrollmentDate,
  MemMTMEnrollDt,
  HS_Loadtimestamp,
  HS_LastUpdateTimestamp
FROM {DatabaseNameExportFeed}{V}.SDO_MTM_ELIGIBILITY_DISENROLLMENT_EXTRACT;
"""

# ------------------------------------------------------------------------------------
# Execute
# ------------------------------------------------------------------------------------
try:
    my_connection.create_session()
    print("Database connected")

    # Baseline + OSS slice
    oss.Statement(T1).execute(my_connection);                         print("T1 done")
    oss.Statement(MTMMonthlyDisenrollment).execute(my_connection);    print("Base table done")
    oss.Statement(MTM_OSS).execute(my_connection);                    print("MTM_OSS slice done")

    # Deceased (TRR pipeline)
    oss.Statement(DODDaily).execute(my_connection);                   print("DODDaily done")
    oss.Statement(ENRLMT_MAP).execute(my_connection);                 print("Enrlmt map done")
    oss.Statement(DODDaily_Update).execute(my_connection);            print("DODDaily MemberID backfill done")
    oss.Statement(DODDailyFinal).execute(my_connection);              print("DODDailyFinal done")
    oss.Statement(ENROLL_AFTER_DOD).execute(my_connection);           print("Enroll-after-DOD slice done")
    oss.Statement(ENROLL_MAP_UPDATE).execute(my_connection);          print("Enroll map backfill done")
    oss.Statement(DODDailyFinal_Pruned).execute(my_connection);       print("Removed re-enrolled after DOD")

    # Deceased updates (ESI then OSS), then mark reason
    oss.Statement(DECEASED_UPDATE_ESI).execute(my_connection);        print("Deceased via ESI applied")
    oss.Statement(DECEASED_UPDATE_OSS).execute(my_connection);        print("Deceased via OSS applied")
    oss.Statement(DECEASED_REASON).execute(my_connection);            print("Deceased reason set")

    # Disenrolled (OSS then ESI), then mark reason
    oss.Statement(DISENROLLED_OSS).execute(my_connection);            print("Disenrolled via OSS applied")
    oss.Statement(DISENROLLED_ESI).execute(my_connection);            print("Disenrolled via ESI applied")
    oss.Statement(DISENROLLED_REASON).execute(my_connection);         print("Disenrolled reason set")

    # Contract change (OSS then ESI), EGWP filter, then mark reason
    oss.Statement(CONTRACT_CHANGE_OSS).execute(my_connection);        print("Contract change via OSS")
    oss.Statement(CONTRACT_CHANGE_ESI).execute(my_connection);        print("Contract change via ESI")
    oss.Statement(EGWP_FILTER).execute(my_connection);                print("EGWP dual-contract filter applied")
    oss.Statement(CONTRACT_REASON).execute(my_connection);            print("Contract change reason set")

    # OptOut mapping
    oss.Statement(OPTOUT).execute(my_connection);                     print("OptOut mapped")

    # Build file table and load target extract
    oss.Statement(DISENROLLMENT_FILE).execute(my_connection);         print("DisenrollmentFile2019 built")
    oss.Statement(TRUNCATE_TARGET).execute(my_connection);            print("Target extract truncated")
    oss.Statement(INSERT_EXTRACT).execute(my_connection);             print("Extract loaded")

    # History insert
    oss.Statement(INSERT_HIST).execute(my_connection);                print("History loaded")

    print("END")

except Exception as e:
    with open(statusFileName, "w") as f:
        f.write(str(e))
        f.write("\n")
        f.write(traceback.format_exc())
    sys.exit(f"Script Failed => {str(e)}")
