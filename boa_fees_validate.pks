DROP PACKAGE BOA_FEES_VALIDATE;

CREATE OR REPLACE PACKAGE  BOA_FEES_VALIDATE
is

   /***********************************************
    Boa_Fees_Validate.16.0
     version # 14.7 on client
     added file type to load queue array 09/06/2013
     09/23/2013    Add  process_new_feeds
     10/01/2013    Add  SGP_PID_CLEANUP Nested table
     11/06/2013    changed update process to just insert
                   and let the apex client figure the latest
     12/31/2013    add AR files to process
    05/04/2014     add call to define duplicates procedure to remove duplicate file names from queue

    03/16/2015     ADDED CHL XCL AKA DAISY
    04/15/2015     TRIM off " from the Daisy fields
   05/27/2015  overload create results procedure
    **********************************************/
   TYPE GenRefCursor  IS REF CURSOR;

   TYPE rowidArray is table of rowid index by binary_integer;

   TYPE BOA_BILLING_LOAD_QUEUE_ARY IS RECORD
   (
     CLIENT          DBMS_SQL.VARCHAR2_TABLE,
     FILE_NAME       DBMS_SQL.VARCHAR2_TABLE,
     COMPLETED        DBMS_SQL.NUMBER_TABLE,
     FILE_TYPE        DBMS_SQL.VARCHAR2_TABLE,
     rowids           rowidArray
    );


    TYPE  XCL_SWEEP_TBL IS RECORD
    (
      SHEETNAME   DBMS_SQL.VARCHAR2_TABLE,
      ACCT        DBMS_SQL.VARCHAR2_TABLE,
      CURRENT_    DBMS_SQL.VARCHAR2_TABLE,
      CURRENTDTL  DBMS_SQL.VARCHAR2_TABLE,
      RECACTION   DBMS_SQL.VARCHAR2_TABLE
    );

   TYPE  XCL_RESULTS_FILES IS RECORD
   (
       PID              DBMS_SQL.NUMBER_TABLE,
       CLIENT_NAME      DBMS_SQL.VARCHAR2_TABLE,
       BATCH_DATE       DBMS_SQL.DATE_TABLE,
       RESULT_FILE_SENT DBMS_SQL.VARCHAR2_TABLE,
       RESULT_FILE_SIZE DBMS_SQL.NUMBER_TABLE,
       RESULT_FILE_NAME DBMS_SQL.VARCHAR2_TABLE
   );


   TYPE  XCL_LAYOUT IS RECORD
   (
       PID              DBMS_SQL.NUMBER_TABLE,
       ORDNUM           DBMS_SQL.VARCHAR2_TABLE,
       DETID            DBMS_SQL.VARCHAR2_TABLE,
       LNUM             DBMS_SQL.varchar2_TABLE,
       PROCESS_DT       DBMS_SQL.VARCHAR2_TABLE,
       DELIMITERS_57    DBMS_SQL.VARCHAR2_TABLE,
       CANCELED         DBMS_SQL.VARCHAR2_TABLE,
       DELIMITERS_6     DBMS_SQL.VARCHAR2_TABLE
   );



  vDB_LINK              VARCHAR2(100);
  vUSER_home            VARCHAR2(100);
  vDAISY_MV_FROM        VARCHAR2(100);
  vDAISY_MV_TO          VARCHAR2(100);
  vDAISY_CPY_FROM       VARCHAR2(100);
  vDAISY_SCRUB_REPORTS  VARCHAR2(100);
  vCHL_OUTBOX_SAVE      VARCHAR2(100);
  vDAISY_CANCEL_RESULTS VARCHAR2(100);
  vTEAM                 VARCHAR2(100);

  TYPE SGP_PPOINVOICE_NTT  IS TABLE OF SGP_PPOINVOICING_ENHANCED%ROWTYPE;

  TYPE SGP_PID_CLEANUP_NTT IS TABLE OF SGP_PID_CLEANUP%ROWTYPE;

  function GET_SPI_PUNCH_CODE (P_TRANID IN varchar2) return varchar2;

  function GET_SPI_WORK_ORDER (P_TRANID IN varchar2) return varchar2;

  function GET_SPI_order_date (P_TRANID IN varchar2) return varchar2;

  function GET_SPI_Valid_date (P_TRANID IN varchar2) return number;

  procedure  LOG_FILENAME( P_CLIENT IN VARCHAR2, P_FILENAME IN VARCHAR2, P_ID OUT NUMBER, P_MESSAGE OUT VARCHAR2);

  PROCEDURE  LOAD_BOA_SUMMARY;

  procedure  LOAD_SGP_FEES(P_FILENAME IN VARCHAR2 , P_ID IN  NUMBER, P_MESSAGE IN VARCHAR2, P_RCODE OUT NUMBER);

  procedure  LOAD_SGP_FEES_ENHANCED;

  procedure  LOAD_BOA_FEESRETURN(P_FILENAME IN VARCHAR2, P_ID IN NUMBER,   P_MESSAGE IN VARCHAR2, P_RCODE OUT NUMBER);

  procedure  LOAD_SGP_PPOINVOICING(P_FILENAME IN VARCHAR2, P_ID IN NUMBER, P_MESSAGE IN VARCHAR2, P_RCODE OUT NUMBER);

  procedure  LOAD_BOA_FEESRETURN_ENHANCED;

  PROCEDURE  CLEAR_LOG_FILENAME ( P_FILENAME IN VARCHAR2);

  PROCEDURE PROCESS_LOAD_QUEUE;

  PROCEDURE INS_TO_LOAD_QUEUE ( P_CLIENT IN VARCHAR2, P_FILENAME VARCHAR2);

  procedure BOA_DEDUPE_PROCESS;

  procedure  LOAD_SGP_PPO_ENHANCED;
  ---- BOA_FEES_VALIDATE.PROCESS_NEW_FEEDS;
  procedure  PROCESS_NEW_FEEDS;
  ---- BOA_FEES_VALIDATE.UPDATE_LOAD_STATUS
  PROCEDURE UPDATE_LOAD_STATUS;

  PROCEDURE BOA_TABLE_CLEANUP( P_DAYS_BACK NUMBER);

  procedure  LOAD_NAV_ARINVOICING(P_FILENAME IN VARCHAR2, P_ID IN NUMBER, P_MESSAGE IN VARCHAR2, P_RCODE OUT NUMBER);

  procedure  LOAD_LPS_HEADER(P_FILENAME IN VARCHAR2 , P_ID IN  NUMBER, P_MESSAGE IN VARCHAR2, P_RCODE OUT NUMBER);

  procedure  LOAD_SVC_RELEASE(P_FILENAME IN VARCHAR2 , P_ID IN  NUMBER, P_MESSAGE IN VARCHAR2, P_RCODE OUT NUMBER);

  procedure  LOSS_ANALYSIS_FILES(P_FILENAME IN VARCHAR2 , P_ID IN  NUMBER, P_MESSAGE IN VARCHAR2, P_RCODE OUT NUMBER, P_ISSUE OUT VARCHAR2);

  procedure  LOAD_XCL_CHL(P_FILENAME IN VARCHAR2 , P_ID IN  NUMBER, P_MESSAGE IN VARCHAR2, P_RCODE OUT NUMBER, P_ISSUE OUT VARCHAR2);

  procedure  LOAD_FILE_ERRORS(P_FILENAME IN VARCHAR2 , P_ID IN  NUMBER, P_MESSAGE IN VARCHAR2, P_RCODE OUT NUMBER);

  procedure PKG_SETUP;

  PROCEDURE SEND_EMAIL  ( P_TEAM VARCHAR2, P_FROM VARCHAR2, P_SUBJECT VARCHAR2,  P_MESSAGE VARCHAR2);

  FUNCTION validate_no (p_number VARCHAR2) RETURN NUMBER;

  PROCEDURE XCL_CHL_RESULTS_FILE;

  PROCEDURE XCL_CHL_RESULTS_FILE(P_flag number);

end;

/

DROP PACKAGE BODY BOA_FEES_VALIDATE;

CREATE OR REPLACE PACKAGE BODY               BOA_FEES_VALIDATE
is
  /****************************************************************************************
     PACKAGE  version # 14.7

    LOAD_SGP_FEES_ENHANCED
    MODIFIED ON 09/10/2013  to process only the new files
    MODIFIED ON 09/17/2013  to move files from load ( base ) folder to loaded folder
                10/04/2013  transfer the loaded file name to the server
                10/15/2013  changed db link @dev01 to @dev01.safeprop.com
                10/21/2013  added apexd1.
                11/01/2013   took out call to homer
               05/20/2014    added LPS and Service release
              05/27/2015 Overload XCL_CHL_RESULTS_FILE Procedure
   *****************************************************************************************/

  function GET_SPI_PUNCH_CODE
  (
    P_TRANID IN varchar2
  ) return  varchar2
  IS
      RETURN_VAL  VARCHAR2(20);
      TR_TRANID   VARCHAR2(200);

      ptrstart   PLS_INTEGER;
      ptrend     PLS_INTEGER;
      codeLen    PLS_INTEGER;
      BAD_DATA   EXCEPTION;
  begin

    TR_TRANID := TRIM(P_TRANID);

    PTRSTART := INSTR(TR_TRANID,'-',1,1);
    PTREND   := INSTR(TR_TRANID,'-',1,2);
    codeLen  := ( (INSTR(TR_TRANID,'-',1,2)) - (INSTR(TR_TRANID,'-',1,1))  - 1);

    RETURN_VAL := CASE WHEN ptrstart = 0  THEN 'BAD_DATA'
                       WHEN ptrend   = 0  THEN 'BAD_DATA'
                       WHEN codeLen  < 0  THEN 'BAD_DATA'
                  END;
    IF (RETURN_VAL = 'BAD_DATA')
       THEN
          RAISE BAD_DATA;
    END IF;

    RETURN_VAL :=  SUBSTR(TR_TRANID,(PTRSTART + 1),  CODELEN );
    RETURN RETURN_VAL;
  EXCEPTION
        WHEN BAD_DATA THEN
           RETURN RETURN_VAL;
        WHEN OTHERS THEN
          RETURN_VAL := SQLERRM;
          RETURN RETURN_VAL;
  end;
/*
 */
  function GET_SPI_WORK_ORDER
  (
    P_TRANID varchar2
  ) return varchar2
  IS
      RETURN_VAL  VARCHAR2(20);
      TR_TRANID   VARCHAR2(200);

      ptrstart   PLS_INTEGER;
      ptrend     PLS_INTEGER;
      codeLen    PLS_INTEGER;
      BAD_DATA   EXCEPTION;
  begin

    TR_TRANID := TRIM(P_TRANID);

    PTRSTART := INSTR(TR_TRANID,'-',1,1);
    PTREND   := INSTR(TR_TRANID,'-',1,2);
    codeLen  := ( (INSTR(TR_TRANID,'-',1,2)) - (INSTR(TR_TRANID,'-',1,1))  - 1);

    RETURN_VAL := CASE WHEN ptrstart = 0  THEN 'BAD_DATA'
                       WHEN ptrend   = 0  THEN 'BAD_DATA'
                       WHEN codeLen  < 0  THEN 'BAD_DATA'
                  END;
    IF (RETURN_VAL = 'BAD_DATA')
       THEN
          RAISE BAD_DATA;
    END IF;

    RETURN_VAL :=  SUBSTR(TR_TRANID,1,(PTRSTART - 1));
    RETURN RETURN_VAL;
  EXCEPTION
        WHEN BAD_DATA THEN
           RETURN RETURN_VAL;
        WHEN OTHERS THEN
          RETURN_VAL := SQLERRM;
          RETURN RETURN_VAL;
  end;
/*
 */

  function GET_SPI_order_date
  (
    P_TRANID varchar2
  ) return varchar2
  IS
      RETURN_VAL  VARCHAR2(20);
      TR_TRANID   VARCHAR2(300);

      ptrstart   PLS_INTEGER;
      ptrend     PLS_INTEGER;
      codeLen    PLS_INTEGER;
      BAD_DATA   EXCEPTION;
  begin

    TR_TRANID := TRIM(P_TRANID);


    PTRSTART := INSTR(TR_TRANID,'-',1,1);
    PTREND   := INSTR(TR_TRANID,'-',1,2);
    codeLen  := ( (INSTR(TR_TRANID,'-',1,2)) - (INSTR(TR_TRANID,'-',1,1))  - 1);

    RETURN_VAL := CASE WHEN ptrstart = 0  THEN 'BAD_DATA'
                       WHEN ptrend   = 0  THEN 'BAD_DATA'
                       WHEN codeLen  < 0  THEN 'BAD_DATA'
                  END;
    IF (RETURN_VAL = 'BAD_DATA')
       THEN
          RAISE BAD_DATA;
    END IF;

    RETURN_VAL :=  SUBSTR(TR_TRANID,PTREND + 1);

    RETURN RETURN_VAL;
  EXCEPTION
        WHEN BAD_DATA THEN
           RETURN RETURN_VAL;
        WHEN OTHERS THEN
          RETURN_VAL := SQLERRM;
          RETURN RETURN_VAL;
  end;
/*********************************
    GET_SPI_VALID_DATE
 ********************************/

  function GET_SPI_valid_date
  (
    P_TRANID varchar2
  ) return number
  IS
      RETURN_VAL number;
      TR_TRANID  VARCHAR2(300);
      ptryr      PLS_INTEGER;
      ptrmo      PLS_INTEGER;
      ptrdd      pls_integer;

      bad_data   EXCEPTION;
  begin

  ---INSTR('2013-08-13-06','-',1,1),INSTR('2013-08-13-06','-',1,2),INSTR('2013-08-13-06','-',1,3)

    PTRYR    := INSTR(TR_TRANID,'-',1,1);
    PTRMO    := INSTR(TR_TRANID,'-',1,2);
    PTRDD    := INSTR(TR_TRANID,'-',1,3);


    RETURN_VAL := CASE WHEN PTRYR    = 0  THEN  0
                       WHEN PTRMO    = 0  THEN  0
                       WHEN PTRDD    = 0  THEN  0
                       WHEN PTRYR    = 5  AND PTRMO = 8 AND PTRDD = 11 THEN 1
                  ELSE 0
                  END;

    RETURN RETURN_VAL;
  EXCEPTION
         WHEN  OTHERS THEN
          RETURN_VAL := SQLCODE;
          RETURN RETURN_VAL;
  end;

/*****************************************
       LOG_FILENAME

 *****************************************/

   procedure  LOG_FILENAME( P_CLIENT VARCHAR2, P_FILENAME VARCHAR2, P_ID OUT NUMBER, P_MESSAGE OUT VARCHAR2)
   IS
    SQLCOUNTS  PLS_INTEGER;
    MSG         VARCHAR2(1000);
    MESSAGE     VARCHAR2(1000);
    PROC        VARCHAR2(1000);
    SubProc     VARCHAR2(1000);
    RCODE       NUMBER(10);
    sql_stmt    VARCHAR2(32000);
    TR_FILENAME VARCHAR2(200);

    BAD_DATA   EXCEPTION;

  BEGIN


     SQLCOUNTS    := 0;
     MSG          := 'Job Starting';
     PROC         := 'BOA_FEES_VALIDATE';
     SubProc      := 'LOG_FILENAME';


  INSERT INTO BOA_PROCESS_LOG
  (
    PROCESS,
    SUB_PROCESS,
    ENTRYDTE,
    ROWCOUNTS,
    MESSAGE
  )
  VALUES ( proc,SubProc,SYSDATE, sqlcounts, MSG);

  COMMIT;
/*
   TR_FILENAME := CASE WHEN P_CLIENT IN ('CHL')    THEN TRIM(REPLACE(P_FILENAME,'C:\boaData\CHL\',''))
                       WHEN P_CLIENT IN ('BANA')   THEN TRIM(REPLACE(P_FILENAME,'C:\boaData\BANA\',''))
                       WHEN P_CLIENT IN ('BACFS')  THEN TRIM(REPLACE(P_FILENAME,'C:\boaData\BACFS\',''))
'%Safeguard_Transmit%'
ARInvI_131231-00.txt
ARInvC_131216-00.txt
                  END;

*/

    MESSAGE   := CASE WHEN P_CLIENT IN ('CHL')    AND UPPER(P_FILENAME) LIKE 'SGP_FEES_%'           THEN 'LOAD_CHL_FEES_DIR'
                      WHEN P_CLIENT IN ('BANA')   AND UPPER(P_FILENAME) LIKE 'SGP_FEES_%'           THEN 'LOAD_BANA_FEES_DIR'
                      WHEN P_CLIENT IN ('BACFS')  AND UPPER(P_FILENAME) LIKE 'BOA_FEESRETURN_%'     THEN 'LOAD_BOA_FEESRETURN'
                      WHEN P_CLIENT IN ('CHL')    AND UPPER(P_FILENAME) LIKE 'SGP_PPOINVOICING_%'   THEN 'LOAD_CHL_PPOINVOICING'
                      WHEN P_CLIENT IN ('BANA')   AND UPPER(P_FILENAME) LIKE 'SGP_PPOINVOICING_%'   THEN 'LOAD_BANA_PPOINVOICING'
                      WHEN P_CLIENT IN ('AR')     AND UPPER(P_FILENAME) LIKE 'ARINVI_%'             THEN 'LOAD_ARINVOICING_DIR'
                      WHEN P_CLIENT IN ('AR')     AND UPPER(P_FILENAME) LIKE 'ARINVC_%'             THEN 'LOAD_ARINVOICING_DIR'
                      WHEN P_CLIENT IN ('SVCREL') AND  P_FILENAME LIKE '%Safeguard_Transmit%'       THEN 'BOA_SVC_REL_DIR'
                      WHEN P_CLIENT IN ('CHL')    AND  P_FILENAME LIKE '%DuplicateOrderCheckSGP_%'  THEN 'XCL_CHL_SWEEP_DIR'
                      WHEN P_CLIENT IN ('FILE_ERRORS') AND  P_FILENAME IS NOT NULL                  THEN 'FILE_ERROR_DIR'
                      when P_CLIENT IN ('Loss_Analyst')                                             THEN 'SGP_LA_FILES'

                 ELSE
                      'UNKNOWN-SOURCE-FOUND'
                 END;


    IF  ( MESSAGE =  'UNKNOWN SOURCE-FOUND')
        THEN
             P_ID      := -6502;
             P_MESSAGE := MESSAGE;
             RAISE BAD_DATA;

    END IF;
------------------- Check if we processed this file before
    BEGIN
         INSERT INTO  BOFA_FILES_PROCESSED ( PID, CLIENT, FILE_NAME, RECORDCNT,ENTRY_DATE,COMMENTS)
         VALUES ( bofa_files_processed_seq.NEXTVAL,P_CLIENT,P_FILENAME,0,SYSDATE, MESSAGE)
         RETURNING PID INTO P_ID;
         COMMIT;


         P_MESSAGE  := MESSAGE;
    EXCEPTION
       WHEN OTHERS THEN
          MSG       := SQLERRM;

          sqlCounts := 0;

          INSERT INTO BOA_PROCESS_LOG
          (
            PROCESS,
            SUB_PROCESS,
            ENTRYDTE,
            ROWCOUNTS,
            MESSAGE
          )
          VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG);

          COMMIT;

         P_ID      := -6502;
         P_MESSAGE := MSG;
    END;


  /************
    THEE END
   ***********/

  MSG       := 'Job Complete';
  sqlCounts := 0;
  INSERT INTO BOA_PROCESS_LOG
  (
    PROCESS,
    SUB_PROCESS,
    ENTRYDTE,
    ROWCOUNTS,
    MESSAGE
  )
  VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG);

  COMMIT;
  /******************************
     Record any errors
   *****************************/

  exception
       WHEN BAD_DATA  THEN
         INSERT INTO BOA_PROCESS_LOG
          (
            PROCESS,
            SUB_PROCESS,
            ENTRYDTE,
            ROWCOUNTS,
            MESSAGE
          )
          VALUES ( proc, SubProc,SYSDATE, 0, MESSAGE);

          COMMIT;

       WHEN OTHERS THEN

       MSG   := SQLERRM;
       RCODE := SQLCODE;

      INSERT INTO BOA_PROCESS_LOG
      (
        PROCESS,
        SUB_PROCESS,
        ENTRYDTE,
        ROWCOUNTS,
        MESSAGE
      )
      VALUES ( proc, SubProc,SYSDATE, RCODE, MSG);

      COMMIT;

  END;


/******************************************
      LOAD BOA SUMMARY
 ******************************************/

  PROCEDURE  LOAD_BOA_SUMMARY
  IS

  SQLCOUNTS  PLS_INTEGER;
  MSG        VARCHAR2(1000);
  PROC       VARCHAR2(1000);
  SubProc    VARCHAR2(1000);

  sql_stmt   VARCHAR2(32000);

  BEGIN


  SQLCOUNTS  := 0;
  MSG        := 'Job Starting';
  PROC       := 'BOA_FEES_VALIDATE';
  SubProc    := 'LOAD_BOA_SUMMARY';


  INSERT INTO BOA_PROCESS_LOG
  (
    PROCESS,
    SUB_PROCESS,
    ENTRYDTE,
    ROWCOUNTS,
    MESSAGE
  )
  VALUES ( proc,SubProc,SYSDATE, sqlcounts, MSG);

  COMMIT;


  SQL_STMT := ' TRUNCATE TABLE BOA_SUMMARY_GOOD DROP STORAGE';

  EXECUTE IMMEDIATE SQL_STMT;


  INSERT INTO  BOA_SUMMARY_GOOD (TRANSACTION,    TRANID, WORK_ORDER, DONE_CD, ORDER_DT, TRANKY, ERROR_CODE, ERROR_MESSAGE, RESOLVED, RESEARCHING, RESENT, DEV, BILLED, SOURCE, LOAN, TRAN_DT, TRANCODE, PRICE, WT, SWT, PRICEPER, QTY, UOM, SERVICEID, ZIP, STATE)
  SELECT a.TRANSACTION_TYPE, a.TRANID, a.WORK_ORDER, a.DONE_CD, a.ORDER_DT, a.TRANKY, a.ERROR_CODE, a.ERROR_MESSAGE, a.RESOLVED, a.RESEARCHING, a.RESENT, a.DEV, a.BILLED, r.sources, r.Loan, r.TRAN_DT, r.TRANCODE, r.PRICE,r.WT,r.SWT,r.PRICEPER,r.QTY,r.UOM,r.ServiceID,r.ZIP,r.ST
  FROM BOA_FEESRETURN_ENHANCED_GOOD A
  LEFT JOIN ( select b.TRANID, b.sources, b.Loan, b.TRAN_DT, b.TRANCODE, b.PRICE,b.WT,b.SWT,b.PRICEPER,b.QTY,b.UOM,b.ServiceID,b.ZIP,B.ST
              from ( SELECT 'SGP_FEES' as SOURCES,
                              A.TRANID,
                              A.Loan,
                              A.SentDate AS TRAN_DT,
                              A.TRANCODE,
                              A.PRICE,
                              A.WT,
                              A.SWT,
                              A.PRICEPER,
                              A.QTY,
                              A.UOM,
                              A.ServiceID,
                              A.ZIP,
                              B.State as ST
                            FROM SGP_FEES A
                            LEFT JOIN ( SELECT ZIPCODE, STATE, CITY FROM ZipCodes) B ON (A.ZIP = B.ZipCode)
                            UNION
                            SELECT
                            'SGP_PPOINVOICING' as SOURCES,
                            C.TRANID,
                            ISDIGIT(C.Loan) AS LOAN,
                            C.SENTDATE As TRAN_DT,
                            C.TRANCODE,
                            ISDIGIT(replace(c.price,'$','0')) AS PRICE,
                            ISDIGIT(C.WT) AS WT,
                            ISDIGIT(C.SWT) AS SWT,
                            ISDIGIT(replace(C.PRICEPER,'$','0')) AS PRICEPER,
                            ISDIGIT(C.QTY) AS QTY,
                            C.UOM,
                            ISDIGIT(C.ServiceID) AS SERVICEID,
                            C.ZIP,
                            D.State as ST
                            FROM SGP_PPOINVOICING  C
                            LEFT JOIN ( SELECT ZIPCODE, STATE, CITY FROM ZipCodes ) D ON (C.ZIP = D.ZipCode) ) b ) r
                          on ( TRIM(r.tranid)  = TRIM(a.tranid ));

  SQLCOUNTS  := SQL%ROWCOUNT;

  MSG  := 'BOA_SUMMARY_GOOD Loaded ';

  COMMIT;


  INSERT INTO BOA_PROCESS_LOG
  (
    PROCESS,
    SUB_PROCESS,
    ENTRYDTE,
    ROWCOUNTS,
    MESSAGE
  )
  VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG);

  COMMIT;


  SQL_STMT := ' TRUNCATE TABLE BOA_SUMMARY DROP STORAGE';

  EXECUTE IMMEDIATE SQL_STMT;

  INSERT INTO  BOA_SUMMARY (TRANSACTION,    TRANID, WORK_ORDER, DONE_CD, ORDER_DT, TRANKY, ERROR_CODE, ERROR_MESSAGE, RESOLVED, RESEARCHING, RESENT, DEV, BILLED, SOURCE, LOAN, TRAN_DT, TRANCODE, PRICE, WT, SWT, PRICEPER, QTY, UOM, SERVICEID, ZIP, STATE)
  SELECT a.TRANSACTION_TYPE, a.TRANID, a.WORK_ORDER, a.DONE_CD, a.ORDER_DT, a.TRANKY, a.ERROR_CODE, a.ERROR_MESSAGE, a.RESOLVED, a.RESEARCHING, a.RESENT, a.DEV, a.BILLED, r.sources, r.Loan, r.TRAN_DT, r.TRANCODE, r.PRICE,r.WT,r.SWT,r.PRICEPER,r.QTY,r.UOM,r.ServiceID,r.ZIP,r.ST
  FROM BOA_FEESRETURN_ENHANCED A
  LEFT JOIN ( select b.TRANID, b.sources, b.Loan, b.TRAN_DT, b.TRANCODE, b.PRICE,b.WT,b.SWT,b.PRICEPER,b.QTY,b.UOM,b.ServiceID,b.ZIP,B.ST
              from ( SELECT 'SGP_FEES' as SOURCES,
                              A.TRANID,
                              A.Loan,
                              A.SentDate AS TRAN_DT,
                              A.TRANCODE,
                              A.PRICE,
                              A.WT,
                              A.SWT,
                              A.PRICEPER,
                              A.QTY,
                              A.UOM,
                              A.ServiceID,
                              A.ZIP,
                              B.State as ST
                            FROM SGP_FEES A
                            LEFT JOIN ( SELECT ZIPCODE, STATE, CITY FROM ZipCodes) B ON (A.ZIP = B.ZipCode)
                            UNION ALL
                            SELECT
                            'SGP_PPOINVOICING' as SOURCES,
                            C.TRANID,
                            ISDIGIT(C.Loan) AS LOAN,
                            C.SENTDATE As TRAN_DT,
                            C.TRANCODE,
                            ISDIGIT(replace(c.price,'$','0')) AS PRICE,
                            ISDIGIT(C.WT) AS WT,
                            ISDIGIT(C.SWT) AS SWT,
                            ISDIGIT(replace(C.PRICEPER,'$','0')) AS PRICEPER,
                            ISDIGIT(C.QTY) AS QTY,
                            C.UOM,
                            ISDIGIT(C.ServiceID) AS SERVICEID,
                            C.ZIP,
                            D.State as ST
                            FROM SGP_PPOINVOICING  C
                            LEFT JOIN ( SELECT ZIPCODE, STATE, CITY FROM ZipCodes ) D ON (C.ZIP = D.ZipCode) ) b ) r
            on ( TRIM(r.tranid )   = TRIM(a.tranid) );

  sqlcounts := sql%rowcount;

  MSG  := 'BOA_SUMMARY bad Loaded ';

  COMMIT;

  INSERT INTO BOA_PROCESS_LOG
  (
    PROCESS,
    SUB_PROCESS,
    ENTRYDTE,
    ROWCOUNTS,
    MESSAGE
  )
  VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG);

  COMMIT;

  /************
    THEE END
   ***********/

  MSG       := 'Job Complete';
  sqlCounts := 0;
  INSERT INTO BOA_PROCESS_LOG
  (
    PROCESS,
    SUB_PROCESS,
    ENTRYDTE,
    ROWCOUNTS,
    MESSAGE
  )
  VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG);

  COMMIT;
  /******************************
     Record any errors
   *****************************/

  exception
       WHEN OTHERS THEN

       MSG  := SQLERRM;

  INSERT INTO BOA_PROCESS_LOG
  (
    PROCESS,
    SUB_PROCESS,
    ENTRYDTE,
    ROWCOUNTS,
    MESSAGE
  )
  VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG);

  COMMIT;

  END;

/**********************************************
   LOAD SGP FEES
 **********************************************/

  procedure  LOAD_SGP_FEES(P_FILENAME IN VARCHAR2, P_ID IN NUMBER, P_MESSAGE IN VARCHAR2, P_RCODE OUT NUMBER )
  IS

  SQLCOUNTS  PLS_INTEGER;
  MSG        VARCHAR2(1000);
  PROC       VARCHAR2(1000);
  SubProc    VARCHAR2(1000);
  rCode      number(10);
  sql_stmt   VARCHAR2(32000);

   BAD_DATA  EXCEPTION;

  BEGIN


    SQLCOUNTS  := 0;
    MSG        := 'Job Starting';
    PROC       := 'BOA_FEES_VALIDATE';
    SubProc    := 'LOAD_SGP_FEES';
    vTEAM      := 'BOA IRecon';

  INSERT INTO BOA_PROCESS_LOG
  (
    PROCESS,
    SUB_PROCESS,
    ENTRYDTE,
    ROWCOUNTS,
    MESSAGE
  )
  VALUES ( proc,SubProc,SYSDATE, sqlcounts, MSG);


  COMMIT;

     IF  P_ID  < 0
       THEN
       rCode := -6502;
       MSG   := 'ERROR OCCURED LOADING FILE NAME ';
       RAISE BAD_DATA;
    END IF;

      SQL_STMT :=  CASE WHEN P_MESSAGE = 'LOAD_CHL_FEES_DIR'           THEN  'ALTER TABLE CHL_FEES_EXT  LOCATION (LOAD_CHL_FEES_DIR:'''||P_FILENAME||''') '
                        WHEN P_MESSAGE = 'LOAD_BANA_FEES_DIR'          THEN  'ALTER TABLE BANA_FEES_EXT  LOCATION (LOAD_BANA_FEES_DIR:'''||P_FILENAME||''') '
                        else 'UNKNOWN-WORKING-DIRECTORY'
                   END;

      IF  ( P_MESSAGE IN ('LOAD_CHL_FEES_DIR','LOAD_BANA_FEES_DIR') )
           THEN
               EXECUTE IMMEDIATE SQL_STMT;
      ELSE
          rCode := -6502;
          RAISE BAD_DATA;
      END IF;


      IF  ( P_MESSAGE IN ('LOAD_CHL_FEES_DIR') )
          THEN
            insert into  SGP_FEES (  TRANID, LOAN, SENTDATE, TRANCODE, PRICE, FIELD6, FIELD7, FIELD8, FIELD9, FIELD10, FIELD11, WT, SWT, PRICEPER, QTY, UOM, SERVICEID, ZIP,PID)
            select TRANID, LOAN, SENTDATE, TRANCODE, PRICE, FIELD6, FIELD7, FIELD8, FIELD9, FIELD10, FIELD11, WT, SWT, PRICEPER, QTY, UOM, SERVICEID, ZIP,P_ID
            FROM CHL_FEES_EXT;

           SQLCOUNTS  := SQL%ROWCOUNT;

           MSG  := P_FILENAME||' CHL FILE LOADED into SGP_FEES';

          COMMIT;
      ELSIF ( P_MESSAGE IN ('LOAD_BANA_FEES_DIR') )
         THEN
            insert into  SGP_FEES (  TRANID, LOAN, SENTDATE, TRANCODE, PRICE, FIELD6, FIELD7, FIELD8, FIELD9, FIELD10, FIELD11, WT, SWT, PRICEPER, QTY, UOM, SERVICEID, ZIP,PID)
            select TRANID, LOAN, SENTDATE, TRANCODE, PRICE, FIELD6, FIELD7, FIELD8, FIELD9, FIELD10, FIELD11, WT, SWT, PRICEPER, QTY, UOM, SERVICEID, ZIP,P_ID
            FROM BANA_FEES_EXT;

           SQLCOUNTS  := SQL%ROWCOUNT;

           MSG  := P_FILENAME||' BANA FILE LOADED into SGP_FEES';

          COMMIT;
      END IF;

--------------------------
-- UPDATE LOG
-------------------------
    UPDATE BOFA_FILES_PROCESSED
    SET RECORDCNT = SQLCOUNTS, COMMENTS = MSG
    WHERE PID  = P_ID;

    COMMIT;

    INSERT INTO SGP_FEES_TOSEND(PID, CNT)
    VALUES ( P_ID, SQLCOUNTS);
    COMMIT;

    INSERT INTO SGP_PID_CLEANUP ( PID, TABLE_NAME, COMPLETED, ENTRYDATE)
    VALUES ( P_ID, 'SGP_FEES', 0, SYSDATE);
    COMMIT;



  INSERT INTO BOA_PROCESS_LOG
  (
    PROCESS,
    SUB_PROCESS,
    ENTRYDTE,
    ROWCOUNTS,
    MESSAGE
  )
  VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG);

  COMMIT;

  /************
    THEE END
   ***********/

   MSG       := P_FILENAME||':Job Complete';

  sqlCounts := 0;

  INSERT INTO BOA_PROCESS_LOG
  (
    PROCESS,
    SUB_PROCESS,
    ENTRYDTE,
    ROWCOUNTS,
    MESSAGE
  )
  VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG);

  COMMIT;

  P_RCODE := 0;

  /******************************
     Record any errors
   *****************************/

  EXCEPTION

       WHEN BAD_DATA THEN

       P_RCODE  := rCode;

        INSERT INTO BOA_PROCESS_LOG
        (
          PROCESS,
          SUB_PROCESS,
          ENTRYDTE,
          ROWCOUNTS,
          MESSAGE
        )
           VALUES ( proc, SubProc, SYSDATE, rCode, P_FILENAME||': '||MSG);

            COMMIT;
       SEND_EMAIL  ( vTEAM, proc||'.'||subProc, 'Message From Load process', MSG);
       WHEN OTHERS THEN

       MSG  := SQLERRM;

     P_RCODE := sqlcode;
       rCode := sqlcode;
  INSERT INTO BOA_PROCESS_LOG
  (
    PROCESS,
    SUB_PROCESS,
    ENTRYDTE,
    ROWCOUNTS,
    MESSAGE
  )
  VALUES ( proc, SubProc,SYSDATE, rCode , P_FILENAME||': '||MSG);

  COMMIT;
            SEND_EMAIL  ( vTEAM, proc||'.'||subProc, 'Message From Load process', MSG);

  END;

  /*********************************************************
    LOAD_SGP_FEES_ENHANCED
    MODIFIED ON 09/10/2013  to process only the new files
   *********************************************************/
  procedure  LOAD_SGP_FEES_ENHANCED
  IS

    CURSOR C2
    IS
    SELECT A.PID
    FROM SGP_FEES A
    LEFT JOIN ( SELECT PID FROM SGP_FEES_LOADED_PIDS ) B ON ( B.PID = A.PID )
    WHERE B.PID IS NULL
    GROUP BY A.PID
    ORDER BY A.PID;

    R2   C2%ROWTYPE;

  SQLCOUNTS  PLS_INTEGER;
  MSG        VARCHAR2(1000);
  PROC       VARCHAR2(1000);
  SubProc    VARCHAR2(1000);

  sql_stmt   VARCHAR2(32000);

  BEGIN

    SQLCOUNTS  := 0;
    MSG        := 'Job Starting';
    PROC       := 'BOA_FEES_VALIDATE';
    SubProc    := 'LOAD_SGP_FEES_ENHANCED';

  INSERT INTO BOA_PROCESS_LOG
  (
    PROCESS,
    SUB_PROCESS,
    ENTRYDTE,
    ROWCOUNTS,
    MESSAGE
  )
  VALUES ( proc,SubProc,SYSDATE, sqlcounts, MSG);

  COMMIT;


--   SQL_STMT := ' TRUNCATE TABLE SGP_FEES_ENHANCED DROP STORAGE';

--     EXECUTE IMMEDIATE SQL_STMT;

    OPEN C2;
      LOOP
        FETCH C2 INTO R2;
        EXIT WHEN C2%NOTFOUND;

        INSERT INTO  SGP_FEES_ENHANCED (TRANID, WORK_ORDER, DONE_CD, ORDER_DT, LOAN, SENTDATE, TRANCODE, PRICE, FIELD6, FIELD7, FIELD8, FIELD9, FIELD10, FIELD11, WT, SWT, PRICEPER, QTY, UOM, SERVICEID, ZIP,PID )
        SELECT TRIM(A.TRANID),
                BOA_FEES_VALIDATE.GET_SPI_WORK_ORDER (A.TRANID) AS WORK_ORDER,
                BOA_FEES_VALIDATE.GET_SPI_PUNCH_CODE (A.TRANID) AS DONE_CD,
                BOA_FEES_VALIDATE.GET_SPI_order_date (A.TRANID) AS ORDER_DT,
                A.LOAN,
                A.SENTDATE,
                A.TRANCODE,
                A.PRICE,
                A.FIELD6,
                A.FIELD7,
                A.FIELD8,
                A.FIELD9,
                A.FIELD10,
                A.FIELD11,
                A.WT,
                A.SWT,
                A.PRICEPER,
                A.QTY,
                A.UOM,
                A.SERVICEID,
                A.ZIP,
                A.PID
           FROM SGP_FEES A
          WHERE a.PID = r2.pid;

         SQLCOUNTS  := SQL%ROWCOUNT;

         COMMIT;

        MSG := ' SGP_FEES to SGP_FEES ENHANCED.. pid '||r2.pid;

        INSERT INTO SGP_FEES_LOADED_PIDS
        VALUES (R2.PID, SQLCOUNTS);
        COMMIT;

        INSERT INTO BOA_PROCESS_LOG
        (
          PROCESS,
          SUB_PROCESS,
          ENTRYDTE,
          ROWCOUNTS,
          MESSAGE
        )
        VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG);

        COMMIT;

      END LOOP;

     CLOSE C2;

      SQL_STMT := ' INSERT INTO IPP$LIBRARIAN.BOA_FEE_STATUS_LOG@'||vDB_LINK||' ( PID, PROCESS, PER_COMPETE, ENTRY_DATE) ';
      SQL_STMT := SQL_STMT||' VALUES ( :1, :2, :3, :4 )';

      EXECUTE IMMEDIATE SQL_STMT USING 2, 'Enhance Fees and Invoices' , 50 ,SYSDATE;

      COMMIT;

  /************
    THEE END
   ***********/

   MSG  := 'SGP_FEES_ENHANCED Loaded ';
   sqlCounts := 0;

    INSERT INTO BOA_PROCESS_LOG
    (
      PROCESS,
      SUB_PROCESS,
      ENTRYDTE,
      ROWCOUNTS,
      MESSAGE
    )
    VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG);

    COMMIT;
  /******************************
     Record any errors
   *****************************/

  exception
       WHEN OTHERS THEN

       MSG  := SQLERRM;

  INSERT INTO BOA_PROCESS_LOG
  (
    PROCESS,
    SUB_PROCESS,
    ENTRYDTE,
    ROWCOUNTS,
    MESSAGE
  )
  VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG);

  COMMIT;

  END;

/******************************************************
    LOAD_BOA_FEESRETURN
 ******************************************************/
   procedure  LOAD_BOA_FEESRETURN(P_FILENAME IN VARCHAR2, P_ID IN NUMBER, P_MESSAGE IN VARCHAR2, P_RCODE OUT NUMBER)

   IS

    SQLCOUNTS  PLS_INTEGER;
    MSG        VARCHAR2(1000);
    PROC       VARCHAR2(1000);
    SubProc    VARCHAR2(1000);
    rCode      NUMBER;

    sql_stmt   VARCHAR2(32000);
    whoami     varchar2(100);

     BAD_DATA  EXCEPTION;

  BEGIN


    SQLCOUNTS  := 0;
    MSG        := 'Job Starting';
    PROC       := 'BOA_FEES_VALIDATE';
    SubProc    := 'LOAD_BOA_FEESRETURN';
    rCode      := 0;
    vTEAM      := 'BOA IRecon';

  INSERT INTO BOA_PROCESS_LOG
  (
    PROCESS,
    SUB_PROCESS,
    ENTRYDTE,
    ROWCOUNTS,
    MESSAGE
  )
  VALUES ( proc,SubProc,SYSDATE, sqlcounts, MSG);

  COMMIT;


   IF  P_ID  < 0
       THEN
       MSG  := 'ERROR OCCURED LOADING FILE NAME ';
       rCode := -6502;

       RAISE BAD_DATA;
    END IF;


   IF P_MESSAGE NOT IN ('LOAD_BOA_FEESRETURN')
    THEN
      MSG   := P_MESSAGE||': INVALID WORKING DIRECTORY';
      rCode := -6502;
      RAISE BAD_DATA;
   END IF;

    SQL_STMT := 'ALTER TABLE BOA_FEESRETURN_EXT LOCATION (LOAD_BOA_FEESRETURN:'''||P_FILENAME||''') ';

   EXECUTE IMMEDIATE SQL_STMT;

    INSERT INTO BOA_FEESRETURN (  TRANSACTION_TYPE, TRANID, TRANKY, ERROR_CODE, ERROR_MESSAGE, RESOLVED, RESEARCHING, RESENT, DEV, BILLED ,PID)
    SELECT   TRANSACTION_TYPE, TRANID, TRANKY, ERROR_CODE, ERROR_MESSAGE, RESOLVED, RESEARCHING, RESENT, DEV, nvl(BILLED,0),P_ID
    FROM BOA_FEESRETURN_EXT;

     SQLCOUNTS  := SQL%ROWCOUNT;

     MSG  := P_FILENAME||' Loaded into Boa_Feesreturn';

    COMMIT;

--------------------------
-- UPDATE LOG
-------------------------
    UPDATE BOFA_FILES_PROCESSED
    SET RECORDCNT = SQLCOUNTS, COMMENTS = MSG
    WHERE PID  = P_ID;

    COMMIT;

    insert into BOA_FEESRETURN_TOSEND (pid, cnt)
    values (P_ID, SQLCOUNTS);
    COMMIT;

    INSERT INTO SGP_PID_CLEANUP ( PID, TABLE_NAME, COMPLETED, ENTRYDATE)
    VALUES ( P_ID, 'BOA_FEESRETURN', 0, SYSDATE);
    COMMIT;


  INSERT INTO BOA_PROCESS_LOG
  (
    PROCESS,
    SUB_PROCESS,
    ENTRYDTE,
    ROWCOUNTS,
    MESSAGE
  )
  VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG);

  COMMIT;

    INSERT INTO BOA_BILLABLE_WORKORDERS_PID(WORK_ORDER, TRANKY, ENTRYDT, CREATED_BY, PID)
    SELECT  BOA_FEES_VALIDATE.GET_SPI_WORK_ORDER (TRANID) AS WORK_ORDER,TRANKY,SYSDATE, whoami, P_ID
    FROM BOA_FEESRETURN_EXT
    where ERROR_CODE = 0
      AND  substr(TRANKY, 1, 1) IN ('0','1','2','3','4','5','6','7','8','9');

     SQLCOUNTS  := SQL%ROWCOUNT;

     MSG  := 'Billable accounts from '||P_FILENAME||' loaded into BOA_BILLABLE_WORKORDERS';

    COMMIT;




  INSERT INTO BOA_PROCESS_LOG
  (
    PROCESS,
    SUB_PROCESS,
    ENTRYDTE,
    ROWCOUNTS,
    MESSAGE
  )
  VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG);

  COMMIT;
/*
        EXECUTE IMMEDIATE 'TRUNCATE TABLE BOA_BILLABLE_WO_UNQ DROP STORAGE';

        INSERT INTO   BOA_BILLABLE_WO_UNQ (   WORK_ORDER ,  TRANKY,  ENTRYDT,  CREATED_BY)
        SELECT  R.WORK_ORDER ,  R.TRANKY,  R.ENTRYDT,  R.CREATED_BY
        FROM ( SELECT WORK_ORDER ,  TRANKY,  ENTRYDT,  CREATED_BY, RANK() OVER( PARTITION BY  WORK_ORDER, TRANKY ORDER BY ENTRYDT desc, ROWNUM ) RK
               FROM  BOA_BILLABLE_WORKORDERS_PID ) R
        WHERE R.RK = 1;

          SQLCOUNTS  := SQL%ROWCOUNT;

      COMMIT;


      MSG        := 'Remove duplicates from boa billable work orders ';
*/

  INSERT INTO BOA_PROCESS_LOG
      (
        PROCESS,
        SUB_PROCESS,
        ENTRYDTE,
        ROWCOUNTS,
        MESSAGE
      )
  VALUES ( proc,SubProc,SYSDATE, sqlcounts, MSG);

  COMMIT;

  /************
    THEE END
   ***********/

   MSG       := P_FILENAME||':Job Complete';

  sqlCounts := 0;
  p_rcode   := 0;

  INSERT INTO BOA_PROCESS_LOG
  (
    PROCESS,
    SUB_PROCESS,
    ENTRYDTE,
    ROWCOUNTS,
    MESSAGE
  )
  VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG);

  COMMIT;


  /******************************
     Record any errors
   *****************************/

  exception
       WHEN  BAD_DATA THEN
         p_rcode  := rcode;
        INSERT INTO BOA_PROCESS_LOG
        (
          PROCESS,
          SUB_PROCESS,
          ENTRYDTE,
          ROWCOUNTS,
          MESSAGE
        )
        VALUES ( proc, SubProc,SYSDATE, rCode, P_FILENAME||': '||MSG);

        COMMIT;

       SEND_EMAIL  ( vTEAM, proc||'.'||subProc, 'Message From Load process', MSG);

       WHEN OTHERS THEN

       MSG     := SQLERRM;
       rcode   := sqlcode;
       p_rcode := sqlcode;

        INSERT INTO BOA_PROCESS_LOG
        (
          PROCESS,
          SUB_PROCESS,
          ENTRYDTE,
          ROWCOUNTS,
          MESSAGE
        )
        VALUES ( proc, SubProc,SYSDATE, rcode, P_FILENAME||': '||MSG);

        COMMIT;

        SEND_EMAIL  ( vTEAM, proc||'.'||subProc, 'Message From Load process', MSG);

  END;

/******************************************************
    LOAD_SGP_PPOINVOICING

 ******************************************************/
  procedure  LOAD_SGP_PPOINVOICING(P_FILENAME IN VARCHAR2, P_ID IN NUMBER, P_MESSAGE IN VARCHAR2, p_rcode out number )

   IS

    SQLCOUNTS  PLS_INTEGER;
    MSG        VARCHAR2(1000);
    PROC       VARCHAR2(1000);
    SubProc    VARCHAR2(1000);
    rcode      NUMBER;

    sql_stmt   VARCHAR2(32000);
    whoami     varchar2(100);
    BAD_DATA   EXCEPTION;

  BEGIN


    SQLCOUNTS  := 0;
    MSG        := 'Job Starting';
    PROC       := 'BOA_FEES_VALIDATE';
    SubProc    := 'LOAD_SGP_PPOINVOICING';
    vTEAM      := 'BOA IRecon';

  INSERT INTO BOA_PROCESS_LOG
  (
    PROCESS,
    SUB_PROCESS,
    ENTRYDTE,
    ROWCOUNTS,
    MESSAGE
  )
  VALUES ( proc,SubProc,SYSDATE, sqlcounts, MSG);

  COMMIT;

   If ( p_id < 0 )
     then
       MSG :=   'an Error occured while attempt to log the file name ';
       rcode := -6502;
       RAISE BAD_DATA;
   END IF;

      SQL_STMT :=   CASE  WHEN P_MESSAGE = 'LOAD_CHL_PPOINVOICING'   THEN  'ALTER TABLE CHL_PPOINVOICING_EXT  LOCATION (LOAD_CHL_PPOINVOICING_DIR:'''||P_FILENAME||''') '
                          WHEN P_MESSAGE = 'LOAD_BANA_PPOINVOICING'  THEN  'ALTER TABLE BANA_PPOINVOICING_EXT  LOCATION (LOAD_BANA_PPOINVOICING_DIR:'''||P_FILENAME||''') '
                    ELSE  'UNKNOWN-WORKING-DIRECTORY'
                   END;

  IF (SQL_STMT IN ('UNKNOWN-WORKING-DIRECTORY') )
     THEN
     MSG   := SQL_STMT;
     RCODE := -6502;
     RAISE BAD_DATA;
  END IF;

    EXECUTE IMMEDIATE SQL_STMT;

 IF ( P_MESSAGE IN  ('LOAD_CHL_PPOINVOICING') )
     THEN
      insert into  SGP_PPOINVOICING(  TRANID, LOAN, SENTDATE, TRANCODE, PRICE, FIELD6, FIELD7, FIELD8, FIELD9, FIELD10, FIELD11, WT, SWT, PRICEPER, QTY, UOM, SERVICEID, ZIP,pid)
      select TRANID, LOAN, SENTDATE, TRANCODE, PRICE, FIELD6, FIELD7, FIELD8, FIELD9, FIELD10, FIELD11, WT, SWT, PRICEPER, QTY, UOM, SERVICEID, ZIP,P_ID
      from CHL_PPOINVOICING_EXT;
      SQLCOUNTS  := SQL%ROWCOUNT;
      MSG  := P_FILENAME||' CHL FILE LOADED into SGP_PPOINVOICING';

      COMMIT;
 ELSIF (  P_MESSAGE IN  ('LOAD_BANA_PPOINVOICING') )
     THEN
      insert into  SGP_PPOINVOICING(  TRANID, LOAN, SENTDATE, TRANCODE, PRICE, FIELD6, FIELD7, FIELD8, FIELD9, FIELD10, FIELD11, WT, SWT, PRICEPER, QTY, UOM, SERVICEID, ZIP,pid)
      select TRANID, LOAN, SENTDATE, TRANCODE, PRICE, FIELD6, FIELD7, FIELD8, FIELD9, FIELD10, FIELD11, WT, SWT, PRICEPER, QTY, UOM, SERVICEID, ZIP,P_ID
      from BANA_PPOINVOICING_EXT;
      SQLCOUNTS  := SQL%ROWCOUNT;
      MSG  := P_FILENAME||' BANA FILE LOADED into SGP_PPOINVOICING';

      COMMIT;
 END IF;

--------------------------
-- UPDATE LOG
-------------------------
    UPDATE BOFA_FILES_PROCESSED
    SET RECORDCNT = SQLCOUNTS, COMMENTS = MSG
    WHERE PID  = P_ID;

    COMMIT;

    INSERT INTO SGP_PPO_TOSEND(PID, CNT)
    VALUES ( P_ID, SQLCOUNTS);
    COMMIT;

    INSERT INTO SGP_PID_CLEANUP ( PID, TABLE_NAME, COMPLETED, ENTRYDATE)
    VALUES ( P_ID, 'SGP_PPOINVOICING', 0, SYSDATE);
    COMMIT;

  INSERT INTO BOA_PROCESS_LOG
  (
    PROCESS,
    SUB_PROCESS,
    ENTRYDTE,
    ROWCOUNTS,
    MESSAGE
  )
  VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG);

  COMMIT;

  /************
    THEE END
   ***********/

   MSG       := P_FILENAME||':Job Complete';

  sqlCounts := 0;

  INSERT INTO BOA_PROCESS_LOG
  (
    PROCESS,
    SUB_PROCESS,
    ENTRYDTE,
    ROWCOUNTS,
    MESSAGE
  )
  VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG);

  COMMIT;

  P_RCODE := 0;

  /******************************
     Record any errors
   *****************************/

  exception
        WHEN  BAD_DATA THEN
        P_RCODE := RCODE;
        INSERT INTO BOA_PROCESS_LOG
        (
          PROCESS,
          SUB_PROCESS,
          ENTRYDTE,
          ROWCOUNTS,
          MESSAGE
        )
        VALUES ( proc, SubProc,SYSDATE, rcode, P_FILENAME||': '||MSG);

        COMMIT;
                SEND_EMAIL  ( vTEAM, proc||'.'||subProc, 'Message From Load process', MSG);

      WHEN OTHERS THEN

       MSG  := SQLERRM;
       P_RCODE := SQLCODE;
         RCODE := SQLCODE;

  INSERT INTO BOA_PROCESS_LOG
  (
    PROCESS,
    SUB_PROCESS,
    ENTRYDTE,
    ROWCOUNTS,
    MESSAGE
  )
  VALUES ( proc, SubProc,SYSDATE, RCODE, P_FILENAME||': '||MSG);

  COMMIT;
            SEND_EMAIL  ( vTEAM, proc||'.'||subProc, 'Message From Load process', MSG);

  END;

 /********************************************
     procedure  LOAD_BOA_FEESRETURN_ENHANCED
 ********************************************/

 procedure  LOAD_BOA_FEESRETURN_ENHANCED
   IS

    SQLCOUNTS   PLS_INTEGER;
    MSG         VARCHAR2(1000);
    PROC        VARCHAR2(1000);
    SubProc     VARCHAR2(1000);

    sql_stmt    VARCHAR2(32000);
    ErrorCount  PLS_INTEGER;
    STMTNO      PLS_INTEGER;

    Rcode       number;

    wTranid     varchar2(100);

   TYPE REF_CURSOR IS REF CURSOR;

   GenRefCursor   REF_CURSOR;

/**********************************************************
   Create workarea
  ********************************************************/
 TYPE rowidArray is table of rowid index by binary_integer;

 TYPE   WORKAREA IS RECORD (
        TRANID          DBMS_SQL.VARCHAR2_TABLE,
        rowids          rowidArray
);

 WA WORKAREA;


  dml_errors       EXCEPTION;
  PRAGMA EXCEPTION_INIT(dml_errors, -24381);


  BEGIN

    SQLCOUNTS  := 0;
    MSG        := 'Job Starting';
    PROC       := 'BOA_FEES_VALIDATE';
    SubProc    := 'LOAD_BOA_FEESRETURN_ENHANCED';

  INSERT INTO BOA_PROCESS_LOG
  (
    PROCESS,
    SUB_PROCESS,
    ENTRYDTE,
    ROWCOUNTS,
    MESSAGE
  )
  VALUES ( proc,SubProc,SYSDATE, sqlcounts, MSG);

  COMMIT;
/*
  INSERT INTO BOA_PROCESS_LOG
  (
    PROCESS,
    SUB_PROCESS,
    ENTRYDTE,
    ROWCOUNTS,
    MESSAGE
  )
  SELECT  'BOA_FEES_VALIDATE', 'BEFORE FEESRETURN_BILLED', TRUNC(SYSDATE), BILLED, COUNT(*)
  FROM   BOA_FEESRETURN
  GROUP BY 'BOA_FEES_VALIDATE', 'BEFORE FEESRETURN_BILLED', TRUNC(SYSDATE), BILLED
  ORDER BY BILLED;

  COMMIT;
*/
    OPEN  GenRefCursor FOR select R.TRANID, R.ROWID
                            FROM BOA_FEESRETURN R
                            LEFT JOIN (select B.work_order, A.TRANID, A.BILLED
                                        from BOA_BILLABLE_WO_UNQ b
                                         LEFT JOIN( SELECT TRANID,
                                                    BOA_FEES_VALIDATE.GET_SPI_WORK_ORDER (TRANID) as work_order,
                                                    BOA_FEES_VALIDATE.GET_SPI_PUNCH_CODE (TRANID) as order_cd,
                                                    BOA_FEES_VALIDATE.GET_SPI_order_date (TRANID) as order_dt,
                                                    BILLED
                                               FROM BOA_FEESRETURN
                                              WHERE BILLED = 0 ) A
                                       on ( A.WORK_ORDER  = b.work_order)
                                     where  A.WORK_ORDER  = b.work_order) C
                               ON  ( R.TRANID   = C.TRANID)
                            WHERE R.TRANID   = C.TRANID
                              AND   R.BILLED   = 0;

           LOOP
                FETCH genRefCursor bulk collect into wa.tranid, wa.rowids limit  1000;
                exit when wa.tranid.count = 0;

                            SQLCOUNTs := SQLCOUNTs + WA.tranid.COUNT;

                    /********************************************/
                      ---  UPDATE IN BATCH  at the SIZE OF COMMIT RATE
                    /*******************************************/
                   FORALL J IN 1..WA.tranid.COUNT SAVE EXCEPTIONS
                   EXECUTE IMMEDIATE 'UPDATE BOA_FEESRETURN  SET billed = 1  where rowid = :a ' USING WA.Rowids(j);

                   commit;

           END LOOP;


   MSG       := 'Number of accounts marked billed';


  INSERT INTO BOA_PROCESS_LOG
  (
    PROCESS,
    SUB_PROCESS,
    ENTRYDTE,
    ROWCOUNTS,
    MESSAGE
  )
  VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG);

  COMMIT;
/*
  INSERT INTO BOA_PROCESS_LOG
  (
    PROCESS,
    SUB_PROCESS,
    ENTRYDTE,
    ROWCOUNTS,
    MESSAGE
  )
  SELECT  'BOA_FEES_VALIDATE', 'AFTER FEESRETURN_BILLED', TRUNC(SYSDATE), BILLED, COUNT(*)
  FROM   BOA_FEESRETURN
  GROUP BY 'BOA_FEES_VALIDATE', 'AFTER FEESRETURN_BILLED', TRUNC(SYSDATE), BILLED
  ORDER BY BILLED;

  COMMIT;



  execute immediate 'TRUNCATE TABLE BOA_FEESRETURN_ENHANCED DROP STORAGE';

  INSERT INTO BOA_FEESRETURN_ENHANCED (TRANSACTION_TYPE,
                                       TRANID,
                                       WORK_ORDER,
                                       DONE_CD,
                                       ORDER_DT,
                                       TRANKY,
                                       ERROR_CODE,
                                       ERROR_MESSAGE,
                                       RESOLVED,
                                       RESEARCHING,
                                       RESENT,
                                       DEV,
                                       BILLED)
     SELECT TRANSACTION_TYPE,
            TRANID,
            BOA_FEES_VALIDATE.GET_SPI_WORK_ORDER (TRANID),
            BOA_FEES_VALIDATE.GET_SPI_PUNCH_CODE (TRANID),
            BOA_FEES_VALIDATE.GET_SPI_order_date (TRANID),
            TRANKY,
            ERROR_CODE,
            ERROR_MESSAGE,
            RESOLVED,
            RESEARCHING,
            RESENT,
            DEV,
            BILLED
       FROM BOA_FEESRETURN
       WHERE BILLED = 0;

   SQLCOUNTS := SQL%ROWCOUNT;

   MSG       := 'Number of accounts loaded BOA_FEESRETURNED_ENHANCED';

  COMMIT;

  INSERT INTO BOA_PROCESS_LOG
  (
    PROCESS,
    SUB_PROCESS,
    ENTRYDTE,
    ROWCOUNTS,
    MESSAGE
  )
  VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG);

  COMMIT;


  execute immediate 'TRUNCATE TABLE BOA_FEESRETURN_ENHANCED_GOOD DROP STORAGE';

    INSERT INTO BOA_FEESRETURN_ENHANCED_GOOD (TRANSACTION_TYPE,
                                               TRANID,
                                               WORK_ORDER,
                                               DONE_CD,
                                               ORDER_DT,
                                               TRANKY,
                                               ERROR_CODE,
                                               ERROR_MESSAGE,
                                               RESOLVED,
                                               RESEARCHING,
                                               RESENT,
                                               DEV,
                                               BILLED)
       SELECT TRANSACTION_TYPE,
              TRANID,
              BOA_FEES_VALIDATE.GET_SPI_WORK_ORDER (TRANID),
              BOA_FEES_VALIDATE.GET_SPI_PUNCH_CODE (TRANID),
              BOA_FEES_VALIDATE.GET_SPI_order_date (TRANID),
              TRANKY,
              ERROR_CODE,
              ERROR_MESSAGE,
              RESOLVED,
              RESEARCHING,
              RESENT,
              DEV,
              BILLED
         FROM BOA_FEESRETURN
         WHERE BILLED = 1;

   SQLCOUNTS := SQL%ROWCOUNT;

   MSG       := 'Number of accounts loaded BOA_FEESRETURN_ENHANCED_GOOD';

  COMMIT;

  INSERT INTO BOA_PROCESS_LOG
  (
    PROCESS,
    SUB_PROCESS,
    ENTRYDTE,
    ROWCOUNTS,
    MESSAGE
  )
  VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG);

  COMMIT;

  */

  /************
    THEE END
   ***********/

   MSG       := 'Job Complete';

  sqlCounts := 0;

  INSERT INTO BOA_PROCESS_LOG
  (
    PROCESS,
    SUB_PROCESS,
    ENTRYDTE,
    ROWCOUNTS,
    MESSAGE
  )
  VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG);

  COMMIT;


  /******************************
     Record any errors
   *****************************/

  exception

          WHEN dml_errors THEN

                ErrorCount := SQL%BULK_EXCEPTIONS.COUNT;

                    FOR j IN 1 .. ErrorCount LOOP
                        rcode     := SQL%BULK_EXCEPTIONS(j).ERROR_CODE;
                        WTranid   := WA.TRANID(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX);
                        STMTNO    := SQL%BULK_EXCEPTIONS(j).ERROR_INDEX;
                        MSG       := 'FORALL ERROR ';

                        INSERT INTO BOA_PROCESS_LOG
                        (
                          PROCESS,
                          SUB_PROCESS,
                          ENTRYDTE,
                          ROWCOUNTS,
                          MESSAGE
                        )
                        VALUES ( proc, SubProc,SYSDATE, 0, MSG||wTranid);

                        COMMIT;

                    END LOOP;


       WHEN OTHERS THEN

             MSG  := SQLERRM;

                INSERT INTO BOA_PROCESS_LOG
                (
                  PROCESS,
                  SUB_PROCESS,
                  ENTRYDTE,
                  ROWCOUNTS,
                  MESSAGE
                )
                VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS,MSG);

                COMMIT;

  END;
  /*

   */

     PROCEDURE  CLEAR_LOG_FILENAME ( P_FILENAME IN VARCHAR2)
     IS

      SQLCOUNTS  PLS_INTEGER;
      MSG        VARCHAR2(1000);
      PROC       VARCHAR2(1000);
      SubProc    VARCHAR2(1000);

      sql_stmt   VARCHAR2(32000);
      whoami     varchar2(100);

    BEGIN


      SQLCOUNTS  := 0;
      MSG        := 'Job Starting';
      PROC       := 'BOA_FEES_VALIDATE';
      SubProc    := 'CLEAR_LOG_FILENAME';

      INSERT INTO BOA_PROCESS_LOG
      (
        PROCESS,
        SUB_PROCESS,
        ENTRYDTE,
        ROWCOUNTS,
        MESSAGE
      )
      VALUES ( proc,SubProc,SYSDATE, sqlcounts, MSG);

      COMMIT;


      DELETE FROM  BOFA_FILES_PROCESSED  WHERE UPPER(FILE_NAME)  =  UPPER(P_FILENAME);

      sqlCounts := SQL%ROWCOUNT;

      COMMIT;
      MSG  := P_FILENAME||' was removed from load table';

    INSERT INTO BOA_PROCESS_LOG
    (
      PROCESS,
      SUB_PROCESS,
      ENTRYDTE,
      ROWCOUNTS,
      MESSAGE
    )
    VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG);

    COMMIT;


    /************
      THEE END
     ***********/

     MSG       := 'Job Complete';

    sqlCounts := 0;

    INSERT INTO BOA_PROCESS_LOG
    (
      PROCESS,
      SUB_PROCESS,
      ENTRYDTE,
      ROWCOUNTS,
      MESSAGE
    )
    VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG);

    COMMIT;


    /******************************
       Record any errors
     *****************************/

    EXCEPTION
       WHEN OTHERS THEN

             MSG  := SQLERRM;

                INSERT INTO BOA_PROCESS_LOG
                (
                  PROCESS,
                  SUB_PROCESS,
                  ENTRYDTE,
                  ROWCOUNTS,
                  MESSAGE
                )
                VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS,MSG);

                COMMIT;

    END;

    /*********************
      PROCESS_LOAD_QUEUE
     ********************/

      PROCEDURE PROCESS_LOAD_QUEUE
      IS

        CURSOR C1
        IS
        SELECT A.PID
        FROM BOA_FEESRETURN_TOSEND A
        LEFT JOIN ( SELECT PID FROM SGP_FEESRETURN_PROCESSED_PIDS ) B ON ( B.PID = A.PID )
        WHERE B.PID IS NULL
        GROUP BY A.PID
        ORDER BY A.PID;

        R1   C1%ROWTYPE;


        CURSOR C2
        IS
        SELECT A.PID
        FROM SGP_FEES_TOSEND A
        LEFT JOIN ( SELECT PID FROM SGP_FEES_PROCESSED_PIDS ) B ON ( B.PID = A.PID )
        WHERE B.PID IS NULL
        GROUP BY A.PID
        ORDER BY A.PID;

        R2   C2%ROWTYPE;


        CURSOR C3
        IS
        SELECT A.PID
        FROM SGP_PPO_TOSEND A
        LEFT JOIN ( SELECT PID FROM SGP_PPO_PROCESSED_PIDS ) B ON ( B.PID = A.PID )
        WHERE B.PID IS NULL
        GROUP BY A.PID
        ORDER BY A.PID;

        R3   C3%ROWTYPE;


        CURSOR C4
        IS
        SELECT A.PID
        FROM NAV_ARINV_TOSEND A
        LEFT JOIN ( SELECT PID FROM NAV_ARINV_PROCESSED_PIDS ) B ON ( B.PID = A.PID )
        WHERE B.PID IS NULL
        GROUP BY A.PID
        ORDER BY A.PID;

        R4   C4%ROWTYPE;


        CURSOR C6
        IS
        SELECT A.PID
        FROM SVC_REL_TOSEND a
        LEFT JOIN ( SELECT PID FROM SVC_REL_PROCESSED_PIDS ) B ON ( B.PID = A.PID )
        WHERE B.PID IS NULL
        GROUP BY A.PID
        ORDER BY A.PID;

        R6   C6%ROWTYPE;

        CURSOR C7
        IS
        SELECT A.PID
        FROM XCL_TOSEND a
        LEFT JOIN ( SELECT PID FROM XCL_PROCESSED_PIDS ) B ON ( B.PID = A.PID )
        WHERE B.PID IS NULL
        GROUP BY A.PID
        ORDER BY A.PID;

        R7   C7%ROWTYPE;


        CURSOR C8
        IS
        SELECT A.PID
        FROM FILE_ERRS_TOSEND a
        LEFT JOIN ( SELECT PID FROM FILE_ERRS_PROCESSED_PIDS ) B ON ( B.PID = A.PID )
        WHERE B.PID IS NULL
        GROUP BY A.PID
        ORDER BY A.PID;

        R8   C8%ROWTYPE;

       CURSOR C9 ( PPID NUMBER)
       IS
       SELECT MESSAGE
       FROM FILE_ERRORS
       WHERE PID = PPID;

       R9   c9%ROWTYPE;

        CURSOR C10
        IS
        SELECT A.PID
        FROM SGP_ResPend_TO_SEND a
        LEFT JOIN ( SELECT PID FROM SGP_ResPend_SENT_PIDS ) B ON ( B.PID = A.PID )
        WHERE B.PID IS NULL
        GROUP BY A.PID
        ORDER BY A.PID;

        R10   C10%ROWTYPE;


        CURSOR C11
        IS
        SELECT A.PID
        FROM LA_PEND_REJS_TO_SEND a
        LEFT JOIN ( SELECT PID FROM LA_PEND_REJS_SENDT ) B ON ( B.PID = A.PID )
        WHERE B.PID IS NULL
        GROUP BY A.PID
        ORDER BY A.PID;

        R11   C11%ROWTYPE;

        CURSOR C12
        IS
        SELECT A.PID
        FROM LA_PEND_APPROV_TO_SEND a
        LEFT JOIN ( SELECT PID FROM LA_PEND_APPROV_SENDT ) B ON ( B.PID = A.PID )
        WHERE B.PID IS NULL
        GROUP BY A.PID
        ORDER BY A.PID;

        R12   C12%ROWTYPE;

        CURSOR C13
        IS
        SELECT A.PID
        FROM LA_OUT_ADJUST_TO_SEND a
        LEFT JOIN ( SELECT PID FROM LA_OUT_ADJUST_SENDT ) B ON ( B.PID = A.PID )
        WHERE B.PID IS NULL
        GROUP BY A.PID
        ORDER BY A.PID;

        R13   C13%ROWTYPE;

        CURSOR C14
        IS
        SELECT A.PID
        FROM LA_IR_ASSIGN_TO_SEND a
        LEFT JOIN ( SELECT PID FROM LA_IR_ASSIGN_SENDT ) B ON ( B.PID = A.PID )
        WHERE B.PID IS NULL
        GROUP BY A.PID
        ORDER BY A.PID;

        R14   C14%ROWTYPE;


/***************************/

      TYPE  FileList  is RECORD
      (
         CLIENT    DBMS_SQL.VARCHAR2_TABLE,
         FILE_NAME DBMS_SQL.VARCHAR2_TABLE
      );

      FL   Filelist;

      Q    BOA_BILLING_LOAD_QUEUE_ARY;

--      f UTL_FILE.FILE_TYPE := UTL_FILE.FOPEN('BOA_COMPLETED_DIR', 'BOA_COMPLETED_FILES.txt', 'w');
      f UTL_FILE.FILE_TYPE := UTL_FILE.FOPEN('BOA_COMPLETED_DIR', 'BOA_LOADED_FILES.txt', 'w');
--      f UTL_FILE.FILE_TYPE := UTL_FILE.FOPEN('BOA_COMPLETED_DIR', 'BOA_REMOVE_FILES.txt', 'w');


      SQLCOUNTS  PLS_INTEGER;
      CNT        PLS_INTEGER;

      MSG           VARCHAR2(1000);
      PROC          VARCHAR2(1000);
      SubProc       VARCHAR2(1000);
      FileType      VARCHAR2(100);
      lineout       VARCHAR2(1000);

      F_MESSAGE      VARCHAR2(32000);
      S_MESSAGE      VARCHAR2(2000);
      P_OUT          NUMBER;
      P_MESSAGE      VARCHAR2(300);
      rCode          number;
      vCompleted     NUMBER;
      vLoadComplete  VARCHAR2(5);
      P_ISSUE        VARCHAR2(100);
      SQL_STMT       VARCHAR2(32000);
--   CHL CANCEL VARIABLES
      file_name_in   VARCHAR2(200);
      file_name_out  VARCHAR2(200);

      GC             GenRefCursor;
      bad_data       exception;

      gc1            GenRefCursor;

    BEGIN


      SQLCOUNTS  := 0;
      MSG        := 'Job Starting';
      PROC       := 'BOA_FEES_VALIDATE';
      SubProc    := 'PROCESS_LOAD_QUEUE';
      vCompleted    := 0;
      vLoadComplete := 'NO';
      vTEAM      := 'RDM';


      INSERT INTO BOA_PROCESS_LOG
      (
        PROCESS,
        SUB_PROCESS,
        ENTRYDTE,
        ROWCOUNTS,
        MESSAGE
      )
      VALUES ( proc,SubProc,SYSDATE, sqlcounts, MSG);

      COMMIT;

/***************************************/
 ---  REMOVE DUPLICES FROM Load queue
/***************************************/
      DEFINE_DUPLICATES;


      OPEN GC FOR SELECT  LTRIM(RTRIM( CLIENT)) AS CLIENT, LTRIM(RTRIM(FILE_NAME)) AS FILE_NAME,  COMPLETED, FILE_TYPE, ROWID  FROM BOA_BILLING_LOAD_QUEUE WHERE COMPLETED = 0;

      FETCH GC BULK COLLECT INTO Q.CLIENT, Q.FILE_NAME, Q.COMPLETED, Q.FILE_TYPE, Q.rowids;

      CLOSE GC;

         vTEAM := 'RDM';

          for k in 1..q.client.count loop

---              BOA_FEES_VALIDATE.LOG_FILENAME( Q.CLIENT(k), Q.FILE_NAME(k), P_OUT, P_MESSAGE );
----            swing through and flag any filename that was loaded before.

             sql_stmt  := 'SELECT CLIENT, FILE_NAME  from  BOFA_FILES_PROCESSED  WHERE CLIENT = :1  and FILE_NAME = :2 ';

             OPEN GC1 FOR SQL_STMT   USING   Q.CLIENT(k), Q.FILE_NAME(k);
                  FETCH GC1 BULK COLLECT INTO FL.client, fl.file_name;
             close gc1;



              IF ( fl.client.count > 0 )
                  THEN
                  MSG  := 'Issue was found with Filename '||Q.FILE_NAME(k)||' It was loaded before';
                  Q.COMPLETED(k) := 2;

                  SEND_EMAIL  ( vTEAM, PROC||':'||SubProc, 'Loading Errors',  MSG);

                  INSERT INTO BOA_PROCESS_LOG
                  (
                    PROCESS,
                    SUB_PROCESS,
                    ENTRYDTE,
                    ROWCOUNTS,
                    MESSAGE
                  )
                  VALUES ( proc, SubProc,SYSDATE, 0, MSG);

                  COMMIT;

              END IF;

          end loop;


               forall i in 1..q.client.count
               EXECUTE IMMEDIATE 'UPDATE BOA_BILLING_LOAD_QUEUE  SET COMPLETED = :A  where rowid = :D ' USING  q.completed(i), q.Rowids(i);
               commit;


----- read the table again  where completed is zero 20131129_Safeguard_Transmit.xlsx.csv

            OPEN GC FOR SELECT  LTRIM(RTRIM( CLIENT)) AS CLIENT, LTRIM(RTRIM(FILE_NAME)) AS FILE_NAME,  COMPLETED, FILE_TYPE, ROWID  FROM BOA_BILLING_LOAD_QUEUE WHERE COMPLETED = 0;

            FETCH GC BULK COLLECT INTO Q.CLIENT, Q.FILE_NAME, Q.COMPLETED, Q.FILE_TYPE, Q.rowids;

            CLOSE GC;

          for j in 1..q.client.count loop

              BOA_FEES_VALIDATE.LOG_FILENAME( Q.CLIENT(j), Q.FILE_NAME(j), P_OUT, P_MESSAGE );


                IF (P_OUT > 0 )
                  THEN
                       IF ( Q.CLIENT(j)  IN ('FILE_ERRORS') )
                            THEN
                               Q.FILE_TYPE(j)  := 'File load Errors';
                               BOA_FEES_VALIDATE.LOAD_FILE_ERRORS(Q.FILE_NAME(j), P_OUT,P_MESSAGE, rCode);

                       ELSIF (Q.CLIENT(j) in ('Loss_Analyst') )
                            THEN
                               LOSS_ANALYSIS_FILES(Q.FILE_NAME(j), P_OUT, P_MESSAGE, rCODE , P_ISSUE);
                               Q.FILE_TYPE(j) := P_ISSUE;

                       ELSIF ( Q.FILE_NAME(j) LIKE 'SGP_FEES_%')
                            THEN
                                Q.FILE_TYPE(j) := 'FEES';
                                BOA_FEES_VALIDATE.LOAD_SGP_FEES(Q.FILE_NAME(j), P_OUT, P_MESSAGE, rCode);

                       ELSIF ( Q.FILE_NAME(j) LIKE 'BOA_FEESRETURN_%')
                            THEN
                                Q.FILE_TYPE(j)  := 'FEES Returned';
                                BOA_FEES_VALIDATE.LOAD_BOA_FEESRETURN(Q.FILE_NAME(j), P_OUT,P_MESSAGE,rCode);

                       ELSIF ( Q.FILE_NAME(j) LIKE 'SGP_PPOINVOICING_%')
                            THEN
                               Q.FILE_TYPE(j)  := 'PPO Invoicing';
                               BOA_FEES_VALIDATE.LOAD_SGP_PPOINVOICING(Q.FILE_NAME(j), P_OUT,P_MESSAGE, rCode);

                       ELSIF ( Q.FILE_NAME(j) LIKE 'ARInv%')
                            THEN
                               Q.FILE_TYPE(j)  := 'AR Invoicing';
                               BOA_FEES_VALIDATE.LOAD_NAV_ARINVOICING(Q.FILE_NAME(j), P_OUT,P_MESSAGE, rCode);

                       ELSIF ( Q.FILE_NAME(j)  LIKE '%_Safeguard_%')
                            THEN
                               Q.FILE_TYPE(j)  := 'Svc Rel';
                               BOA_FEES_VALIDATE.LOAD_SVC_Release(Q.FILE_NAME(j), P_OUT,P_MESSAGE, rCode);

                       ELSIF ( Q.FILE_NAME(j)  LIKE '%DuplicateOrderCheckSGP_%' AND Q.CLIENT(j) IN ('CHL')  )
                            THEN
                               BOA_FEES_VALIDATE.LOAD_XCL_CHL(Q.FILE_NAME(j), P_OUT,P_MESSAGE, rCode, P_ISSUE);
                               Q.FILE_TYPE(j)  := P_ISSUE;
                       END IF;

                      INSERT INTO FILES_LOADED_TOSEND
                      VALUES (P_OUT);
                      COMMIT;
                ELSE
                        MSG := 'We had a issue with the LOG FILE NAME procedure ';
                        raise bad_data;
                END IF;

                if  (p_message in ('UNKNOWN-WORKING-DIRECTORY') )
                then
                     MSG  := 'Issue was found RETURN MESSAGE '||Q.FILE_NAME(j)||' abort';
                    RAISE BAD_DATA;
                end if;

               if (rCode !=  0 )
                  then
                     MSG  := 'Issue was found while loading '||Q.FILE_NAME(j)||' abort';
                    RAISE BAD_DATA;
               end if;



          end loop;



      /* POPULATE ENHANCED VERSIONS */

      begin

             BOA_FEES_VALIDATE.LOAD_SGP_FEES_ENHANCED;
             BOA_FEES_VALIDATE.LOAD_SGP_PPO_ENHANCED;

      end;


       BEGIN

              SubProc  := 'INSERT INTO SGP_FEES RETURN ';
              MSG      := 'STARTING';
              SQLCOUNTS := 0;
              CNT       := 0;

              INSERT INTO BOA_PROCESS_LOG
              (
                PROCESS,
                SUB_PROCESS,
                ENTRYDTE,
                ROWCOUNTS,
                MESSAGE
              )
              VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG);

              COMMIT;

                open c1;
                     loop
                         fetch c1 into r1;
                         exit when c1%notfound;
                --         EXIT WHEN CNT > 10;

                          CNT := CNT + 1;

                          SQL_STMT := ' INSERT INTO  IPP$LIBRARIAN.BOA_FEESRETURN_EXT@'||vDB_LINK||' (TRANSACTION_TYPE, TRANID, TRANKY, ERROR_CODE, ERROR_MESSAGE, RESOLVED, RESEARCHING, RESENT, DEV, BILLED, PID ) ';
                          SQL_STMT := SQL_STMT||' select  A.TRANSACTION_TYPE, A.TRANID, A.TRANKY, A.ERROR_CODE, A.ERROR_MESSAGE, A.RESOLVED, A.RESEARCHING, A.RESENT, A.DEV, A.BILLED, A.PID ';
                          SQL_STMT := SQL_STMT||'    from BOA_FEESRETURN A ';
                          SQL_STMT := SQL_STMT||'     WHERE A.PID = :2 ';

                          EXECUTE IMMEDIATE SQL_STMT USING R1.PID;

                          SQLCOUNTS := SQL%ROWCOUNT;
                          COMMIT;

                          MSG := 'Processing Return Fees.. pid '||r1.pid;


                          INSERT INTO BOA_PROCESS_LOG
                          (
                            PROCESS,
                            SUB_PROCESS,
                            ENTRYDTE,
                            ROWCOUNTS,
                            MESSAGE
                          )
                          VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG);

                          COMMIT;

                           SQL_STMT := ' SELECT COUNT(*) FROM IPP$LIBRARIAN.BOA_FEESRETURN_EXT@'||vDB_LINK||' WHERE PID = :A ';
                           EXECUTE IMMEDIATE SQL_STMT INTO SQLCOUNTS USING R1.PID;


                          INSERT INTO BOA_PROCESS_LOG
                          (
                            PROCESS,
                            SUB_PROCESS,
                            ENTRYDTE,
                            ROWCOUNTS,
                            MESSAGE
                          )
                          VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, 'CHECK BOA_FEESRETURN_EXT on PROD');

                          COMMIT;


                           INSERT INTO SGP_FEESRETURN_PROCESSED_PIDS
                           VALUES (R1.PID, SQLCOUNTS);
                           COMMIT;

                     end loop;
                close c1;


                    SubProc   := 'INSERT INTO SGP_FEES ENHANCED ';
                    MSG       := 'STARTING';
                    SQLCOUNTS := 0;
                    CNT       := 0;

                    INSERT INTO BOA_PROCESS_LOG
                    (
                      PROCESS,
                      SUB_PROCESS,
                      ENTRYDTE,
                      ROWCOUNTS,
                      MESSAGE
                    )
                    VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG);

                    COMMIT;


                open c2;
                     loop
                         fetch c2 into r2;
                         exit when c2%notfound;
                --         EXIT WHEN CNT > 0;

                          CNT := CNT + 1;


                          SQL_STMT := ' INSERT INTO  IPP$LIBRARIAN.SGP_FEES_ENHANCED@'||vDB_LINK||' (TRANID, WORK_ORDER, DONE_CD, ORDER_DT, LOAN, SENTDATE, TRANCODE, PRICE, FIELD6, FIELD7, FIELD8, FIELD9, FIELD10, FIELD11, WT, SWT, PRICEPER, QTY, UOM, SERVICEID, ZIP, PID)';
                          SQL_STMT := SQL_STMT||' select  A.TRANID, A.WORK_ORDER, A.DONE_CD, A.ORDER_DT, A.LOAN, A.SENTDATE, A.TRANCODE, A.PRICE, A.FIELD6, A.FIELD7, A.FIELD8, A.FIELD9, A.FIELD10, A.FIELD11, A.WT, A.SWT, A.PRICEPER, A.QTY, A.UOM, A.SERVICEID, A.ZIP, A.PID';
                          SQL_STMT := SQL_STMT||' from SGP_FEES_ENHANCED A ';
                          SQL_STMT := SQL_STMT||' WHERE A.PID = :2 ';
                          EXECUTE IMMEDIATE SQL_STMT USING R2.PID;

                          SQLCOUNTS := SQL%ROWCOUNT;

                          COMMIT;

                          MSG := 'Processing SGP_FEES ENHANCED.. apexp1 pid '||r2.pid;


                           SQL_STMT := ' SELECT COUNT(*) FROM IPP$LIBRARIAN.SGP_FEES_ENHANCED@'||vDB_LINK||' WHERE PID = :A ';
                           EXECUTE IMMEDIATE SQL_STMT INTO SQLCOUNTS USING R2.PID;



                          INSERT INTO BOA_PROCESS_LOG
                          (
                            PROCESS,
                            SUB_PROCESS,
                            ENTRYDTE,
                            ROWCOUNTS,
                            MESSAGE
                          )
                          VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, 'CHECK SGP_FEES_ENHANCED on PROD');

                          COMMIT;

                           INSERT INTO SGP_FEES_PROCESSED_PIDS
                           VALUES (R2.PID,SQLCOUNTS);
                           COMMIT;

                            INSERT INTO BOA_PROCESS_LOG
                            (
                              PROCESS,
                              SUB_PROCESS,
                              ENTRYDTE,
                              ROWCOUNTS,
                              MESSAGE
                            )
                            VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG);

                          COMMIT;

                     end loop;
                close c2;



                   SubProc   := 'INSERT INTO SGP_PPO ENHANCED ';
                    MSG       := 'STARTING';
                    SQLCOUNTS := 0;
                    CNT       := 0;

                    INSERT INTO BOA_PROCESS_LOG
                    (
                      PROCESS,
                      SUB_PROCESS,
                      ENTRYDTE,
                      ROWCOUNTS,
                      MESSAGE
                    )
                    VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG);

                    COMMIT;

                open c3;
                     loop
                         fetch c3 into r3;
                         exit when c3%notfound;
                ---         EXIT WHEN CNT > 0;

                          CNT := CNT + 1;

                          SQL_STMT := ' INSERT INTO  IPP$LIBRARIAN.SGP_PPOINVOICING_ENHANCED@'||vDB_LINK||' (TRANID, WORK_ORDER, DONE_CD, ORDER_DT, LOAN, SENTDATE, TRANCODE, PRICE, FIELD6, FIELD7, FIELD8, FIELD9, FIELD10, FIELD11, WT, SWT, PRICEPER, QTY, UOM, SERVICEID, ZIP, PID)';
                          SQL_STMT := SQL_STMT||' select  A.TRANID, A.WORK_ORDER, A.DONE_CD, A.ORDER_DT, A.LOAN, A.SENTDATE, A.TRANCODE, A.PRICE, A.FIELD6, A.FIELD7, A.FIELD8, A.FIELD9, A.FIELD10, A.FIELD11, A.WT, A.SWT, A.PRICEPER, A.QTY, A.UOM, A.SERVICEID, A.ZIP, A.PID';
                          SQL_STMT := SQL_STMT||' from SGP_PPOINVOICING_ENHANCED A';
                          SQL_STMT := SQL_STMT||'  WHERE A.PID = :4 ';

                          EXECUTE IMMEDIATE SQL_STMT USING R3.PID;

                          SQLCOUNTS := SQL%ROWCOUNT;

                          COMMIT;

                          MSG := 'Processing PPOINVOICING... apexd1 pid '||r3.pid;

                            INSERT INTO BOA_PROCESS_LOG
                            (
                              PROCESS,
                              SUB_PROCESS,
                              ENTRYDTE,
                              ROWCOUNTS,
                              MESSAGE
                            )
                            VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG);

                          COMMIT;

                           SQL_STMT := ' SELECT COUNT(*) FROM IPP$LIBRARIAN.SGP_PPOINVOICING_ENHANCED@'||vDB_LINK||' WHERE PID = :A ';
                           EXECUTE IMMEDIATE SQL_STMT INTO SQLCOUNTS USING R3.PID;


                          INSERT INTO BOA_PROCESS_LOG
                          (
                            PROCESS,
                            SUB_PROCESS,
                            ENTRYDTE,
                            ROWCOUNTS,
                            MESSAGE
                          )
                          VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, 'CHECK SGP_PPOINVOICING_ENHANCED on PROD');

                          COMMIT;


                           INSERT INTO SGP_PPO_PROCESSED_PIDS
                           VALUES (R3.PID, SQLCOUNTS);
                           COMMIT;

                     end loop;
                close c3;

                open c4;
                     loop
                         fetch c4 into r4;
                         exit when c4%notfound;
                ---         EXIT WHEN CNT > 0;

                          CNT := CNT + 1;

                         SubProc   := 'INSERT INTO BOA NAV ARINVOICING ';

                          SQL_STMT := ' INSERT INTO  IPP$LIBRARIAN.BOA_NAV_ARINVOICING@'||vDB_LINK||' (RECORD_TYPE,INVOICE_DATE,INVOICE_NBR,ORDER_NBR,BILL_CODE,CLIENT_CODE,BILL_TO_CODE,DEPT,X1,PROP_NBR,LOAN_NBR,VENDOR_CODE,WORK_CODE,X2,X3,CONS,SALES,COSTS,X4,X5,PHOTOS,X6,X7,ORDER_DATE,STATE,ZIP,CONTROL_NBR,ORDER_BY_NAME,COMPLETION_DATE,OCCUPANCY,LOAN_TYPE,ZIP_CODE2,TAX_AMOUNT,EXEMPT_AMOUNT,EXEMPT_REASON,TAX_PERCENT,PID)';
                          SQL_STMT := SQL_STMT||' select  A.RECORD_TYPE,A.INVOICE_DATE,A.INVOICE_NBR,A.ORDER_NBR,A.BILL_CODE,A.CLIENT_CODE,A.BILL_TO_CODE,A.DEPT,A.X1,A.PROP_NBR,A.LOAN_NBR,A.VENDOR_CODE,A.WORK_CODE,A.X2,A.X3,A.CONS,A.SALES,A.COSTS,A.X4,A.X5,A.PHOTOS,A.X6,A.X7,A.ORDER_DATE,A.STATE,A.ZIP,A.CONTROL_NBR,A.ORDER_BY_NAME,A.COMPLETION_DATE,A.OCCUPANCY,A.LOAN_TYPE,A.ZIP_CODE2,A.TAX_AMOUNT,A.EXEMPT_AMOUNT,A.EXEMPT_REASON,A.TAX_PERCENT,A.PID';
                          SQL_STMT := SQL_STMT||' from BOA_NAV_ARINVOICING A';
                          SQL_STMT := SQL_STMT||'  WHERE A.PID = :4 ';

                          EXECUTE IMMEDIATE SQL_STMT USING R4.PID;

                          SQLCOUNTS := SQL%ROWCOUNT;

                          COMMIT;

                          MSG := 'Processing AR INVOICING... apexp1 pid '||r4.pid;

                            INSERT INTO BOA_PROCESS_LOG
                            (
                              PROCESS,
                              SUB_PROCESS,
                              ENTRYDTE,
                              ROWCOUNTS,
                              MESSAGE
                            )
                            VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG);

                          COMMIT;


                           SQL_STMT := ' SELECT COUNT(*) FROM IPP$LIBRARIAN.BOA_NAV_ARINVOICING@'||vDB_LINK||' WHERE PID = :A ';
                           EXECUTE IMMEDIATE SQL_STMT INTO SQLCOUNTS USING R4.PID;



                          INSERT INTO BOA_PROCESS_LOG
                          (
                            PROCESS,
                            SUB_PROCESS,
                            ENTRYDTE,
                            ROWCOUNTS,
                            MESSAGE
                          )
                          VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, 'CHECK BOA_NAV_ARINVOICING on PROD');

                          COMMIT;


                           INSERT INTO NAV_ARINV_PROCESSED_PIDS
                           VALUES (R4.PID, SQLCOUNTS);
                           COMMIT;

                     end loop;
                close c4;

                open C6;
                     loop
                         fetch C6 into R6;
                         exit when C6%notfound;
                ---         EXIT WHEN CNT > 0;  INSERT INTO BOA_SVC_REL(ACCOUNTID, DEREGISTER, NEWSERVICER, PID, SRLDATE, SRLID)

                          CNT := CNT + 1;

                         SubProc   := 'INSERT INTO BOA SERVICE RELEASE ';

                          SQL_STMT := ' INSERT INTO  IPP$LIBRARIAN.BOA_SVC_REL@'||vDB_LINK||' (ACCOUNTID, DEREGISTER, NEWSERVICER, PID, SRLDATE, SRLID)';
                          SQL_STMT := SQL_STMT||' select  A.ACCOUNTID, A.DEREGISTER, A.NEWSERVICER, A.PID, A.SRLDATE, A.SRLID';
                          SQL_STMT := SQL_STMT||' from BOA_SVC_REL A';
                          SQL_STMT := SQL_STMT||'  WHERE A.PID = :6 ';

                          EXECUTE IMMEDIATE SQL_STMT USING R6.PID;

                          SQLCOUNTS := SQL%ROWCOUNT;

                          COMMIT;

                          MSG := 'Processing BOA SERVICE RELEASE... apexp1 pid '||R6.pid;

                            INSERT INTO BOA_PROCESS_LOG
                            (
                              PROCESS,
                              SUB_PROCESS,
                              ENTRYDTE,
                              ROWCOUNTS,
                              MESSAGE
                            )
                            VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG);

                          COMMIT;


                           SQL_STMT := ' SELECT COUNT(*) FROM IPP$LIBRARIAN.BOA_SVC_REL@'||vDB_LINK||' WHERE PID = :A ';
                           EXECUTE IMMEDIATE SQL_STMT INTO SQLCOUNTS USING R6.PID;



                          INSERT INTO BOA_PROCESS_LOG
                          (
                            PROCESS,
                            SUB_PROCESS,
                            ENTRYDTE,
                            ROWCOUNTS,
                            MESSAGE
                          )
                          VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, 'CHECK BOA_SVC_REL on PROD');

                          COMMIT;


                           INSERT INTO SVC_REL_PROCESSED_PIDS
                           VALUES (R6.PID, SQLCOUNTS);
                           COMMIT;

                     end loop;
                close C6;


                OPEN C7;
                  LOOP
                  FETCH C7 INTO R7;
                  EXIT WHEN C7%NOTFOUND;

                         SubProc   := 'INSERT INTO RDM.XCL_SWEEP ';

                          SQL_STMT := ' INSERT INTO  RDM.XCL_SWEEP@'||vDB_LINK||' (CLIENT_NAME, SHEETNAME, ACCT, CURRENT_, CURRENTDTL, RECACTION, LOAD_DT, PID)';
                          SQL_STMT := SQL_STMT||' select A.CLIENT_NAME, A.SHEETNAME, A.ACCT, A.CURRENT_, A.CURRENTDTL, A.RECACTION, A.LOAD_DT, A.PID ';
                          SQL_STMT := SQL_STMT||' from  ( SELECT CLIENT_NAME, SHEETNAME, ACCT, CURRENT_, CURRENTDTL, RECACTION, LOAD_DT, PID, RANK() OVER ( PARTITION BY SHEETNAME, ACCT, CURRENT_, CURRENTDTL, RECACTION ORDER BY PID DESC, ROWNUM ) RK  FROM XCL_SWEEP ) A';
                          SQL_STMT := SQL_STMT||'  WHERE A.RK = 1  AND A.PID = :7 ';

                          EXECUTE IMMEDIATE SQL_STMT USING R7.PID;

                          SQLCOUNTS := SQL%ROWCOUNT;

                          COMMIT;

                          MSG := 'Processing SPI XCL CHL... '||vDB_LINK||' pid '||R7.pid;

                            INSERT INTO BOA_PROCESS_LOG
                            (
                              PROCESS,
                              SUB_PROCESS,
                              ENTRYDTE,
                              ROWCOUNTS,
                              MESSAGE
                            )
                            VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG);

                          COMMIT;

                          INSERT INTO XCL_PROCESSED_PIDS
                          VALUES (R7.PID, SQLCOUNTS);
                          COMMIT;

                 END LOOP;
               CLOSE C7;


                OPEN C8;
                  LOOP
                  FETCH C8 INTO R8;
                  EXIT WHEN C8%NOTFOUND;
                  cnt  := 1;
                  OPEN C9(R8.PID);
                     F_MESSAGE := 'ERRORS:';

                     LOOP
                       FETCH C9 INTO R9;
                       EXIT WHEN C9%NOTFOUND;
                       exit when cnt > 99;
                       F_MESSAGE := F_MESSAGE||' '||R9.MESSAGE;
                       cnt := cnt + 1;
                     END LOOP;
                  CLOSE C9;

                         S_MESSAGE := SUBSTR(F_MESSAGE,1,999);

--                         INSERT INTO SHOW_STMT
--                         VALUES (S_MESSAGE);
--                         COMMIT;

                         SubProc   := 'INSERT INTO FILE_ERRORS ';

                          SQL_STMT := ' INSERT INTO  RDM.FILE_ERRORS@'||vDB_LINK||'  (PID, PROCESS,  MESSAGE, LOAD_DT)';
                          SQL_STMT := SQL_STMT||' select A.PID, A.PROCESS, '''||S_MESSAGE||''', A.LOAD_DT ';
                          SQL_STMT := SQL_STMT||' from FILE_ERRORS A';
                          SQL_STMT := SQL_STMT||'  WHERE A.PID = :8 ';
                          SQL_STMT := SQL_STMT||'  AND ROWNUM = 1 ';

                          EXECUTE IMMEDIATE SQL_STMT USING R8.PID;

                          SQLCOUNTS := SQL%ROWCOUNT;

                          COMMIT;

                          MSG := 'Processing FILE_ERRORS... '||vDB_LINK||' pid '||R8.pid;

                            INSERT INTO BOA_PROCESS_LOG
                            (
                              PROCESS,
                              SUB_PROCESS,
                              ENTRYDTE,
                              ROWCOUNTS,
                              MESSAGE
                            )
                            VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG);

                          COMMIT;

                          INSERT INTO FILE_ERRS_PROCESSED_PIDS
                          VALUES (R8.PID, SQLCOUNTS);
                          COMMIT;

                 END LOOP;
               CLOSE C8;


                OPEN C10;
                  LOOP
                  FETCH C10 INTO R10;
                  EXIT WHEN C10%NOTFOUND;

                         SubProc   := ' INSERT INTO RDM.LA_RES_PENDING ';

                          SQL_STMT := ' INSERT INTO  RDM.LA_RESOLUTION_PENDING_STG@'||vDB_LINK||'  (INVOICE_NBR,ORDER_NBR,VENDOR_CONTACT,LOAN_NBR,INVOICE_DATE,DEPT,STATE,RESOLUTION_TYPE,INVOICE_AMT,VENDOR_COMMENT,VENDOR_DATE,CLIENT_COMMENT';
                          SQL_STMT := SQL_STMT||' ,CLIENT_DATE,REASON,RESOLUTION_DEADLINE,CURTAIL_DATE,PID, LOAD_ID)';
                          SQL_STMT := SQL_STMT||' SELECT INVOICE_NBR, ORDER_NBR, VENDOR_CONTACT, LOAN_NBR, INVOICE_DATE, DEPT, STATE, RESOLUTION_TYPE, INVOICE_AMT, VENDOR_COMMENT, VENDOR_DATE, CLIENT_COMMENT';
                          SQL_STMT := SQL_STMT||' ,CLIENT_DATE, REASON, RESOLUTION_DEADLINE, CURTAIL_DATE, PID, LOAD_ID';
                          SQL_STMT := SQL_STMT||' FROM LA_RESOLUTION_PENDING ';
                          SQL_STMT := SQL_STMT||' WHERE PID = :A ';
                          SQL_STMT := SQL_STMT||' ORDER BY LOAD_ID ';
                          EXECUTE IMMEDIATE SQL_STMT USING R10.PID;

                          SQLCOUNTS := SQL%ROWCOUNT;

                          COMMIT;

                          MSG := 'Processing LA_RESOLUTION_PENDING... '||vDB_LINK||' pid '||R10.pid;

                            INSERT INTO BOA_PROCESS_LOG
                            (
                              PROCESS,
                              SUB_PROCESS,
                              ENTRYDTE,
                              ROWCOUNTS,
                              MESSAGE
                            )
                            VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG);

                          COMMIT;

                          INSERT INTO SGP_ResPend_SENT_PIDS
                          VALUES (R10.PID, SQLCOUNTS);
                          COMMIT;

                 END LOOP;
               CLOSE C10;

                OPEN C11;
                  LOOP
                  FETCH C11 INTO R11;
                  EXIT WHEN C11%NOTFOUND;

                         SubProc   := ' INSERT INTO RDM.LA_PENDING_REJECTIONS_STG ';

                          SQL_STMT := ' INSERT INTO RDM.LA_PENDING_REJECTIONS_STG@'||vDB_LINK||' (CLIENT,SERVICER,LOAN_NBR,INVOICE_TYPE,INVOICE_DATE,INVOICE_NBR,ORDER_NBR,STATE,SERVICER_COMMENTS,COMMENTS_DATE,EXPIRES_IN,INVOICE_AMT,LOAD_ID,FILE_ID)';
                          SQL_STMT := SQL_STMT||' SELECT CLIENT,SERVICER,LOAN_NBR,INVOICE_TYPE,INVOICE_DATE,INVOICE_NBR,ORDER_NBR,STATE,SERVICER_COMMENTS,COMMENTS_DATE,EXPIRES_IN,INVOICE_AMT,LOAD_ID,FILE_ID';
                          SQL_STMT := SQL_STMT||' FROM LA_PENDING_REJECTIONS ';
                          SQL_STMT := SQL_STMT||' WHERE FILE_ID = :A ';
                          SQL_STMT := SQL_STMT||' ORDER BY LOAD_ID ';
                          EXECUTE IMMEDIATE SQL_STMT USING R11.PID;

                          SQLCOUNTS := SQL%ROWCOUNT;

                          COMMIT;

                          MSG := 'Processing LA_PENDING_REJECTIONS... '||vDB_LINK||' pid '||R11.pid;

                            INSERT INTO BOA_PROCESS_LOG
                            (
                              PROCESS,
                              SUB_PROCESS,
                              ENTRYDTE,
                              ROWCOUNTS,
                              MESSAGE
                            )
                            VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG);

                          COMMIT;

                          INSERT INTO LA_PEND_REJS_SENDT
                          VALUES (R11.PID, SQLCOUNTS);
                          COMMIT;

                 END LOOP;
               CLOSE C11;

                OPEN C12;
                  LOOP
                  FETCH C12 INTO R12;
                  EXIT WHEN C12%NOTFOUND;

                         SubProc   := ' INSERT INTO RDM.LA_PENDING_APPROVAL_STG ';

                          SQL_STMT := ' INSERT INTO RDM.LA_PENDING_APPROVAL_STG@'||VDB_LINK||' (SERVICER,LOAN_NBR,INVOICE_TYPE,INVOICE_DATE,INVOICE_NBR,STATE,LI_CODE,LI_DESCRIPTION,LI_AMT,ADJUSTED_AMT,TOTAL_LI_AMOUNT,REASON,COMMENTS_DATE,EXPIRES_IN,LOAD_ID,FILE_ID)';
                          SQL_STMT := SQL_STMT||' SELECT SERVICER,LOAN_NBR,INVOICE_TYPE,INVOICE_DATE,INVOICE_NBR,STATE,LI_CODE,LI_DESCRIPTION,LI_AMT,ADJUSTED_AMT,TOTAL_LI_AMOUNT,REASON,COMMENTS_DATE,EXPIRES_IN,LOAD_ID,FILE_ID';
                          SQL_STMT := SQL_STMT||' FROM LA_PENDING_APPROVAL ';
                          SQL_STMT := SQL_STMT||' WHERE FILE_ID = :A ';
                          SQL_STMT := SQL_STMT||' ORDER BY LOAD_ID ';
                          EXECUTE IMMEDIATE SQL_STMT USING R12.PID;

                          SQLCOUNTS := SQL%ROWCOUNT;

                          COMMIT;

                          MSG := 'Processing LA_PENDING_APPROVAL... '||vDB_LINK||' pid '||R12.pid;

                            INSERT INTO BOA_PROCESS_LOG
                            (
                              PROCESS,
                              SUB_PROCESS,
                              ENTRYDTE,
                              ROWCOUNTS,
                              MESSAGE
                            )
                            VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG);

                          COMMIT;

                          INSERT INTO LA_PEND_APPROV_SENDT
                          VALUES (R12.PID, SQLCOUNTS);
                          COMMIT;

                 END LOOP;
               CLOSE C12;


                OPEN C13;
                  LOOP
                  FETCH C13 INTO R13;
                  EXIT WHEN C13%NOTFOUND;

                         SubProc   := ' INSERT INTO RDM.LA_OUTSTANDING_ADJUSTED_STG ';

                          SQL_STMT := '   INSERT INTO RDM.LA_OUTSTANDING_ADJUSTED_STG@'||vDB_LINK||' (INVOICE_NBR,VENDOR,VENDOR_CONTACT,LOAN_NBR,INVOICE_DATE,DEPT,STATE,BORROWER,ADJUSTED_TOTAL,EARLIEST_ADJ_DT,CURTAIL_DATE,PID )';
                          SQL_STMT := SQL_STMT||' SELECT INVOICE_NBR,VENDOR,VENDOR_CONTACT,LOAN_NBR,INVOICE_DATE,DEPT,STATE,BORROWER,ADJUSTED_TOTAL,EARLIEST_ADJ_DT,CURTAIL_DATE, PID';
                          SQL_STMT := SQL_STMT||' FROM LA_OUTSTANDING_ADJUSTED ';
                          SQL_STMT := SQL_STMT||' WHERE PID = :A ';
                          SQL_STMT := SQL_STMT||' ORDER BY LOAD_ID ';
                          EXECUTE IMMEDIATE SQL_STMT USING R13.PID;

                          SQLCOUNTS := SQL%ROWCOUNT;

                          COMMIT;

                          MSG := 'Processing LA_OUTSTANDING_ADJUSTED... '||vDB_LINK||' pid '||R13.pid;

                            INSERT INTO BOA_PROCESS_LOG
                            (
                              PROCESS,
                              SUB_PROCESS,
                              ENTRYDTE,
                              ROWCOUNTS,
                              MESSAGE
                            )
                            VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG);

                          COMMIT;

                          INSERT INTO LA_OUT_ADJUST_SENDT
                          VALUES (R13.PID, SQLCOUNTS);
                          COMMIT;

                 END LOOP;
                CLOSE C13;

                 OPEN C14;
                  LOOP
                  FETCH C14 INTO R14;
                  EXIT WHEN C14%NOTFOUND;

                         SubProc   := ' INSERT INTO RDM.LA_IR_ASSIGNMENT_STG ';

                          SQL_STMT := ' INSERT INTO RDM.LA_IR_ASSIGNMENT_STG@'||vDB_LINK||' (RECEIVED_DATE,LAST_UPDATED,CLIENT,COL_D,INVOICE_NBR,INVOICE_DATE,WORK_ORDER_NBR,LOAN_NBR,COL_I,DISPUTE_AMT,COL_K,CLIENT_COMMENT,COL_M,LOSS_ANALYST,WRITE_OFF_AMOUNT,WRITE_OFF,WRITE_OFF_REASON_CODE,WRITE_OFF_REASON,DONE_BILLING_CODE,';
                          SQL_STMT := SQL_STMT||' VENDOR_CODE,CHARGEBACK_AMOUNT,DISPUTE_TYPE,SOURCE_OF_DISPUTE,COL_X,APPROVAL,PENDING_RESEARCH,DISPUTE_APPEAL_COMMENT,IM_ICLEAR_INV,CLIENT_EMPLOYEE,FILE_ID,LOAD_ID)';
                          SQL_STMT := SQL_STMT||' SELECT  RECEIVED_DATE,LAST_UPDATED,CLIENT,COL_D,INVOICE_NBR,INVOICE_DATE,WORK_ORDER_NBR,LOAN_NBR,COL_I,DISPUTE_AMT,COL_K,CLIENT_COMMENT,COL_M,LOSS_ANALYST,WRITE_OFF_AMOUNT,WRITE_OFF,WRITE_OFF_REASON_CODE,WRITE_OFF_REASON,DONE_BILLING_CODE,';
                          SQL_STMT := SQL_STMT||' VENDOR_CODE,CHARGEBACK_AMOUNT,DISPUTE_TYPE,SOURCE_OF_DISPUTE,COL_X,APPROVAL,PENDING_RESEARCH,DISPUTE_APPEAL_COMMENT,IM_ICLEAR_INV,CLIENT_EMPLOYEE,FILE_ID, LOAD_ID';
                          SQL_STMT := SQL_STMT||' FROM LA_IR_ASSIGNMENT ';
                          SQL_STMT := SQL_STMT||' WHERE FILE_ID = :A ';
                          SQL_STMT := SQL_STMT||' ORDER BY LOAD_ID ';
                          EXECUTE IMMEDIATE SQL_STMT USING R14.PID;

                          SQLCOUNTS := SQL%ROWCOUNT;

                          COMMIT;

                          MSG := 'Processing LA_IR_ASSIGNMENT... '||vDB_LINK||' pid '||R14.pid;

                            INSERT INTO BOA_PROCESS_LOG
                            (
                              PROCESS,
                              SUB_PROCESS,
                              ENTRYDTE,
                              ROWCOUNTS,
                              MESSAGE
                            )
                            VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG);

                           COMMIT;

                           INSERT INTO LA_IR_ASSIGN_SENDT
                           VALUES (R14.PID, SQLCOUNTS);
                           COMMIT;

                  END LOOP;
                CLOSE C14;

       END;


---       f UTL_FILE.FILE_TYPE := UTL_FILE.FOPEN('BOA_COMPLETED_DIR', 'BOA_COMPLETED_FILES.txt', 'w');

          for x in 1..q.client.count loop

                   IF  (Q.CLIENT(x) = 'CHL' AND Q.FILE_NAME(x) LIKE '%DuplicateOrderCheckSGP_SWEEP_%' )  THEN

                       file_name_in := LTRIM(Q.FILE_NAME(x));

                       file_name_out := substr(file_name_in,1,37)||'.csv';
--                        FROM \CHL\IN_BOX\SAVE\SWEEP\                                TO \CHL\IN_BOX\SAVE\SWEEP\SAVE\
                       lineout :=  vDAISY_MV_FROM||file_name_out||'|'||vDAISY_MV_TO||file_name_out;

                       UTL_FILE.PUT_LINE(f,lineout);

                   END IF;

                   IF ( Q.CLIENT(x) = 'CHL' AND Q.FILE_NAME(x) LIKE '%DuplicateOrderCheckSGP_SWEEP_%' AND  Q.FILE_TYPE(x) IN ('NONE') )  THEN

--                                     C:\boaData\CHL\SAVE\               Z:\

                       lineout :=     vDAISY_CPY_FROM||file_name_out||'|'||vDAISY_SCRUB_REPORTS||file_name_out;

                       UTL_FILE.PUT_LINE(f,lineout);

                   end if;

                       lineout := vUSER_home||Q.CLIENT(x)||'\'||Q.FILE_NAME(x)||'|'||vUSER_home||Q.CLIENT(x)||'\LOADED\'||Q.FILE_NAME(x);

                       UTL_FILE.PUT_LINE(f,lineout);

          end loop;

           UTL_FILE.FCLOSE(f);

           vCompleted    := 1;
           vLoadComplete := 'YES';
           SubProc       := 'PROCESS_LOAD_QUEUE';



          forall i in 1..q.client.count
               EXECUTE IMMEDIATE 'UPDATE BOA_BILLING_LOAD_QUEUE  SET COMPLETED = :A, LOAD_COMPLTE = :B, FILE_TYPE = :C  where rowid = :D ' USING  vCompleted, vLoadComplete, q.FILE_TYPE(i), q.Rowids(i);
               commit;



    /************
      THEE END
     ***********/

     MSG       := 'Job Complete';

     SQLCOUNTS := 0;

    INSERT INTO BOA_PROCESS_LOG
    (
      PROCESS,
      SUB_PROCESS,
      ENTRYDTE,
      ROWCOUNTS,
      MESSAGE
    )
    VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG);

    COMMIT;


    /******************************
       Record any errors
     *****************************/

    EXCEPTION
       WHEN BAD_DATA THEN
                INSERT INTO BOA_PROCESS_LOG
                (
                  PROCESS,
                  SUB_PROCESS,
                  ENTRYDTE,
                  ROWCOUNTS,
                  MESSAGE
                )
                VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS,MSG);

                COMMIT;

                SEND_EMAIL  ( vTEAM, PROC||':'||SubProc, 'Loading Errors',  MSG);
                UTL_FILE.FCLOSE(f);
       WHEN OTHERS THEN

             MSG  := SQLERRM;

                INSERT INTO BOA_PROCESS_LOG
                (
                  PROCESS,
                  SUB_PROCESS,
                  ENTRYDTE,
                  ROWCOUNTS,
                  MESSAGE
                )
                VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS,MSG);

                COMMIT;

                SEND_EMAIL  ( vTEAM, PROC||':'||SubProc, 'Loading Errors',  MSG);
                UTL_FILE.FCLOSE(f);
    END;
/*-----------------------------
    CHL, BANA, BACFS, NAV

  --------------------------*/
    PROCEDURE INS_TO_LOAD_QUEUE ( P_CLIENT IN VARCHAR2, P_FILENAME VARCHAR2)
    IS

      SQLCOUNTS  PLS_INTEGER;
      MSG        VARCHAR2(1000);
      PROC       VARCHAR2(1000);
      SubProc    VARCHAR2(1000);


      bad_data   exception;

    BEGIN


      SQLCOUNTS  := 0;
      MSG        := 'Job Starting';
      PROC       := 'BOA_FEES_VALIDATE';
      SubProc    := 'INS_TO_LOAD_QUEUE';

      INSERT INTO BOA_PROCESS_LOG
      (
        PROCESS,
        SUB_PROCESS,
        ENTRYDTE,
        ROWCOUNTS,
        MESSAGE
      )
      VALUES ( proc,SubProc,SYSDATE, sqlcounts, MSG);

      COMMIT;

     ---- MAKE ENTRY

      INSERT INTO BOA_BILLING_LOAD_QUEUE
      (
        CLIENT,
        FILE_NAME,
        COMPLETED
      )
      VALUES ( P_CLIENT, P_FILENAME, 0);

      COMMIT;
    /************
      THEE END
     ***********/

     MSG       :=  P_CLIENT||' : '||P_FILENAME||' saved in queue';

     vTEAM     := CASE WHEN P_FILENAME LIKE 'DuplicateOrderCheckSGP_SWEEP%' THEN 'SYS Cancel'
                       WHEN P_CLIENT IN ('FILE_ERRORS')                     THEN 'RDM'
                       ELSE 'BOA IRecon'
                  END;
    sqlCounts := 0;

    INSERT INTO BOA_PROCESS_LOG
    (
      PROCESS,
      SUB_PROCESS,
      ENTRYDTE,
      ROWCOUNTS,
      MESSAGE
    )
    VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG);

    COMMIT;

        SEND_EMAIL  ( vTEAM, proc||'.'||subProc, 'Message from Load Queue', MSG);



    /******************************
       Record any errors
     *****************************/

    EXCEPTION
       WHEN BAD_DATA THEN
                INSERT INTO BOA_PROCESS_LOG
                (
                  PROCESS,
                  SUB_PROCESS,
                  ENTRYDTE,
                  ROWCOUNTS,
                  MESSAGE
                )
                VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS,MSG);

                COMMIT;

       SEND_EMAIL  ( vTEAM, proc||':'||subProc, 'Message from Load Queue', MSG);

       WHEN OTHERS THEN

             MSG  := SQLERRM;

                INSERT INTO BOA_PROCESS_LOG
                (
                  PROCESS,
                  SUB_PROCESS,
                  ENTRYDTE,
                  ROWCOUNTS,
                  MESSAGE
                )
                VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS,MSG);

                COMMIT;
         SEND_EMAIL  ( vTEAM, proc||':'||subProc, 'Message from Load Queue', MSG);

    END;
/*************************************************

 ************************************************/

    procedure BOA_DEDUPE_PROCESS
    is
      SQLCOUNTS  PLS_INTEGER;
      MSG        VARCHAR2(1000);
      PROC       VARCHAR2(1000);
      SubProc    VARCHAR2(1000);

      sql_stmt   VARCHAR2(32000);

      BEGIN


      SQLCOUNTS  := 0;
      MSG        := 'Job Starting';
      PROC       := 'BOA_FEES_VALIDATE';
      SubProc    := 'BOA_DEDUPE_PROCESS';


      INSERT INTO BOA_PROCESS_LOG
      (
        PROCESS,
        SUB_PROCESS,
        ENTRYDTE,
        ROWCOUNTS,
        MESSAGE
      )
      VALUES ( proc,SubProc,SYSDATE, sqlcounts, MSG);

      COMMIT;



    execute immediate 'TRUNCATE TABLE BOA_FEESRETURN_RKD DROP STORAGE';



    INSERT INTO BOA_FEESRETURN_RKD( TRANSACTION_TYPE, TRANID, TRANKY, ERROR_CODE, ERROR_MESSAGE, RESOLVED, RESEARCHING, RESENT, DEV, BILLED, RK )
    SELECT R.TRANSACTION_TYPE, TRIM(R.TRANID), R.TRANKY, R.ERROR_CODE, R.ERROR_MESSAGE, r.RESOLVED, R.RESEARCHING, R.RESENT, R.DEV, R.BILLED, R.RK
    FROM ( SELECT TRANSACTION_TYPE, TRANID, TRANKY, ERROR_CODE, ERROR_MESSAGE, RESOLVED, RESEARCHING, RESENT, DEV, BILLED, RANK() OVER ( PARTITION BY TRANSACTION_TYPE, TRANID, TRANKY, ERROR_CODE, ERROR_MESSAGE, RESOLVED, RESEARCHING, RESENT, DEV, BILLED ORDER BY ROWNUM) RK
           FROM ( select TRANSACTION_TYPE, TRANID, TRANKY, ERROR_CODE, (case when ERROR_MESSAGE is null then 'NULL' ELSE ERROR_MESSAGE END) AS ERROR_MESSAGE, RESOLVED, RESEARCHING, RESENT, DEV, BILLED
                  FROM BOA_FEESRETURN)) R;

          SQLCOUNTS  := SQL%ROWCOUNT;

     COMMIT;


      MSG        := 'Assign group numberS to BOA_FEESRETURN';

      INSERT INTO BOA_PROCESS_LOG
      (
        PROCESS,
        SUB_PROCESS,
        ENTRYDTE,
        ROWCOUNTS,
        MESSAGE
      )
      VALUES ( proc,SubProc,SYSDATE, sqlcounts, MSG);

      COMMIT;


    execute immediate ' TRUNCATE TABLE BOA_FEESRETURNED_ENHANCED DROP STORAGE ';

        INSERT INTO BOA_FEESRETURNED_ENHANCED (TRANSACTION_TYPE,
                                                   TRANID,
                                                   WORK_ORDER,
                                                   DONE_CD,
                                                   ORDER_DT,
                                                   TRANKY,
                                                   ERROR_CODE,
                                                   ERROR_MESSAGE,
                                                   RESOLVED,
                                                   RESEARCHING,
                                                   RESENT,
                                                   DEV,
                                                   BILLED)
           SELECT TRANSACTION_TYPE,
                  TRANID,
                  BOA_FEES_VALIDATE.GET_SPI_WORK_ORDER (TRANID),
                  BOA_FEES_VALIDATE.GET_SPI_PUNCH_CODE (TRANID),
                  BOA_FEES_VALIDATE.GET_SPI_order_date (TRANID),
                  TRANKY,
                  ERROR_CODE,
                  ERROR_MESSAGE,
                  RESOLVED,
                  RESEARCHING,
                  RESENT,
                  DEV,
                  BILLED
             From BOA_FEESRETURN_RKD
             where error_message  in ( 'Duplicate Transaction.')
             AND RK = 1;

          SQLCOUNTS  := SQL%ROWCOUNT;

    COMMIT;



      MSG        := 'Add accounts with errm Duplicate Transaction to Enhanced';


      INSERT INTO BOA_PROCESS_LOG
      (
        PROCESS,
        SUB_PROCESS,
        ENTRYDTE,
        ROWCOUNTS,
        MESSAGE
      )
      VALUES ( proc,SubProc,SYSDATE, sqlcounts, MSG);

      COMMIT;


    EXECUTE IMMEDIATE ' TRUNCATE TABLE BOA_FEESRETURN_NONDUPTRAN';


    INSERT INTO BOA_FEESRETURN_NonDupTran(TRANSACTION_TYPE, TRANID, WORK_ORDER, DONE_CD, ORDER_DT, TRANKY, ERROR_CODE, ERROR_MESSAGE, RESOLVED, RESEARCHING, RESENT, DEV, BILLED, RK )
    SELECT R.TRANSACTION_TYPE, R.TRANID, r.work_order, r.done_cd, r.order_dt,  R.TRANKY, R.ERROR_CODE, R.ERROR_MESSAGE, r.RESOLVED, R.RESEARCHING, R.RESENT, R.DEV, R.BILLED, R.RK
    FROM ( SELECT TRANSACTION_TYPE, TRANID, BOA_FEES_VALIDATE.GET_SPI_WORK_ORDER (TRANID) as work_order,BOA_FEES_VALIDATE.GET_SPI_PUNCH_CODE (TRANID) as done_cd, BOA_FEES_VALIDATE.GET_SPI_order_date (TRANID) as order_dt,
            TRANKY, ERROR_CODE, ERROR_MESSAGE, RESOLVED, RESEARCHING, RESENT, DEV, BILLED, RANK() OVER ( PARTITION BY  TRANID  ORDER BY tranid desc, ROWNUM) RK
              FROM  BOA_FEESRETURN_RKD
              where error_message  not in ( 'Duplicate Transaction.')
              AND RK = 1
              ) R ;

          SQLCOUNTS  := SQL%ROWCOUNT;

     COMMIT;


      MSG        := 'Assign group numberS to non duplicate transactions';

      INSERT INTO BOA_PROCESS_LOG
      (
        PROCESS,
        SUB_PROCESS,
        ENTRYDTE,
        ROWCOUNTS,
        MESSAGE
      )
      VALUES ( proc,SubProc,SYSDATE, sqlcounts, MSG);

      COMMIT;



        INSERT INTO BOA_FEESRETURNED_ENHANCED (TRANSACTION_TYPE,
                                                   TRANID,
                                                   WORK_ORDER,
                                                   DONE_CD,
                                                   ORDER_DT,
                                                   TRANKY,
                                                   ERROR_CODE,
                                                   ERROR_MESSAGE,
                                                   RESOLVED,
                                                   RESEARCHING,
                                                   RESENT,
                                                   DEV,
                                                   BILLED)
           SELECT TRANSACTION_TYPE,
                  TRANID,
                  BOA_FEES_VALIDATE.GET_SPI_WORK_ORDER (TRANID),
                  BOA_FEES_VALIDATE.GET_SPI_PUNCH_CODE (TRANID),
                  BOA_FEES_VALIDATE.GET_SPI_order_date (TRANID),
                  TRANKY,
                  ERROR_CODE,
                  ERROR_MESSAGE,
                  RESOLVED,
                  RESEARCHING,
                  RESENT,
                  DEV,
                  BILLED
             From BOA_FEESRETURN_NonDupTran
             where  RK = 1;

          SQLCOUNTS  := SQL%ROWCOUNT;

     COMMIT;

    EXECUTE IMMEDIATE 'TRUNCATE TABLE BOA_FEESRETURN_RKD DROP STORAGE ';


      MSG        := ' TRUNCATE TABLE BOA_FEESRETURN_RKD';

      INSERT INTO BOA_PROCESS_LOG
      (
        PROCESS,
        SUB_PROCESS,
        ENTRYDTE,
        ROWCOUNTS,
        MESSAGE
      )
      VALUES ( proc,SubProc,SYSDATE, sqlcounts, MSG);

      COMMIT;


    EXECUTE IMMEDIATE 'TRUNCATE TABLE BOA_FEESRETURN_NonDupTran DROP STORAGE ';


      MSG        := ' TRUNCATE TABLE BOA_FEESRETURN_NonDupTran';

      INSERT INTO BOA_PROCESS_LOG
      (
        PROCESS,
        SUB_PROCESS,
        ENTRYDTE,
        ROWCOUNTS,
        MESSAGE
      )
      VALUES ( proc,SubProc,SYSDATE, sqlcounts, MSG);

      COMMIT;


      MSG        := 'Add Non duplicate trans to ENHANCED';

      INSERT INTO BOA_PROCESS_LOG
      (
        PROCESS,
        SUB_PROCESS,
        ENTRYDTE,
        ROWCOUNTS,
        MESSAGE
      )
      VALUES ( proc,SubProc,SYSDATE, sqlcounts, MSG);

      COMMIT;



    EXECUTE IMMEDIATE 'TRUNCATE TABLE BOA_SUMMARY_GOOD_DEDUP DROP STORAGE';


      INSERT INTO  BOA_SUMMARY_GOOD_DEDUP (TRANSACTION,    TRANID, WORK_ORDER, DONE_CD, ORDER_DT, TRANKY, ERROR_CODE, ERROR_MESSAGE, RESOLVED, RESEARCHING, RESENT, DEV, BILLED, SOURCE, LOAN, TRAN_DT, TRANCODE, PRICE, WT, SWT, PRICEPER, QTY, UOM, SERVICEID, ZIP, STATE)
      SELECT a.TRANSACTION_TYPE, a.TRANID, a.WORK_ORDER, a.DONE_CD, a.ORDER_DT, a.TRANKY, a.ERROR_CODE, a.ERROR_MESSAGE, a.RESOLVED, a.RESEARCHING, a.RESENT, a.DEV, a.BILLED, r.sources, r.Loan, r.TRAN_DT, r.TRANCODE, r.PRICE,r.WT,r.SWT,r.PRICEPER,r.QTY,r.UOM,r.ServiceID,r.ZIP,r.ST
      FROM BOA_FEESRETURNED_ENHANCED A
      LEFT JOIN ( select b.TRANID, b.sources, b.Loan, b.TRAN_DT, b.TRANCODE, b.PRICE,b.WT,b.SWT,b.PRICEPER,b.QTY,b.UOM,b.ServiceID,b.ZIP,B.ST
                  from ( SELECT 'SGP_FEES' as SOURCES,
                                  A.TRANID,
                                  A.Loan,
                                  A.SentDate AS TRAN_DT,
                                  A.TRANCODE,
                                  A.PRICE,
                                  A.WT,
                                  A.SWT,
                                  A.PRICEPER,
                                  A.QTY,
                                  A.UOM,
                                  A.ServiceID,
                                  A.ZIP,
                                  B.State as ST
                                FROM SGP_FEES A
                                LEFT JOIN ( SELECT ZIPCODE, STATE, CITY FROM ZipCodes) B ON (A.ZIP = B.ZipCode)
                                UNION
                                SELECT
                                'SGP_PPOINVOICING' as SOURCES,
                                C.TRANID,
                                ISDIGIT(C.Loan) AS LOAN,
                                C.SENTDATE As TRAN_DT,
                                C.TRANCODE,
                                ISDIGIT(replace(c.price,'$','0')) AS PRICE,
                                ISDIGIT(C.WT) AS WT,
                                ISDIGIT(C.SWT) AS SWT,
                                ISDIGIT(replace(C.PRICEPER,'$','0')) AS PRICEPER,
                                ISDIGIT(C.QTY) AS QTY,
                                C.UOM,
                                ISDIGIT(C.ServiceID) AS SERVICEID,
                                C.ZIP,
                                D.State as ST
                                FROM SGP_PPOINVOICING  C
                                LEFT JOIN ( SELECT ZIPCODE, STATE, CITY FROM ZipCodes ) D ON (C.ZIP = D.ZipCode) ) b ) r
                              on ( TRIM(r.tranid)  = TRIM(a.tranid ))
       where a.billed = 1;

          SQLCOUNTS  := SQL%ROWCOUNT;

     COMMIT;


      MSG        := 'Add Unique billed to BOA SUMMARY GOOD DEDUP';

      INSERT INTO BOA_PROCESS_LOG
      (
        PROCESS,
        SUB_PROCESS,
        ENTRYDTE,
        ROWCOUNTS,
        MESSAGE
      )
      VALUES ( proc,SubProc,SYSDATE, sqlcounts, MSG);

      COMMIT;


    EXECUTE IMMEDIATE 'TRUNCATE TABLE BOA_SUMMARY_BAD_DEDUP  DROP STORAGE';

      INSERT INTO  BOA_SUMMARY_BAD_DEDUP (TRANSACTION,    TRANID, WORK_ORDER, DONE_CD, ORDER_DT, TRANKY, ERROR_CODE, ERROR_MESSAGE, RESOLVED, RESEARCHING, RESENT, DEV, BILLED, SOURCE, LOAN, TRAN_DT, TRANCODE, PRICE, WT, SWT, PRICEPER, QTY, UOM, SERVICEID, ZIP, STATE)
      SELECT a.TRANSACTION_TYPE, a.TRANID, a.WORK_ORDER, a.DONE_CD, a.ORDER_DT, a.TRANKY, a.ERROR_CODE, a.ERROR_MESSAGE, a.RESOLVED, a.RESEARCHING, a.RESENT, a.DEV, a.BILLED, r.sources, r.Loan, r.TRAN_DT, r.TRANCODE, r.PRICE,r.WT,r.SWT,r.PRICEPER,r.QTY,r.UOM,r.ServiceID,r.ZIP,r.ST
      FROM BOA_FEESRETURNED_ENHANCED A
      LEFT JOIN ( select b.TRANID, b.sources, b.Loan, b.TRAN_DT, b.TRANCODE, b.PRICE,b.WT,b.SWT,b.PRICEPER,b.QTY,b.UOM,b.ServiceID,b.ZIP,B.ST
                  from ( SELECT 'SGP_FEES' as SOURCES,
                                  A.TRANID,
                                  A.Loan,
                                  A.SentDate AS TRAN_DT,
                                  A.TRANCODE,
                                  A.PRICE,
                                  A.WT,
                                  A.SWT,
                                  A.PRICEPER,
                                  A.QTY,
                                  A.UOM,
                                  A.ServiceID,
                                  A.ZIP,
                                  B.State as ST
                                FROM SGP_FEES A
                                LEFT JOIN ( SELECT ZIPCODE, STATE, CITY FROM ZipCodes) B ON (A.ZIP = B.ZipCode)
                                UNION
                                SELECT
                                'SGP_PPOINVOICING' as SOURCES,
                                C.TRANID,
                                ISDIGIT(C.Loan) AS LOAN,
                                C.SENTDATE As TRAN_DT,
                                C.TRANCODE,
                                ISDIGIT(replace(c.price,'$','0')) AS PRICE,
                                ISDIGIT(C.WT) AS WT,
                                ISDIGIT(C.SWT) AS SWT,
                                ISDIGIT(replace(C.PRICEPER,'$','0')) AS PRICEPER,
                                ISDIGIT(C.QTY) AS QTY,
                                C.UOM,
                                ISDIGIT(C.ServiceID) AS SERVICEID,
                                C.ZIP,
                                D.State as ST
                                FROM SGP_PPOINVOICING  C
                                LEFT JOIN ( SELECT ZIPCODE, STATE, CITY FROM ZipCodes ) D ON (C.ZIP = D.ZipCode) ) b ) r
                              on ( TRIM(r.tranid)  = TRIM(a.tranid) )
       where a.billed = 0;

          SQLCOUNTS  := SQL%ROWCOUNT;

     COMMIT;


      MSG        := 'Add Unique NON billed to BOA SUMMARY BAD DEDUP';

      INSERT INTO BOA_PROCESS_LOG
      (
        PROCESS,
        SUB_PROCESS,
        ENTRYDTE,
        ROWCOUNTS,
        MESSAGE
      )
      VALUES ( proc,SubProc,SYSDATE, sqlcounts, MSG);

      COMMIT;



      MSG       := 'Job Complete';
      sqlCounts := 0;
      INSERT INTO BOA_PROCESS_LOG
      (
        PROCESS,
        SUB_PROCESS,
        ENTRYDTE,
        ROWCOUNTS,
        MESSAGE
      )
      VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG);

      COMMIT;
      /******************************
         Record any errors
       *****************************/

      exception
           WHEN OTHERS THEN

           MSG  := SQLERRM;

      INSERT INTO BOA_PROCESS_LOG
      (
        PROCESS,
        SUB_PROCESS,
        ENTRYDTE,
        ROWCOUNTS,
        MESSAGE
      )
      VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG);

      COMMIT;

    END;


  /******************************************************
    LOAD_SGP_PPO_ENHANCED
    modified on 09/10/2013 to process only new files

   *****************************************************/
  procedure  LOAD_SGP_PPO_ENHANCED
  IS

    CURSOR C3
    IS
    SELECT A.PID
    FROM SGP_PPOINVOICING A
    LEFT JOIN ( SELECT PID FROM SGP_PPO_LOADED_PIDS ) B ON ( B.PID = A.PID )
    WHERE B.PID IS NULL
    GROUP BY A.PID
    ORDER BY A.PID;

    R3   C3%ROWTYPE;

  SQLCOUNTS  PLS_INTEGER;
  MSG        VARCHAR2(1000);
  PROC       VARCHAR2(1000);
  SubProc    VARCHAR2(1000);

  sql_stmt   VARCHAR2(32000);

  BEGIN

    SQLCOUNTS  := 0;
    MSG        := 'Job Starting';
    PROC       := 'BOA_FEES_VALIDATE';
    SubProc    := 'LOAD_SGP_PPO_ENHANCED';
    vTEAM      := 'BOA IRecon';

  INSERT INTO BOA_PROCESS_LOG
  (
    PROCESS,
    SUB_PROCESS,
    ENTRYDTE,
    ROWCOUNTS,
    MESSAGE
  )
  VALUES ( proc,SubProc,SYSDATE, sqlcounts, MSG);

  COMMIT;


--   SQL_STMT := ' TRUNCATE TABLE SGP_PPOINVOICING_ENHANCED DROP STORAGE';

--    EXECUTE IMMEDIATE SQL_STMT;
  open c3;
       loop
          fetch c3 into r3;
          exit when c3%notfound;

          INSERT INTO  SGP_PPOINVOICING_ENHANCED (TRANID, WORK_ORDER, DONE_CD, ORDER_DT, LOAN, SENTDATE, TRANCODE, PRICE, FIELD6, FIELD7, FIELD8, FIELD9, FIELD10, FIELD11, WT, SWT, PRICEPER, QTY, UOM, SERVICEID, ZIP,PID )
          SELECT TRIM(TRANID),
                    BOA_FEES_VALIDATE.GET_SPI_WORK_ORDER (TRANID) AS WORK_ORDER,
                    BOA_FEES_VALIDATE.GET_SPI_PUNCH_CODE (TRANID) AS DONE_CD,
                    BOA_FEES_VALIDATE.GET_SPI_order_date (TRANID) AS ORDER_DT,
                    LOAN,
                    SENTDATE,
                    TRANCODE,
                    PRICE,
                    FIELD6,
                    FIELD7,
                    FIELD8,
                    FIELD9,
                    FIELD10,
                    FIELD11,
                    WT,
                    SWT,
                    PRICEPER,
                    QTY,
                    UOM,
                    SERVICEID,
                    ZIP,
                    PID
               FROM SGP_PPOINVOICING
               WHERE PID = R3.PID;



           SQLCOUNTS  := SQL%ROWCOUNT;

           COMMIT;

            MSG       := 'SGP_PPO TO SGP_PPO ENHANCED.. '||R3.PID;

            INSERT INTO BOA_PROCESS_LOG
            (
              PROCESS,
              SUB_PROCESS,
              ENTRYDTE,
              ROWCOUNTS,
              MESSAGE
            )
            VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG);

            COMMIT;

          INSERT INTO SGP_PPO_LOADED_PIDS
          VALUES (R3.PID, SQLCOUNTS );
          COMMIT;

    END LOOP;

  CLOSE C3;

      SQL_STMT := ' INSERT INTO IPP$LIBRARIAN.BOA_FEE_STATUS_LOG@'||vDB_LINK||' ( PID, PROCESS, PER_COMPETE, ENTRY_DATE) ';
      SQL_STMT := SQL_STMT||' VALUES ( :1, :2, :3, :4 )';

      EXECUTE IMMEDIATE SQL_STMT USING 2, 'Enhance Fees and Invoices' ,50 ,SYSDATE;

      COMMIT;


  /************
    THEE END
   ***********/

   MSG  := 'SGP_PPOINVOICING_ENHANCED Loaded ';
   sqlCounts := 0;
  INSERT INTO BOA_PROCESS_LOG
  (
    PROCESS,
    SUB_PROCESS,
    ENTRYDTE,
    ROWCOUNTS,
    MESSAGE
  )
  VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG);

  COMMIT;
  /******************************
     Record any errors
   *****************************/

  exception
       WHEN OTHERS THEN

       MSG  := SQLERRM;

  INSERT INTO BOA_PROCESS_LOG
  (
    PROCESS,
    SUB_PROCESS,
    ENTRYDTE,
    ROWCOUNTS,
    MESSAGE
  )
  VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG);

  COMMIT;

  END;
   /******************************************
     PROCESS_NEW_FEEDS

     Created 09/23/2013

    *****************************************/
   PROCEDURE PROCESS_NEW_FEEDS
   is

    SQLCOUNTS  PLS_INTEGER;
    MSG        VARCHAR2(1000);
    PROC       VARCHAR2(1000);
    SubProc    VARCHAR2(1000);

    sql_stmt   VARCHAR2(32000);

    qCount     pls_integer;
    fCount     pls_integer;
    rCount     pls_integer;
    pCount     pls_integer;
    aCount     pls_integer;

    gc         genRefCursor;

    Q    BOA_BILLING_LOAD_QUEUE_ARY;

  BEGIN

      SQLCOUNTS  := 0;
      MSG        := 'Job Starting';
      PROC       := 'BOA_FEES_VALIDATE';
      SubProc    := 'PROCESS_NEW_FEEDS';

      qCount     := 0;
      fCount     := 0;
      rCount     := 0;
      pCount     := 0;
      aCount     := 0;

      INSERT INTO BOA_PROCESS_LOG
      (
        PROCESS,
        SUB_PROCESS,
        ENTRYDTE,
        ROWCOUNTS,
        MESSAGE
      )
      VALUES ( proc,SubProc,SYSDATE, sqlcounts, MSG);

      COMMIT;


      OPEN GC FOR SELECT  LTRIM(RTRIM( CLIENT)) AS CLIENT, LTRIM(RTRIM(FILE_NAME)) AS FILE_NAME,  COMPLETED, FILE_TYPE,  ROWID  FROM BOA_BILLING_LOAD_QUEUE WHERE COMPLETED = 0;

      FETCH GC BULK COLLECT INTO Q.CLIENT, Q.FILE_NAME, Q.COMPLETED, Q.FILE_TYPE, Q.rowids;

      CLOSE GC;


        qCount     := q.client.count;


          for j in 1..q.client.count loop


             IF   ( Q.FILE_NAME(j) LIKE '%DuplicateOrderCheckSGP_%')
                  THEN
                     Q.FILE_TYPE(j)  := 'CHL CANCEL';
                     qCount          := qCount + 1;
             ELSIF ( Q.CLIENT(j) LIKE 'FILE_ERRORS%')
                  THEN
                     Q.FILE_TYPE(j)  := 'Loading Errors';
                     qCount          := qCount + 1;

             ELSIF   ( Q.FILE_NAME(j) LIKE 'SGP_FEES_%')
                  THEN
                      Q.FILE_TYPE(j) := 'FEES';
                      qCount         := qCount + 1;
             ELSIF ( Q.FILE_NAME(j) LIKE 'BOA_FEESRETURN_%')
                  THEN
                      Q.FILE_TYPE(j)  := 'FEES Returned';
                      qCount          := qCount + 1;
             ELSIF ( Q.FILE_NAME(j) LIKE 'SGP_PPOINVOICING_%')
                  THEN
                     Q.FILE_TYPE(j)  := 'PPO Invoicing';
                     qCount          := qCount + 1;
             ELSIF ( Q.FILE_NAME(j) LIKE 'ARInv%')
                  THEN
                     Q.FILE_TYPE(j)  := 'AR Invoicing';
                     qCount          := qCount + 1;
             ELSIF ( Q.FILE_NAME(j) LIKE '%_IR_Assignment_%')
                  THEN
                     Q.FILE_TYPE(j)  := 'LA Files';
                     qCount          := qCount + 1;

             ELSIF ( Q.FILE_NAME(j) LIKE 'ResolutionPending_%')
                  THEN
                     Q.FILE_TYPE(j)  := 'LA Files';
                     qCount          := qCount + 1;

             END IF;
          end loop;


         if   ( qCount > 0 )
         then
               BOA_FEES_VALIDATE.PROCESS_LOAD_QUEUE;
         end if;


      MSG  := 'Process complete ';
      sqlCounts := 0;
      INSERT INTO BOA_PROCESS_LOG
      (
        PROCESS,
        SUB_PROCESS,
        ENTRYDTE,
        ROWCOUNTS,
        MESSAGE
      )
      VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG);

      COMMIT;

    exception
       WHEN OTHERS THEN

       MSG  := SQLERRM;

      INSERT INTO BOA_PROCESS_LOG
      (
        PROCESS,
        SUB_PROCESS,
        ENTRYDTE,
        ROWCOUNTS,
        MESSAGE
      )
      VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG);

      COMMIT;

     SEND_EMAIL  ( vTEAM, PROC||':'||SubProc, 'Loading Errors',  MSG);

   END;
  /*-------------------------------------------

   -------------------------------------------*/

  PROCEDURE UPDATE_LOAD_STATUS
  IS

        CURSOR C4
        IS
        SELECT A.PID
        FROM LOAD_QUEUE_TOSEND A
        LEFT JOIN ( SELECT PID FROM LOAD_QUEUE_PROCESSED ) B ON ( B.PID = A.PID )
        WHERE B.PID IS NULL
        GROUP BY A.PID
        ORDER BY A.PID;

        R4   C4%ROWTYPE;


        CURSOR C5
        IS
        SELECT A.PID
        FROM  FILES_LOADED_TOSEND A
        LEFT JOIN ( SELECT PID FROM FILES_LOADED_PROCESSED ) B ON ( B.PID = A.PID )
        WHERE B.PID IS NULL
        GROUP BY A.PID
        ORDER BY A.PID;

        R5   C5%ROWTYPE;

    SQLCOUNTS  PLS_INTEGER;
    ppid       pls_integer;

    MSG        VARCHAR2(1000);
    PROC       VARCHAR2(1000);
    SubProc    VARCHAR2(1000);

    sql_stmt   VARCHAR2(32000);

    gc         genRefCursor;


  BEGIN

      SQLCOUNTS  := 0;
      ppid       := 0;
      MSG        := 'Job Starting';
      PROC       := 'BOA_FEES_VALIDATE';
      SubProc    := 'UPDATE_LOAD_STATUS';


      INSERT INTO BOA_PROCESS_LOG
      (
        PROCESS,
        SUB_PROCESS,
        ENTRYDTE,
        ROWCOUNTS,
        MESSAGE
      )
      VALUES ( proc,SubProc,SYSDATE, sqlcounts, MSG);

      COMMIT;

      /****************************************
       don't send what was not loaded



          delete FROM LOAD_QUEUE_TOSEND A
                 where exists ( select 1
                                 from   bOA_BILLING_LOAD_QUEUE b
                                where b.COMPLETED = 2
                                 and   a.pid = b.pid);

        commit;

       ***************************************/

                open c4;
                     loop
                         fetch c4 into r4;
                         exit when c4%notfound;
                --         EXIT WHEN CNT > 10;

                          SQL_STMT := ' INSERT INTO  RDM.BOA_BILLING_LOAD_QUEUE@'||vDB_LINK||' (PID,  CLIENT,FILE_NAME,COMPLETED,ENTRYDTE,LOAD_COMPLTE,FILE_TYPE ) ';
                          SQL_STMT := SQL_STMT||' select  A.PID, A.CLIENT, A.FILE_NAME, A.COMPLETED, sysdate, A.LOAD_COMPLTE, A.FILE_TYPE ';
                          SQL_STMT := SQL_STMT||'    from BOA_BILLING_LOAD_QUEUE A ';
                          SQL_STMT := SQL_STMT||'     WHERE A.PID = :1 ';

                          EXECUTE IMMEDIATE SQL_STMT USING R4.PID;



                           MSG := 'IPP$LIBRARIAN BILLING LOAD QUEUE APEXD1';

                          SQL_STMT := ' INSERT INTO IPP$LIBRARIAN.BOA_BILLING_LOAD_QUEUE@'||vDB_LINK||' (PID,  CLIENT,FILE_NAME,COMPLETED,ENTRYDTE,LOAD_COMPLTE,FILE_TYPE ) ';
                          SQL_STMT := SQL_STMT||' select  A.PID, A.CLIENT, A.FILE_NAME, A.COMPLETED, A.ENTRYDTE, A.LOAD_COMPLTE, A.FILE_TYPE ';
                          SQL_STMT := SQL_STMT||'    from BOA_BILLING_LOAD_QUEUE A ';
                          SQL_STMT := SQL_STMT||'     WHERE A.PID = :1 ';

                          EXECUTE IMMEDIATE SQL_STMT USING R4.PID;


                          MSG := 'Both RDM(s) plus LIBRARIAN BOA BILLING LOAD QUEUE UPDATE WITH .. pid '||r4.pid;


                          INSERT INTO BOA_PROCESS_LOG
                          (
                            PROCESS,
                            SUB_PROCESS,
                            ENTRYDTE,
                            ROWCOUNTS,
                            MESSAGE
                          )
                          VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG);

                          COMMIT;

                                select COUNT(*)
                                into  ppid
                                from  bOA_BILLING_LOAD_QUEUE A
                                LEFT JOIN ( SELECT PID, CLIENT, FILE_NAME, RECORDCNT, ENTRY_DATE AS LOAD_DATE
                                             FROM BOFA_FILES_PROCESSED ) B
                                           ON ( A.file_name = B.file_name)
                                where a.pid = r4.pid
                                 and  A.COMPLETED = 1;

                           if ( ppid > 0 )
                               then

                               INSERT INTO LOAD_QUEUE_PROCESSED
                               VALUES (R4.PID);
                               COMMIT;

                           end if;

                     end loop;
                close c4;



                            INSERT INTO BOA_PROCESS_LOG
                            (
                              PROCESS,
                              SUB_PROCESS,
                              ENTRYDTE,
                              ROWCOUNTS,
                              MESSAGE
                            )
                            VALUES ( proc,SubProc,SYSDATE, sqlcounts, MSG);

                            COMMIT;



      /****************************************
       ***************************************/



                            MSG := 'IPP$LIBRARIAN BOA_FILES LOAD QUEUE APXP01';

                                    SQL_STMT :=   '    INSERT INTO IPP$LIBRARIAN.BOFA_FILES_PROCESSED@'||vDB_LINK||'  (PID,    ';
                                    SQL_STMT :=  SQL_STMT|| '                                                          CLIENT,    ';
                                    SQL_STMT :=  SQL_STMT|| '                                                          FILE_NAME,    ';
                                    SQL_STMT :=  SQL_STMT|| '                                                          RECORDCNT,    ';
                                    SQL_STMT :=  SQL_STMT|| '                                                          ENTRY_DATE,    ';
                                    SQL_STMT :=  SQL_STMT|| '                                                          COMMENTS,    ';
                                    SQL_STMT :=  SQL_STMT|| '                                                          REPORT_TIME)';
                                    SQL_STMT :=  SQL_STMT|| '       SELECT a.pid,    ';
                                    SQL_STMT :=  SQL_STMT|| '              a.client,    ';
                                    SQL_STMT :=  SQL_STMT|| '              a.file_name,    ';
                                    SQL_STMT :=  SQL_STMT|| '              a.recordcnt,    ';
                                    SQL_STMT :=  SQL_STMT|| '              a.entry_date,    ';
                                    SQL_STMT :=  SQL_STMT|| '              a.comments,    ';
                                    SQL_STMT :=  SQL_STMT|| '              SYSDATE    ';
                                    SQL_STMT :=  SQL_STMT|| '         FROM BOFA_FILES_PROCESSED a     ';
                                    SQL_STMT :=  SQL_STMT|| '         LEFT JOIN ( SELECT PID      ';
                                    SQL_STMT :=  SQL_STMT|| '                        FROM  FILES_LOADED_PROCESSED ) B ON ( A.PID = B.PID)     ';
                                    SQL_STMT :=  SQL_STMT|| '         WHERE B.PID IS NULL    ';

                            EXECUTE IMMEDIATE SQL_STMT;

                            SQLCOUNTS := SQL%ROWCOUNT;

                            COMMIT;

                            INSERT INTO BOA_PROCESS_LOG
                            (
                              PROCESS,
                              SUB_PROCESS,
                              ENTRYDTE,
                              ROWCOUNTS,
                              MESSAGE
                            )
                            VALUES ( proc,SubProc,SYSDATE, sqlcounts, MSG);

                            COMMIT;





                              open c5;
                                   loop
                                       fetch c5 into r5;
                                       exit when c5%notfound;
                              --         EXIT WHEN CNT > 10;


                                          MSG := 'UPDATE BOFA FILES PROCESSED on Server.. pid '||r5.pid;


                                        INSERT INTO BOA_PROCESS_LOG
                                        (
                                          PROCESS,
                                          SUB_PROCESS,
                                          ENTRYDTE,
                                          ROWCOUNTS,
                                          MESSAGE
                                        )
                                        VALUES ( proc, SubProc,SYSDATE, 0, MSG);

                                        COMMIT;


                                         INSERT INTO FILES_LOADED_PROCESSED
                                         VALUES (R5.PID);
                                         COMMIT;

                                   end loop;
                              close c5;


      MSG  := 'Process complete ';
      sqlCounts := 0;
      INSERT INTO BOA_PROCESS_LOG
      (
        PROCESS,
        SUB_PROCESS,
        ENTRYDTE,
        ROWCOUNTS,
        MESSAGE
      )
      VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG);

      COMMIT;

    exception
       WHEN OTHERS THEN

       MSG  := MSG||' => '||SQLERRM;

      INSERT INTO BOA_PROCESS_LOG
      (
        PROCESS,
        SUB_PROCESS,
        ENTRYDTE,
        ROWCOUNTS,
        MESSAGE
      )
      VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG);

      COMMIT;

  END;

/*******************************************************/

  PROCEDURE BOA_TABLE_CLEANUP( P_DAYS_BACK NUMBER)
  IS

    SGP_PID_CLEANUPNTT SGP_PID_CLEANUP_NTT;

    SQLCOUNTS  PLS_INTEGER;

    MSG        VARCHAR2(1000);
    PROC       VARCHAR2(1000);
    SubProc    VARCHAR2(1000);

    sql_stmt    VARCHAR2(32000);
    DML_STMT1   VARCHAR2(32000);
    DML_STMT2   VARCHAR2(32000);
    DML_STMT3   VARCHAR2(32000);

    P_DATE     DATE;
    P_ZERO     NUMBER;

    gc         genRefCursor;


  BEGIN

      SQLCOUNTS  := 0;
      MSG        := 'Job Starting';
      PROC       := 'BOA_FEES_VALIDATE';
      SubProc    := 'BOA_TABLE_CLEANUP';
      P_DATE     := TRUNC(SYSDATE);
      P_ZERO     := 0;

      SGP_PID_CLEANUPNTT := SGP_PID_CLEANUP_NTT();
      SQL_STMT  := 'select pid, TABLE_NAME, COMPLETED, ENTRYDATE from SGP_PID_CLEANUP WHERE TRUNC(ENTRYDATE) <  ( :1 - :2) AND COMPLETED = :3 ';

      OPEN GC FOR SQL_STMT USING P_DATE, P_DAYS_BACK, P_ZERO;

              FETCH GC  BULK COLLECT INTO SGP_PID_CLEANUPNTT;
               FOR k in 1..SGP_PID_CLEANUPNTT.COUNT LOOP

                     DML_STMT1 := 'TAKE-NO-ACTION';

                     DML_STMT2 := 'TAKE-NO-ACTION';


                     IF (SGP_PID_CLEANUPNTT(k).TABLE_NAME IN ('SGP_FEES'))  THEN

                         DML_STMT1 := ' DELETE FROM SGP_FEES WHERE PID = :A ';

                         DML_STMT2 := ' DELETE FROM SGP_FEES_ENHANCED WHERE PID = :B ';

                     END IF;

                     IF (SGP_PID_CLEANUPNTT(k).TABLE_NAME IN ('SGP_PPOINVOICING'))  THEN

                         DML_STMT1 := ' DELETE FROM SGP_PPOINVOICING WHERE PID = :C ';

                         DML_STMT2 := ' DELETE FROM SGP_PPOINVOICING_ENHANCED WHERE PID = :D ';

                     END IF;


                     IF (SGP_PID_CLEANUPNTT(k).TABLE_NAME IN ('BOA_FEESRETURN'))  THEN

                         DML_STMT1 := ' DELETE FROM BOA_FEESRETURN WHERE PID = :E ';


                     END IF;

                     IF (SGP_PID_CLEANUPNTT(k).TABLE_NAME IN ('BOA_NAV_ARINVOICING'))  THEN

                         DML_STMT1 := ' DELETE FROM BOA_NAV_ARINVOICING WHERE PID = :F ';


                     END IF;

                     IF (SGP_PID_CLEANUPNTT(k).TABLE_NAME IN ('XCL_SWEEP','SPI_DAISY'))  THEN

                         DML_STMT1 := ' DELETE FROM XCL_SWEEP WHERE PID = :F ';


                     END IF;


                   IF  (DML_STMT1 NOT IN ('TAKE-NO-ACTION') )
                      THEN

                      EXECUTE IMMEDIATE DML_STMT1 USING  SGP_PID_CLEANUPNTT(k).pid;

                      SQLCOUNTS  := SQL%ROWCOUNT;

                      COMMIT;

                        INSERT INTO BOA_PROCESS_LOG
                        (
                          PROCESS,
                          SUB_PROCESS,
                          ENTRYDTE,
                          ROWCOUNTS,
                          MESSAGE
                        )
                        VALUES ( proc, SubProc,SYSDATE, sqlcounts, DML_STMT1||' = '||SGP_PID_CLEANUPNTT(k).pid);

                        COMMIT;

                   END IF;

                   IF  (DML_STMT2 NOT IN ('TAKE-NO-ACTION') )
                      THEN

                     EXECUTE IMMEDIATE DML_STMT2 USING  SGP_PID_CLEANUPNTT(k).pid;

                      SQLCOUNTS  := SQL%ROWCOUNT;

                     COMMIT;

                        INSERT INTO BOA_PROCESS_LOG
                        (
                          PROCESS,
                          SUB_PROCESS,
                          ENTRYDTE,
                          ROWCOUNTS,
                          MESSAGE
                        )
                        VALUES ( proc, SubProc,SYSDATE, sqlcounts, DML_STMT2||' = '||SGP_PID_CLEANUPNTT(k).pid);

                        COMMIT;


                   END IF;

                  DML_STMT3  := ' UPDATE SGP_PID_CLEANUP SET  COMPLETED = 1 WHERE PID = :4 ';

                  EXECUTE IMMEDIATE  DML_STMT3 USING  SGP_PID_CLEANUPNTT(k).pid;

                  COMMIT;

               END LOOP;
      CLOSE GC;



      INSERT INTO BOA_PROCESS_LOG
      (
        PROCESS,
        SUB_PROCESS,
        ENTRYDTE,
        ROWCOUNTS,
        MESSAGE
      )
      VALUES ( proc,SubProc,SYSDATE, sqlcounts, MSG);

      COMMIT;

      MSG  := 'Process complete ';
      sqlCounts := 0;
      INSERT INTO BOA_PROCESS_LOG
      (
        PROCESS,
        SUB_PROCESS,
        ENTRYDTE,
        ROWCOUNTS,
        MESSAGE
      )
      VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG);

      COMMIT;
    exception
       WHEN OTHERS THEN

       MSG  := SQLERRM;

      INSERT INTO BOA_PROCESS_LOG
      (
        PROCESS,
        SUB_PROCESS,
        ENTRYDTE,
        ROWCOUNTS,
        MESSAGE
      )
      VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG);

      COMMIT;

  END;


/********************************************************************************************************************
  procedure  LOAD_NAV_ARINVOICING
  CREATED   12/31/2013

 *******************************************************************************************************************/
  procedure  LOAD_NAV_ARINVOICING(P_FILENAME IN VARCHAR2, P_ID IN NUMBER, P_MESSAGE IN VARCHAR2, p_rcode out number )

   IS

    SQLCOUNTS  PLS_INTEGER;
    MSG        VARCHAR2(1000);
    PROC       VARCHAR2(1000);
    SubProc    VARCHAR2(1000);
    rcode      NUMBER;

    sql_stmt   VARCHAR2(32000);
    whoami     varchar2(100);
    BAD_DATA   EXCEPTION;

  BEGIN


    SQLCOUNTS  := 0;
    MSG        := 'Job Starting';
    PROC       := 'BOA_FEES_VALIDATE';
    SubProc    := 'LOAD_NAV_ARINVOICING';
    vTEAM      := 'BOA IRecon';

  INSERT INTO BOA_PROCESS_LOG
  (
    PROCESS,
    SUB_PROCESS,
    ENTRYDTE,
    ROWCOUNTS,
    MESSAGE
  )
  VALUES ( proc,SubProc,SYSDATE, sqlcounts, MSG);

  COMMIT;

   If ( p_id < 0 )
     then
       MSG :=   'an Error occured while attempt to log the file name ';
       rcode := -6502;
       RAISE BAD_DATA;
   END IF;

      SQL_STMT :=   CASE  WHEN P_MESSAGE = 'LOAD_ARINVOICING_DIR'   THEN  'ALTER TABLE NAV_ARINVOICING_EXT  LOCATION (LOAD_ARINVOICING_DIR:'''||P_FILENAME||''') '
                    ELSE  'UNKNOWN-WORKING-DIRECTORY'
                   END;

  IF (SQL_STMT IN ('UNKNOWN-WORKING-DIRECTORY') )
     THEN
     MSG   := SQL_STMT;
     RCODE := -6502;
     RAISE BAD_DATA;
  END IF;

    EXECUTE IMMEDIATE SQL_STMT;

      insert into  BOA_NAV_ARINVOICING
       (
         RECORD_TYPE,INVOICE_DATE,INVOICE_NBR,ORDER_NBR,BILL_CODE,CLIENT_CODE,BILL_TO_CODE,DEPT,X1,PROP_NBR,LOAN_NBR,VENDOR_CODE,WORK_CODE,X2,X3,CONS,SALES,COSTS,X4,X5,PHOTOS,X6,X7,ORDER_DATE,STATE,ZIP,CONTROL_NBR,ORDER_BY_NAME,COMPLETION_DATE,OCCUPANCY,LOAN_TYPE,ZIP_CODE2,TAX_AMOUNT,EXEMPT_AMOUNT,EXEMPT_REASON,TAX_PERCENT,PID
       )
       select RECORD_TYPE,INVOICE_DATE,INVOICE_NBR,ORDER_NBR,BILL_CODE,CLIENT_CODE,BILL_TO_CODE,DEPT,X1,PROP_NBR,LOAN_NBR,VENDOR_CODE,WORK_CODE,X2,X3,CONS,SALES,COSTS,X4,X5,PHOTOS,X6,X7,ORDER_DATE,STATE,ZIP,CONTROL_NBR,ORDER_BY_NAME,COMPLETION_DATE,OCCUPANCY,LOAN_TYPE,ZIP_CODE2,TAX_AMOUNT,EXEMPT_AMOUNT,EXEMPT_REASON,TAX_PERCENT,P_ID
       from NAV_ARINVOICING_EXT;

      SQLCOUNTS  := SQL%ROWCOUNT;
      MSG  := P_FILENAME||' NAV FILE LOADED into BOA_NAV_ARINVOICING';

      COMMIT;

--------------------------
-- UPDATE LOG
-------------------------
    UPDATE BOFA_FILES_PROCESSED
    SET RECORDCNT = SQLCOUNTS, COMMENTS = MSG
    WHERE PID  = P_ID;

    COMMIT;

    INSERT INTO NAV_ARINV_TOSEND(PID, CNT)
    VALUES ( P_ID, SQLCOUNTS);
    COMMIT;

    INSERT INTO SGP_PID_CLEANUP ( PID, TABLE_NAME, COMPLETED, ENTRYDATE)
    VALUES ( P_ID, 'BOA_NAV_ARINVOICING', 0, SYSDATE);
    COMMIT;

  INSERT INTO BOA_PROCESS_LOG
  (
    PROCESS,
    SUB_PROCESS,
    ENTRYDTE,
    ROWCOUNTS,
    MESSAGE
  )
  VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG);

  COMMIT;

  /************
    THEE END
   ***********/

   MSG       := P_FILENAME||':Job Complete';

  sqlCounts := 0;

  INSERT INTO BOA_PROCESS_LOG
  (
    PROCESS,
    SUB_PROCESS,
    ENTRYDTE,
    ROWCOUNTS,
    MESSAGE
  )
  VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG);

  COMMIT;

  P_RCODE := 0;

  /******************************
     Record any errors
   *****************************/

  exception
        WHEN  BAD_DATA THEN
        P_RCODE := RCODE;
        INSERT INTO BOA_PROCESS_LOG
        (
          PROCESS,
          SUB_PROCESS,
          ENTRYDTE,
          ROWCOUNTS,
          MESSAGE
        )
        VALUES ( proc, SubProc,SYSDATE, rcode, P_FILENAME||': '||MSG);

        COMMIT;

                  SEND_EMAIL  ( vTEAM, proc||'.'||subProc, 'Message From Load process', MSG);

      WHEN OTHERS THEN

       MSG  := SQLERRM;
       P_RCODE := SQLCODE;
         RCODE := SQLCODE;
        INSERT INTO BOA_PROCESS_LOG
        (
          PROCESS,
          SUB_PROCESS,
          ENTRYDTE,
          ROWCOUNTS,
          MESSAGE
        )
        VALUES ( proc, SubProc,SYSDATE, RCODE, P_FILENAME||': '||MSG);

      COMMIT;

        SEND_EMAIL  ( vTEAM, proc||'.'||subProc, 'Message From Load process', MSG);

  END;

    procedure  LOAD_LPS_HEADER(P_FILENAME IN VARCHAR2 , P_ID IN  NUMBER, P_MESSAGE IN VARCHAR2, P_RCODE OUT NUMBER)
   IS

    SQLCOUNTS  PLS_INTEGER;
    MSG        VARCHAR2(1000);
    PROC       VARCHAR2(1000);
    SubProc    VARCHAR2(1000);
    rcode      NUMBER;

    sql_stmt   VARCHAR2(32000);
    whoami     varchar2(100);
    BAD_DATA   EXCEPTION;

  BEGIN


    SQLCOUNTS  := 0;
    MSG        := 'Job Starting';
    PROC       := 'BOA_FEES_VALIDATE';
    SubProc    := 'LOAD_LPS_HEADER';

  INSERT INTO BOA_PROCESS_LOG
  (
    PROCESS,
    SUB_PROCESS,
    ENTRYDTE,
    ROWCOUNTS,
    MESSAGE
  )
  VALUES ( proc,SubProc,SYSDATE, sqlcounts, MSG);

  COMMIT;

   If ( p_id < 0 )
     then
       MSG :=   'an Error occured while attempt to log the file name ';
       rcode := -6502;
       RAISE BAD_DATA;
   END IF;

      SQL_STMT :=   CASE  WHEN P_MESSAGE = 'BOA_LPS_HEADER_DIR'   THEN  'ALTER TABLE BOA_LPS_HEADER_EXT  LOCATION (BOA_LPS_HEADER_DIR:'''||P_FILENAME||''') '
                    ELSE  'UNKNOWN-WORKING-DIRECTORY'
                   END;

  IF (SQL_STMT IN ('UNKNOWN-WORKING-DIRECTORY') )
     THEN
     MSG   := SQL_STMT;
     RCODE := -6502;
     RAISE BAD_DATA;
  END IF;

INSERT INTO BOA_LPS_HEADER (VENDORID,
                            INVOICENUMBER,
                            INVOICEDATE,
                            INVOICETYPECODE,
                            INVOICEAMOUNT,
                            SERVICERCODE,
                            SERVICERLOANNUMBER,
                            CREATEDBYVENDORUSERID,
                            DEPARTMENTID,
                            BKSTATE,
                            JUDICIALINDICATOR,
                            BORROWERNAME,
                            PROPERTYADDRESS1,
                            PROPERTYADDRESS2,
                            PROPERTYCITY,
                            PROPERTYCOUNTY,
                            PROPERTYSTATE,
                            PROPERTYZIPCODE,
                            PROPERTYZIPCODESUFFIX,
                            REFERRALDATE,
                            BKCHAPTER,
                            BKCASENUMBER,
                            BKFILEDAFTERSALE,
                            BKLOANCURRENTWHENFILED,
                            BKCOLLATERALINVESTPROP,
                            BKSERVICESPOSTCONF,
                            FCSALEDATE,
                            FCSALENOTHELDREASON,
                            FCSALEHELD,
                            LEGALACTIONDATE,
                            VENDORREFNUMBER,
                            BKSERVICESPRECONFIRM,
                            BKDISPOSITION,
                            AH,
                            AI,
                            ORDERTYPE,
                            ORDERCOMPLETEDATE,
                            PROPERTYVALUE,
                            SERVICEREQUESTID,
                            SERVICEREQUESTSTAGE,
                            VIN1,
                            VIN2,
                            VIN3,
                            INVOICECOMMENT,
                            DATEBKFILED,
                            FCSALERESULT,
                            WRITISSUED,
                            WRITISSUEDDATE,
                            WRITHELDTYPE,
                            MFRFILEDDATE,
                            DILFILEDDATE,
                            ASSETNUMBER,
                            PARENTSERVICERCODE,
                            REFERRALTYPEID,
                            PID)
   SELECT VENDORID,
          INVOICENUMBER,
          TO_DATE(INVOICEDATE,'MM/DD/YYYY'),
          INVOICETYPECODE,
          INVOICEAMOUNT,
          SERVICERCODE,
          SERVICERLOANNUMBER,
          CREATEDBYVENDORUSERID,
          DEPARTMENTID,
          BKSTATE,
          JUDICIALINDICATOR,
          BORROWERNAME,
          PROPERTYADDRESS1,
          PROPERTYADDRESS2,
          PROPERTYCITY,
          PROPERTYCOUNTY,
          PROPERTYSTATE,
          PROPERTYZIPCODE,
          PROPERTYZIPCODESUFFIX,
          REFERRALDATE,
          BKCHAPTER,
          BKCASENUMBER,
          BKFILEDAFTERSALE,
          BKLOANCURRENTWHENFILED,
          BKCOLLATERALINVESTPROP,
          BKSERVICESPOSTCONF,
          FCSALEDATE,
          FCSALENOTHELDREASON,
          FCSALEHELD,
          LEGALACTIONDATE,
          VENDORREFNUMBER,
          BKSERVICESPRECONFIRM,
          BKDISPOSITION,
          AH,
          AI,
          ORDERTYPE,
          ORDERCOMPLETEDATE,
          PROPERTYVALUE,
          SERVICEREQUESTID,
          SERVICEREQUESTSTAGE,
          VIN1,
          VIN2,
          VIN3,
          INVOICECOMMENT,
          DATEBKFILED,
          FCSALERESULT,
          WRITISSUED,
          WRITISSUEDDATE,
          WRITHELDTYPE,
          MFRFILEDDATE,
          DILFILEDDATE,
          ASSETNUMBER,
          PARENTSERVICERCODE,
          REFERRALTYPEID,
          P_ID
     FROM BOA_LPS_HEADER_EXT;

      SQLCOUNTS  := SQL%ROWCOUNT;
      MSG  := P_FILENAME||' LPS FILE LOADED into BOA_LPS_HEADER';

      COMMIT;

--------------------------
-- UPDATE LOG
-------------------------
    UPDATE BOFA_FILES_PROCESSED
    SET RECORDCNT = SQLCOUNTS, COMMENTS = MSG
    WHERE PID  = P_ID;

    COMMIT;

    INSERT INTO LPS_HEADER_TOSEND(PID, CNT)
    VALUES ( P_ID, SQLCOUNTS);
    COMMIT;

    INSERT INTO SGP_PID_CLEANUP ( PID, TABLE_NAME, COMPLETED, ENTRYDATE)
    VALUES ( P_ID, 'BOA_LPS_HEADER', 0, SYSDATE);
    COMMIT;

  INSERT INTO BOA_PROCESS_LOG
  (
    PROCESS,
    SUB_PROCESS,
    ENTRYDTE,
    ROWCOUNTS,
    MESSAGE
  )
  VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG);

  COMMIT;

  /************
    THEE END
   ***********/

   MSG       := P_FILENAME||':Job Complete';

  sqlCounts := 0;

  INSERT INTO BOA_PROCESS_LOG
  (
    PROCESS,
    SUB_PROCESS,
    ENTRYDTE,
    ROWCOUNTS,
    MESSAGE
  )
  VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG);

  COMMIT;

  P_RCODE := 0;

  /******************************
     Record any errors
   *****************************/

  exception
        WHEN  BAD_DATA THEN
        P_RCODE := RCODE;
        INSERT INTO BOA_PROCESS_LOG
        (
          PROCESS,
          SUB_PROCESS,
          ENTRYDTE,
          ROWCOUNTS,
          MESSAGE
        )
        VALUES ( proc, SubProc,SYSDATE, rcode, P_FILENAME||': '||MSG);

        COMMIT;

      WHEN OTHERS THEN

       MSG  := SQLERRM;
       P_RCODE := SQLCODE;
         RCODE := SQLCODE;
  INSERT INTO BOA_PROCESS_LOG
  (
    PROCESS,
    SUB_PROCESS,
    ENTRYDTE,
    ROWCOUNTS,
    MESSAGE
  )
  VALUES ( proc, SubProc,SYSDATE, RCODE, P_FILENAME||': '||MSG);

  COMMIT;

  END;

  procedure  LOAD_SVC_RELEASE(P_FILENAME IN VARCHAR2 , P_ID IN  NUMBER, P_MESSAGE IN VARCHAR2, P_RCODE OUT NUMBER)
   IS

    SQLCOUNTS  PLS_INTEGER;
    MSG        VARCHAR2(1000);
    PROC       VARCHAR2(1000);
    SubProc    VARCHAR2(1000);
    rcode      NUMBER;

    sql_stmt   VARCHAR2(32000);
    whoami     varchar2(100);
    BAD_DATA   EXCEPTION;

  BEGIN


    SQLCOUNTS  := 0;
    MSG        := 'Job Starting';
    PROC       := 'BOA_FEES_VALIDATE';
    SubProc    := 'LOAD_SVC_RELEASE';
    vTEAM      := 'BOA IRecon';
  INSERT INTO BOA_PROCESS_LOG
  (
    PROCESS,
    SUB_PROCESS,
    ENTRYDTE,
    ROWCOUNTS,
    MESSAGE
  )
  VALUES ( proc,SubProc,SYSDATE, sqlcounts, MSG);

  COMMIT;

   If ( p_id < 0 )
     then
       MSG :=   'an Error occured while attempt to log the file name ';
       rcode := -6502;
       RAISE BAD_DATA;
   END IF;

      SQL_STMT :=   CASE  WHEN P_MESSAGE = 'BOA_SVC_REL_DIR'   THEN  'ALTER TABLE BOA_SVC_REL_EXT  LOCATION (BOA_SVC_REL_DIR:'''||P_FILENAME||''') '
                    ELSE  'UNKNOWN-WORKING-DIRECTORY'
                   END;

  IF (SQL_STMT IN ('UNKNOWN-WORKING-DIRECTORY') )
     THEN
     MSG   := SQL_STMT;
     RCODE := -6502;
     RAISE BAD_DATA;
  END IF;

        EXECUTE IMMEDIATE SQL_STMT;


        INSERT INTO BOA_SVC_REL (ACCOUNTID, SRLID,SRLDate,NewServicer,Deregister,PID)
        SELECT ACCOUNTID,SRLID,SRLDate,NewServicer,Deregister, P_ID
        FROM BOA_SVC_REL_EXT;

        SQLCOUNTS  := SQL%ROWCOUNT;
        MSG  := P_FILENAME||' Service release LOADED into BOA_SVC_REL';

        COMMIT;

--------------------------
-- UPDATE LOG
-------------------------
    UPDATE BOFA_FILES_PROCESSED
    SET RECORDCNT = SQLCOUNTS, COMMENTS = MSG
    WHERE PID  = P_ID;

    COMMIT;

    INSERT INTO SVC_REL_TOSEND(PID, CNT)
    VALUES ( P_ID, SQLCOUNTS);
    COMMIT;

    INSERT INTO SGP_PID_CLEANUP ( PID, TABLE_NAME, COMPLETED, ENTRYDATE)
    VALUES ( P_ID, 'BOA_SVC_REL', 0, SYSDATE);
    COMMIT;

  INSERT INTO BOA_PROCESS_LOG
  (
    PROCESS,
    SUB_PROCESS,
    ENTRYDTE,
    ROWCOUNTS,
    MESSAGE
  )
  VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG);

  COMMIT;

  /************
    THEE END
   ***********/

   MSG       := P_FILENAME||':Job Complete';

  sqlCounts := 0;

  INSERT INTO BOA_PROCESS_LOG
  (
    PROCESS,
    SUB_PROCESS,
    ENTRYDTE,
    ROWCOUNTS,
    MESSAGE
  )
  VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG);

  COMMIT;

  P_RCODE := 0;

  /******************************
     Record any errors
   *****************************/

  exception
        WHEN  BAD_DATA THEN
        P_RCODE := RCODE;
        INSERT INTO BOA_PROCESS_LOG
        (
          PROCESS,
          SUB_PROCESS,
          ENTRYDTE,
          ROWCOUNTS,
          MESSAGE
        )
        VALUES ( proc, SubProc,SYSDATE, rcode, P_FILENAME||': '||MSG);

        COMMIT;

          SEND_EMAIL  ( vTEAM, proc||'.'||subProc, 'Message From Load process', MSG);

      WHEN OTHERS THEN

       MSG  := SQLERRM;
       P_RCODE := SQLCODE;
         RCODE := SQLCODE;
      INSERT INTO BOA_PROCESS_LOG
      (
        PROCESS,
        SUB_PROCESS,
        ENTRYDTE,
        ROWCOUNTS,
        MESSAGE
      )
      VALUES ( proc, SubProc,SYSDATE, RCODE, P_FILENAME||': '||MSG);

      COMMIT;

      SEND_EMAIL  ( vTEAM, proc||'.'||subProc, 'Message From Load process', MSG);

  END;
----          LOSS_ANALYSIS_FILES(Q.FILE_NAME(j),          P_OUT,           P_MESSAGE,              rCODE ,                       P_ISSUE)
   procedure  LOSS_ANALYSIS_FILES(P_FILENAME IN VARCHAR2 , P_ID IN  NUMBER, P_MESSAGE IN VARCHAR2, P_RCODE OUT NUMBER, P_ISSUE OUT VARCHAR2)
   IS
    SQLCOUNTS  PLS_INTEGER;
    MSG        VARCHAR2(1000);
    MSG2       VARCHAR2(1000);
    PROC       VARCHAR2(1000);
    SubProc    VARCHAR2(1000);
    rcode      NUMBER;
    CNT        NUMBER;

    SQL_STMT    VARCHAR2(32000);
    whoami      varchar2(100);
    ISSUE       VARCHAR2(100);
    max_load_id   number;
    min_load_id   number;
    file_type     number;
      BAD_DATA   EXCEPTION;

BEGIN


    SQLCOUNTS  := 0;
    MSG        := 'Job Starting';

    PROC       := 'BOA_FEES_VALIDATE';
    SubProc    := 'LOSS_ANALYSIS';
    ISSUE      := 'NONE';
    vTEAM      := 'RDM';
    MSG2       := 'Just Started';



  INSERT INTO BOA_PROCESS_LOG
  (
    PROCESS,
    SUB_PROCESS,
    ENTRYDTE,
    ROWCOUNTS,
    MESSAGE
  )
  VALUES ( proc,SubProc,SYSDATE, sqlcounts, MSG);

  COMMIT;

      If ( P_ID < 0 )
        then
        MSG :=   'an Error occured while attempt to log the file name ';
        rcode := -6502;
        RAISE BAD_DATA;
      END IF;

      MSG2 := MSG2||', Logged file';

---IR_Assignment
--- Pending-Approval_05242016.csv                                 LA_PENDING_APPROVAL_EXT
--- OutAdj_20160802.xlsx.csv
      SQL_STMT :=   CASE  WHEN P_MESSAGE = 'SGP_LA_FILES'  AND SUBSTR(upper(P_FILENAME),1,17) IN ('RESOLUTIONPENDING')  THEN  'ALTER TABLE LA_RESOLUTION_PENDING_EXT  LOCATION (SGP_LA_FILES:'''||P_FILENAME||''') '
                          WHEN P_MESSAGE = 'SGP_LA_FILES'  AND SUBSTR(upper(P_FILENAME),1,13) IN ('PENDREJECTION')      THEN  'ALTER TABLE LA_PENDING_REJECTIONS_EXT  LOCATION (SGP_LA_FILES:'''||P_FILENAME||''') '
                          WHEN P_MESSAGE = 'SGP_LA_FILES'  AND SUBSTR(upper(P_FILENAME),1,12) IN ('PENDAPPROVAL')       THEN  'ALTER TABLE LA_PENDING_APPROVAL_EXT    LOCATION (SGP_LA_FILES:'''||P_FILENAME||''') '
                          WHEN P_MESSAGE = 'SGP_LA_FILES'  AND SUBSTR(upper(P_FILENAME),1,6)  IN ('OUTADJ')             THEN  'ALTER TABLE LA_OUTSTANDING_ADJUSTED_EXT  LOCATION (SGP_LA_FILES:'''||P_FILENAME||''') '
                          WHEN P_MESSAGE = 'SGP_LA_FILES'  AND SUBSTR(upper(P_FILENAME),5,13) IN ('IR_ASSIGNMENT')      THEN  'ALTER TABLE LA_IR_ASSIGNMENT_EXT  LOCATION (SGP_LA_FILES:'''||P_FILENAME||''') '

                    ELSE  'UNKNOWN-WORKING-DIRECTORY'
                   END;

      IF (SQL_STMT IN ('UNKNOWN-WORKING-DIRECTORY') )
          THEN
          MSG   := SQL_STMT;
          RCODE := -6502;
          RAISE BAD_DATA;
      END IF;

        EXECUTE IMMEDIATE SQL_STMT;

        MSG2 := MSG2||', The Directory Checks out';

         file_type := case when SUBSTR(upper(P_FILENAME),1,17) IN ('RESOLUTIONPENDING')   then 2
                           when SUBSTR(upper(P_FILENAME),1,19) IN ('RESOLUTIONPENDINGCC') then 9
                           when SUBSTR(upper(P_FILENAME),1,13) IN ('PENDREJECTION')       then 4
                           when SUBSTR(upper(P_FILENAME),1,12) IN ('PENDAPPROVAL')        then 3
                           when SUBSTR(upper(P_FILENAME),1,8)  IN  ('OUTADJGC')           then 8
                           when SUBSTR(upper(P_FILENAME),1,6)  IN  ('OUTADJ')             then 1
                           when SUBSTR(upper(P_FILENAME),5,13) IN  ('IR_ASSIGNMENT')      then 5

                      else -1 end;


         IF ( file_type in (2,9) )
             THEN
                INSERT INTO LA_RESOLUTION_PENDING (INVOICE_NBR,ORDER_NBR,VENDOR_CONTACT,LOAN_NBR,INVOICE_DATE,DEPT,STATE,RESOLUTION_TYPE,INVOICE_AMT,VENDOR_COMMENT,VENDOR_DATE,CLIENT_COMMENT,CLIENT_DATE,REASON,RESOLUTION_DEADLINE,CURTAIL_DATE,PID)
                SELECT INVOICE_NBR,ORDER_NBR,VENDOR_CONTACT,LOAN_NBR,INVOICE_DATE,DEPT,STATE,RESOLUTION_TYPE,INVOICE_AMT,VENDOR_COMMENT,VENDOR_DATE,CLIENT_COMMENT,CLIENT_DATE,REASON,RESOLUTION_DEADLINE,CURTAIL_DATE,P_ID
                FROM LA_RESOLUTION_PENDING_EXT;

                    SQLCOUNTS := SQL%ROWCOUNT;

                    COMMIT;
                    SELECT MIN(LOAD_ID), MAX(LOAD_ID)
                    INTO   min_load_id,  max_load_id
                    from   LA_RESOLUTION_PENDING
                    where  PID = P_ID;

                    SQL_STMT := ' INSERT INTO RDM.LA_FILES_LOADED_LIST@'||vDB_LINK||'(PID, FILE_TYPE, FILE_NAME,  LOAD_DATE, LOADED_BY, RECORDCNT, LOADED, BEGIN_LOAD_ID, END_LOAD_ID,PROCESS_NO) ';
                    SQL_STMT := SQL_STMT||' SELECT PID, '||file_type||', FILE_NAME, ENTRY_DATE,''BOA IRecon'', RECORDCNT, 0, '||min_load_id||', '||max_load_id||', 1';
                    SQL_STMT := SQL_STMT||' FROM BOFA_FILES_PROCESSED ';
                    SQL_STMT := SQL_STMT||' WHERE PID = :A';

                    EXECUTE IMMEDIATE SQL_STMT USING P_ID;

                    COMMIT;

                    MSG2 := MSG2||', File NAME sent to RDM';

                    INSERT INTO SGP_ResPend_TO_SEND(PID, CNT)
                    VALUES ( P_ID, SQLCOUNTS);
                    COMMIT;

                    INSERT INTO SGP_PID_CLEANUP ( PID, TABLE_NAME, COMPLETED, ENTRYDATE)
                    VALUES ( P_ID, 'LA_RESOLUTION_PENDING', 0, SYSDATE);
                    COMMIT;

                    INSERT INTO BOA_PROCESS_LOG
                    (
                      PROCESS,
                      SUB_PROCESS,
                      ENTRYDTE,
                      ROWCOUNTS,
                      MESSAGE
                    )
                    VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG2);

                   COMMIT;

                  RCODE := 0;

         END IF;


         IF ( file_type in (4) )
             THEN
                INSERT INTO LA_PENDING_REJECTIONS (CLIENT,SERVICER,LOAN_NBR,INVOICE_TYPE,INVOICE_DATE,INVOICE_NBR,ORDER_NBR,STATE,SERVICER_COMMENTS,COMMENTS_DATE,EXPIRES_IN,INVOICE_AMT,FILE_ID)
                SELECT CLIENT,SERVICER,LOAN_NBR,INVOICE_TYPE,INVOICE_DATE,INVOICE_NBR,ORDER_NBR,STATE,SERVICER_COMMENTS,COMMENTS_DATE,EXPIRES_IN,INVOICE_AMT,P_ID
                FROM LA_PENDING_REJECTIONS_EXT;

                    SQLCOUNTS := SQL%ROWCOUNT;

                    COMMIT;

                    SELECT MIN(LOAD_ID), MAX(LOAD_ID)
                    INTO   min_load_id,  max_load_id
                    from   LA_PENDING_REJECTIONS
                    where  FILE_ID = P_ID;

                    SQL_STMT := ' INSERT INTO RDM.LA_FILES_LOADED_LIST@'||vDB_LINK||'(PID, FILE_TYPE, FILE_NAME,  LOAD_DATE, LOADED_BY, RECORDCNT, LOADED, BEGIN_LOAD_ID, END_LOAD_ID,PROCESS_NO) ';
                    SQL_STMT := SQL_STMT||' SELECT PID, '||file_type||', FILE_NAME, ENTRY_DATE,''BOA IRecon'', RECORDCNT, 0, '||min_load_id||', '||max_load_id||', 1';
                    SQL_STMT := SQL_STMT||' FROM BOFA_FILES_PROCESSED ';
                    SQL_STMT := SQL_STMT||' WHERE PID = :A';

                    EXECUTE IMMEDIATE SQL_STMT USING P_ID;

                    MSG2 := MSG2||', File NAME sent to RDM';


                   INSERT INTO LA_PEND_REJS_TO_SEND(PID, CNT)
                   VALUES ( P_ID, SQLCOUNTS);
                   COMMIT;

                    INSERT INTO SGP_PID_CLEANUP ( PID, TABLE_NAME, COMPLETED, ENTRYDATE)
                    VALUES ( P_ID, 'LA_PENDING_REJECTIONS', 0, SYSDATE);
                    COMMIT;

                    INSERT INTO BOA_PROCESS_LOG
                    (
                      PROCESS,
                      SUB_PROCESS,
                      ENTRYDTE,
                      ROWCOUNTS,
                      MESSAGE
                    )
                    VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG2);

                    COMMIT;

         END IF;

         IF ( file_type in (3))
             THEN
                INSERT INTO LA_PENDING_APPROVAL (SERVICER,LOAN_NBR,INVOICE_TYPE,INVOICE_DATE,INVOICE_NBR,STATE,LI_CODE,LI_DESCRIPTION,LI_AMT,ADJUSTED_AMT,TOTAL_LI_AMOUNT,REASON,COMMENTS_DATE,EXPIRES_IN,FILE_ID)
                SELECT SERVICER,LOAN_NBR,INVOICE_TYPE,INVOICE_DATE,INVOICE_NBR,STATE,LI_CODE,LI_DESCRIPTION,LI_AMT,ADJUSTED_AMT,TOTAL_LI_AMOUNT,REASON,COMMENTS_DATE,EXPIRES_IN,P_ID
                FROM LA_PENDING_APPROVAL_EXT;

                    SQLCOUNTS := SQL%ROWCOUNT;

                    COMMIT;

                    SELECT MIN(LOAD_ID), MAX(LOAD_ID)
                    INTO   min_load_id,  max_load_id
                    from   LA_PENDING_APPROVAL
                    where  FILE_ID = P_ID;

                    SQL_STMT := ' INSERT INTO RDM.LA_FILES_LOADED_LIST@'||vDB_LINK||'(PID, FILE_TYPE, FILE_NAME,  LOAD_DATE, LOADED_BY, RECORDCNT, LOADED, BEGIN_LOAD_ID, END_LOAD_ID,PROCESS_NO) ';
                    SQL_STMT := SQL_STMT||' SELECT PID, '||file_type||', FILE_NAME, ENTRY_DATE,''BOA IRecon'', RECORDCNT, 0, '||min_load_id||', '||max_load_id||', 1';
                    SQL_STMT := SQL_STMT||' FROM BOFA_FILES_PROCESSED ';
                    SQL_STMT := SQL_STMT||' WHERE PID = :A';

                    EXECUTE IMMEDIATE SQL_STMT USING P_ID;

                    MSG2 := MSG2||', File NAME sent to RDM';


                    INSERT INTO LA_PEND_APPROV_TO_SEND(PID, CNT)
                    VALUES ( P_ID, SQLCOUNTS);
                    COMMIT;

                    INSERT INTO SGP_PID_CLEANUP ( PID, TABLE_NAME, COMPLETED, ENTRYDATE)
                    VALUES ( P_ID, 'LA_PENDING_APPROVAL', 0, SYSDATE);
                    COMMIT;

                    INSERT INTO BOA_PROCESS_LOG
                    (
                      PROCESS,
                      SUB_PROCESS,
                      ENTRYDTE,
                      ROWCOUNTS,
                      MESSAGE
                    )
                    VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG2);

                   COMMIT;

         END IF;


         IF ( file_type in (1,8) )
             THEN
                INSERT INTO LA_OUTSTANDING_ADJUSTED (INVOICE_NBR,VENDOR,VENDOR_CONTACT,LOAN_NBR,INVOICE_DATE,DEPT,STATE,BORROWER,ADJUSTED_TOTAL,EARLIEST_ADJ_DT,CURTAIL_DATE,PID )
                SELECT INVOICE_NBR,VENDOR,VENDOR_CONTACT,LOAN_NBR,INVOICE_DATE,DEPT,STATE,BORROWER,ADJUSTED_TOTAL,EARLIEST_ADJ_DT,CURTAIL_DATE, P_ID
                FROM LA_OUTSTANDING_ADJUSTED_EXT;

                    SQLCOUNTS := SQL%ROWCOUNT;

                    COMMIT;

                    SELECT MIN(LOAD_ID), MAX(LOAD_ID)
                    INTO   min_load_id,  max_load_id
                    from   LA_OUTSTANDING_ADJUSTED
                    where  PID = P_ID;

                    SQL_STMT := ' INSERT INTO RDM.LA_FILES_LOADED_LIST@'||vDB_LINK||'(PID, FILE_TYPE, FILE_NAME,  LOAD_DATE, LOADED_BY, RECORDCNT, LOADED, BEGIN_LOAD_ID, END_LOAD_ID,PROCESS_NO) ';
                    SQL_STMT := SQL_STMT||' SELECT PID, '||file_type||', FILE_NAME, ENTRY_DATE,''BOA IRecon'', RECORDCNT, 0, '||min_load_id||', '||max_load_id||', 1';
                    SQL_STMT := SQL_STMT||' FROM BOFA_FILES_PROCESSED ';
                    SQL_STMT := SQL_STMT||' WHERE PID = :A';

                    EXECUTE IMMEDIATE SQL_STMT USING P_ID;

                    MSG2 := MSG2||', File NAME sent to RDM';


                    INSERT INTO LA_OUT_ADJUST_TO_SEND(PID, CNT)
                    VALUES ( P_ID, SQLCOUNTS);
                    COMMIT;

                    INSERT INTO SGP_PID_CLEANUP ( PID, TABLE_NAME, COMPLETED, ENTRYDATE)
                    VALUES ( P_ID, 'LA_OUTSTANDING_ADJUSTED', 0, SYSDATE);
                    COMMIT;

                    INSERT INTO BOA_PROCESS_LOG
                    (
                      PROCESS,
                      SUB_PROCESS,
                      ENTRYDTE,
                      ROWCOUNTS,
                      MESSAGE
                    )
                    VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG2);

                    COMMIT;



         END IF;



         IF ( file_type in (5) )
             THEN
                INSERT INTO LA_IR_ASSIGNMENT (RECEIVED_DATE,LAST_UPDATED,CLIENT,COL_D,INVOICE_NBR,INVOICE_DATE,WORK_ORDER_NBR,LOAN_NBR,COL_I,DISPUTE_AMT,COL_K,CLIENT_COMMENT,COL_M,LOSS_ANALYST,WRITE_OFF_AMOUNT,WRITE_OFF,WRITE_OFF_REASON_CODE,WRITE_OFF_REASON,DONE_BILLING_CODE,
                            VENDOR_CODE,CHARGEBACK_AMOUNT,DISPUTE_TYPE,SOURCE_OF_DISPUTE,COL_X,APPROVAL,PENDING_RESEARCH,DISPUTE_APPEAL_COMMENT,IM_ICLEAR_INV,CLIENT_EMPLOYEE,FILE_ID)
                SELECT  RECEIVED_DATE,LAST_UPDATED,CLIENT,COL_D,INVOICE_NBR,INVOICE_DATE,WORK_ORDER_NBR,LOAN_NBR,COL_I,DISPUTE_AMT,COL_K,CLIENT_COMMENT,COL_M,LOSS_ANALYST,WRITE_OFF_AMOUNT,WRITE_OFF,WRITE_OFF_REASON_CODE,WRITE_OFF_REASON,DONE_BILLING_CODE,
                        VENDOR_CODE,CHARGEBACK_AMOUNT,DISPUTE_TYPE,SOURCE_OF_DISPUTE,COL_X,APPROVAL,PENDING_RESEARCH,DISPUTE_APPEAL_COMMENT,IM_ICLEAR_INV,CLIENT_EMPLOYEE,P_ID
                FROM LA_IR_ASSIGNMENT_EXT;

                SQLCOUNTS := SQL%ROWCOUNT;

                    COMMIT;

                    SELECT MIN(LOAD_ID), MAX(LOAD_ID)
                    INTO   min_load_id,  max_load_id
                    from   LA_IR_ASSIGNMENT
                    where  FILE_ID = P_ID;

                    SQL_STMT := ' INSERT INTO RDM.LA_FILES_LOADED_LIST@'||vDB_LINK||'(PID, FILE_TYPE, FILE_NAME,  LOAD_DATE, LOADED_BY, RECORDCNT, LOADED, BEGIN_LOAD_ID, END_LOAD_ID,PROCESS_NO) ';
                    SQL_STMT := SQL_STMT||' SELECT PID, 5, FILE_NAME, ENTRY_DATE,''BOA IRecon'', RECORDCNT, 0, '||min_load_id||', '||max_load_id||', 1';
                    SQL_STMT := SQL_STMT||' FROM BOFA_FILES_PROCESSED ';
                    SQL_STMT := SQL_STMT||' WHERE PID = :A';

                    EXECUTE IMMEDIATE SQL_STMT USING P_ID;

                    MSG2 := MSG2||', File NAME sent to RDM';


                    INSERT INTO LA_IR_ASSIGN_TO_SEND(PID, CNT)
                    VALUES ( P_ID, SQLCOUNTS);
                    COMMIT;

                    INSERT INTO SGP_PID_CLEANUP ( PID, TABLE_NAME, COMPLETED, ENTRYDATE)
                    VALUES ( P_ID, 'LA_IR_ASSIGNMENT', 0, SYSDATE);
                    COMMIT;

                    INSERT INTO BOA_PROCESS_LOG
                    (
                      PROCESS,
                      SUB_PROCESS,
                      ENTRYDTE,
                      ROWCOUNTS,
                      MESSAGE
                    )
                    VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG2);

                    COMMIT;

         END IF;



        COMMIT;

        MSG2 := MSG2||', The File count is good';

--------------------------
-- UPDATE LOG
-------------------------

    UPDATE BOFA_FILES_PROCESSED
    SET RECORDCNT = SQLCOUNTS, COMMENTS = 'LA FILE COUNTED'
    WHERE PID  = P_ID;

    COMMIT;



/* LA_FILES_LOADED_LIST
   PID            NUMBER,
  FILE_TYPE      NUMBER,
  FILE_NAME      VARCHAR2(1000 BYTE),
  RECORDCNT      NUMBER,
  LOAD_DATE      DATE,
  LOADED_BY      VARCHAR2(100 BYTE),
  COMMENTS       VARCHAR2(1000 BYTE),
  BEGIN_LOAD_ID  NUMBER,
  END_LOAD_ID    NUMBER,
  LOADED         NUMBER                         DEFAULT 0
)



*/

  /************
    THEE END
   ***********/

   MSG2       := P_FILENAME||': Job Complete';

  sqlCounts := 0;

  INSERT INTO BOA_PROCESS_LOG
  (
    PROCESS,
    SUB_PROCESS,
    ENTRYDTE,
    ROWCOUNTS,
    MESSAGE
  )
  VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG2);

  COMMIT;

  P_RCODE := 0;
  P_ISSUE := ISSUE;
  /******************************
     Record any errors
   *****************************/

  exception
        WHEN  BAD_DATA THEN
        P_RCODE := RCODE;
        P_ISSUE := 'BAD_DATA';
        INSERT INTO BOA_PROCESS_LOG
        (
          PROCESS,
          SUB_PROCESS,
          ENTRYDTE,
          ROWCOUNTS,
          MESSAGE
        )
        VALUES ( proc, SubProc,SYSDATE, rcode, P_FILENAME||': '||MSG2);

        COMMIT;

          SEND_EMAIL  ( vTEAM, proc||'.'||subProc, 'Message From Load process', MSG2);

      WHEN OTHERS THEN

       MSG  := SQLERRM;
         P_RCODE := SQLCODE;
         RCODE := SQLCODE;
         P_ISSUE := 'ORA-ERROR';
  INSERT INTO BOA_PROCESS_LOG
  (
    PROCESS,
    SUB_PROCESS,
    ENTRYDTE,
    ROWCOUNTS,
    MESSAGE
  )
  VALUES ( proc, SubProc,SYSDATE, RCODE, P_FILENAME||': '||MSG2);

  COMMIT;

  SEND_EMAIL  ( vTEAM, proc||'.'||subProc, 'Message From Load process', MSG2);

  END;

/***********************************************************************************
      DAISY CANCLE
 ************************************************************************************/
   procedure  LOAD_XCL_CHL(P_FILENAME IN VARCHAR2 , P_ID IN  NUMBER, P_MESSAGE IN VARCHAR2, P_RCODE OUT NUMBER, P_ISSUE OUT VARCHAR2)

   IS

    SQLCOUNTS  PLS_INTEGER;
    MSG        VARCHAR2(1000);
    MSG2       VARCHAR2(1000);
    PROC       VARCHAR2(1000);
    SubProc    VARCHAR2(1000);
    rcode      NUMBER;
    CNT        NUMBER;

    SD         XCL_SWEEP_TBL;
    SQL_STMT   VARCHAR2(32000);
    whoami     varchar2(100);
    ISSUE      VARCHAR2(100);
    BAD_DATA   EXCEPTION;
    GC         GenRefCursor;
  BEGIN


    SQLCOUNTS  := 0;
    MSG        := 'Job Starting';

    PROC       := 'BOA_FEES_VALIDATE';
    SubProc    := 'LOAD_XCL_CHL';
    ISSUE      := 'NONE';
    vTEAM      := 'SYS Cancel';

  INSERT INTO BOA_PROCESS_LOG
  (
    PROCESS,
    SUB_PROCESS,
    ENTRYDTE,
    ROWCOUNTS,
    MESSAGE
  )
  VALUES ( proc,SubProc,SYSDATE, sqlcounts, MSG);

  COMMIT;

   If ( p_id < 0 )
     then
       MSG :=   'an Error occured while attempt to log the file name ';
       rcode := -6502;
       RAISE BAD_DATA;
   END IF;

      SQL_STMT :=   CASE  WHEN P_MESSAGE = 'XCL_CHL_SWEEP_DIR'   THEN  'ALTER TABLE XCL_CHL_SWEEP_EXT  LOCATION (XCL_CHL_SWEEP_DIR:'''||P_FILENAME||''') '
                    ELSE  'UNKNOWN-WORKING-DIRECTORY'
                   END;

  IF (SQL_STMT IN ('UNKNOWN-WORKING-DIRECTORY') )
     THEN
     MSG   := SQL_STMT;
     RCODE := -6502;
     RAISE BAD_DATA;
  END IF;

        EXECUTE IMMEDIATE SQL_STMT;

/*
CREATE TABLE SPI_DAISY
(
  SHEETNAME   VARCHAR2(40 BYTE),
  ACCT        VARCHAR2(40 BYTE),
  CURRENT_    VARCHAR2(40 BYTE),
  CURRENTDTL  VARCHAR2(40 BYTE),
  RECACTION   VARCHAR2(40 BYTE),
  LOAD_DT     DATE

 */

          SQL_STMT := 'SELECT SHEETNAME, ACCT, CURRENT_, CURRENTDTL, RECACTION  FROM XCL_CHL_SWEEP_EXT ';
          SQL_STMT := SQL_STMT||' where upper(sheetname) like ''%SWEEP%'' ';
          OPEN GC FOR SQL_STMT ;
          FETCH GC BULK COLLECT INTO SD.SHEETNAME,
                                     SD.ACCT,
                                     SD.CURRENT_,
                                     SD.CURRENTDTL,
                                     SD.RECACTION;
               CNT := SD.ACCT.COUNT;

               IF (CNT = 0 ) THEN
                  ISSUE :=  'SWEEP-ISSUE';
                  SEND_EMAIL  ( vTEAM, proc||'.'||subProc, 'Message From Load process', 'A sweep issue, please check file for data issues');
               END IF;

         CLOSE GC;

-- TRIM( BOTH '"' from sheetname) sheetname
        INSERT INTO XCL_SWEEP (CLIENT_NAME, SHEETNAME, ACCT, CURRENT_, CURRENTDTL, RECACTION, LOAD_DT, PID)
        SELECT 'CHL' AS CLIENT_NAME, TRIM( BOTH '"' FROM SHEETNAME), TRIM( BOTH '"' FROM ACCT) , TRIM( BOTH '"' FROM CURRENT_), TRIM( BOTH '"' FROM CURRENTDTL), TRIM( BOTH '"' FROM RECACTION), TRUNC(SYSDATE) AS LOAD_DT, P_ID
        FROM XCL_CHL_SWEEP_EXT;

        SQLCOUNTS  := SQL%ROWCOUNT;

        MSG  := P_FILENAME||' XCL_CHL_SWEEP Loaded';

        COMMIT;

--------------------------
-- UPDATE LOG
-------------------------
    MSG2  := CASE WHEN ISSUE NOT IN ('NONE') THEN ISSUE
                 ELSE  MSG end;

    UPDATE BOFA_FILES_PROCESSED
    SET RECORDCNT = SQLCOUNTS, COMMENTS = MSG2
    WHERE PID  = P_ID;

    COMMIT;

    SQL_STMT := ' INSERT INTO RDM.XCL_PROCESS_LOG@'||vDB_LINK||'(PID, CLIENT_NAME, FILE_NAME,  ENTRY_DATE,  ROWCOUNTS, MESSAGE) ';
    SQL_STMT := SQL_STMT||' SELECT PID, CLIENT, FILE_NAME, ENTRY_DATE, RECORDCNT, COMMENTS ';
    SQL_STMT := SQL_STMT||' FROM BOFA_FILES_PROCESSED ';
    SQL_STMT := SQL_STMT||' WHERE PID = :A';

    EXECUTE IMMEDIATE SQL_STMT USING P_ID;

    INSERT INTO XCL_TOSEND(PID, CNT)
    VALUES ( P_ID, SQLCOUNTS);
    COMMIT;

    INSERT INTO SGP_PID_CLEANUP ( PID, TABLE_NAME, COMPLETED, ENTRYDATE)
    VALUES ( P_ID, 'XCL_SWEEP', 0, SYSDATE);
    COMMIT;

  INSERT INTO BOA_PROCESS_LOG
  (
    PROCESS,
    SUB_PROCESS,
    ENTRYDTE,
    ROWCOUNTS,
    MESSAGE
  )
  VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG2);

  COMMIT;

  /************
    THEE END
   ***********/

   MSG2       := MSG2||'-'||P_FILENAME||':Job Complete';

  sqlCounts := 0;

  INSERT INTO BOA_PROCESS_LOG
  (
    PROCESS,
    SUB_PROCESS,
    ENTRYDTE,
    ROWCOUNTS,
    MESSAGE
  )
  VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG2);

  COMMIT;

  P_RCODE := 0;
  P_ISSUE := ISSUE;
  /******************************
     Record any errors
   *****************************/

  exception
        WHEN  BAD_DATA THEN
        P_RCODE := RCODE;
        P_ISSUE := 'BAD_DATA';
        INSERT INTO BOA_PROCESS_LOG
        (
          PROCESS,
          SUB_PROCESS,
          ENTRYDTE,
          ROWCOUNTS,
          MESSAGE
        )
        VALUES ( proc, SubProc,SYSDATE, rcode, P_FILENAME||': '||MSG2);

        COMMIT;

          SEND_EMAIL  ( vTEAM, proc||'.'||subProc, 'Message From Load process', MSG2);

      WHEN OTHERS THEN

       MSG  := SQLERRM;
       P_RCODE := SQLCODE;
         RCODE := SQLCODE;
       P_ISSUE := 'ORA-ERROR';
  INSERT INTO BOA_PROCESS_LOG
  (
    PROCESS,
    SUB_PROCESS,
    ENTRYDTE,
    ROWCOUNTS,
    MESSAGE
  )
  VALUES ( proc, SubProc,SYSDATE, RCODE, P_FILENAME||': '||MSG2);

  COMMIT;

  SEND_EMAIL  ( vTEAM, proc||'.'||subProc, 'Message From Load process', MSG2);

  END;

/*************************************************************************************
   FILE LOAD ERRORS
 **************************************************************************************/

   procedure  LOAD_FILE_ERRORS(P_FILENAME IN VARCHAR2 , P_ID IN  NUMBER, P_MESSAGE IN VARCHAR2, P_RCODE OUT NUMBER)
   IS

    SQLCOUNTS  PLS_INTEGER;
    MSG        VARCHAR2(1000);
    PROC       VARCHAR2(1000);
    SubProc    VARCHAR2(1000);
    rcode      NUMBER;

    sql_stmt   VARCHAR2(32000);
    whoami     varchar2(100);
    BAD_DATA   EXCEPTION;

  BEGIN


    SQLCOUNTS  := 0;
    MSG        := 'Job Starting';
    PROC       := 'BOA_FEES_VALIDATE';
    SubProc    := 'LOAD_FILE_ERRORS';
    vTEAM      := 'RDM';

  INSERT INTO BOA_PROCESS_LOG
  (
    PROCESS,
    SUB_PROCESS,
    ENTRYDTE,
    ROWCOUNTS,
    MESSAGE
  )
  VALUES ( proc,SubProc,SYSDATE, sqlcounts, MSG);

  COMMIT;

   If ( p_id < 0 )
     then
       MSG :=   'an Error occured while attempt to log the file name ';
       rcode := -6502;
       RAISE BAD_DATA;
   END IF;

      SQL_STMT :=   CASE  WHEN P_MESSAGE = 'FILE_ERROR_DIR'   THEN  'ALTER TABLE FILE_ERRORS_EXT  LOCATION (FILE_ERROR_DIR:'''||P_FILENAME||''') '
                    ELSE  'UNKNOWN-WORKING-DIRECTORY'
                   END;

  IF (SQL_STMT IN ('UNKNOWN-WORKING-DIRECTORY') )
     THEN
     MSG   := SQL_STMT;
     RCODE := -6502;
     RAISE BAD_DATA;
  END IF;

        EXECUTE IMMEDIATE SQL_STMT;

/*
CREATE TABLE SPI_DAISY
(
  SHEETNAME   VARCHAR2(40 BYTE),
  ACCT        VARCHAR2(40 BYTE),
  CURRENT_    VARCHAR2(40 BYTE),
  CURRENTDTL  VARCHAR2(40 BYTE),
  RECACTION   VARCHAR2(40 BYTE),
  LOAD_DT     DATE

 */

        INSERT INTO FILE_ERRORS (PID, PROCESS, MESSAGE, LOAD_DT)
        SELECT P_ID, 'LOAD_FILE_ERRORS',  MESSAGE, SYSDATE
        FROM FILE_ERRORS_EXT;

        SQLCOUNTS  := SQL%ROWCOUNT;
        MSG  := P_FILENAME||' File errors Loaded';

        COMMIT;

--------------------------
-- UPDATE LOG
-------------------------
    UPDATE BOFA_FILES_PROCESSED
    SET RECORDCNT = SQLCOUNTS, COMMENTS = MSG
    WHERE PID  = P_ID;

    COMMIT;

    INSERT INTO FILE_ERRS_TOSEND(PID, CNT)
    VALUES ( P_ID, SQLCOUNTS);
    COMMIT;

    INSERT INTO SGP_PID_CLEANUP ( PID, TABLE_NAME, COMPLETED, ENTRYDATE)
    VALUES ( P_ID, 'FILE_ERRORS', 0, SYSDATE);
    COMMIT;

  INSERT INTO BOA_PROCESS_LOG
  (
    PROCESS,
    SUB_PROCESS,
    ENTRYDTE,
    ROWCOUNTS,
    MESSAGE
  )
  VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG);

  COMMIT;

  /************
    THEE END
   ***********/

   MSG       := P_FILENAME||':Job Complete';

  sqlCounts := 0;

  INSERT INTO BOA_PROCESS_LOG
  (
    PROCESS,
    SUB_PROCESS,
    ENTRYDTE,
    ROWCOUNTS,
    MESSAGE
  )
  VALUES ( proc, SubProc,SYSDATE, SQLCOUNTS, MSG);

  COMMIT;

  P_RCODE := 0;

  /******************************
     Record any errors
   *****************************/

  exception
        WHEN  BAD_DATA THEN
        P_RCODE := RCODE;
        INSERT INTO BOA_PROCESS_LOG
        (
          PROCESS,
          SUB_PROCESS,
          ENTRYDTE,
          ROWCOUNTS,
          MESSAGE
        )
        VALUES ( proc, SubProc,SYSDATE, rcode, P_FILENAME||': '||MSG);

        COMMIT;

      SEND_EMAIL  ( vTEAM, SubProc, 'FILE_ERRORS',  MSG);

      WHEN OTHERS THEN

       MSG  := SQLERRM;
       P_RCODE := SQLCODE;
         RCODE := SQLCODE;
      INSERT INTO BOA_PROCESS_LOG
      (
        PROCESS,
        SUB_PROCESS,
        ENTRYDTE,
        ROWCOUNTS,
        MESSAGE
      )
      VALUES ( proc, SubProc,SYSDATE, RCODE, P_FILENAME||': '||MSG);

      COMMIT;

      SEND_EMAIL  ( vTEAM, SubProc, 'FILE_ERRORS',  MSG);

  END;

/*******************************************************

 *******************************************************/
  PROCEDURE SEND_EMAIL  ( P_TEAM VARCHAR2, P_FROM VARCHAR2, P_SUBJECT VARCHAR2,  P_MESSAGE VARCHAR2)
  IS

  L_BODY      VARCHAR2(1000);
  L_BODY_HTML VARCHAR2(32000);
  SQL_STMT    VARCHAR2(32000);

  BEGIN

      l_body := 'To view the content of this message, please use an HTML enabled mail client.'||utl_tcp.crlf;

    l_body_html := '<html>';
    l_body_html := l_body_html||'<head>';
    l_body_html := l_body_html||'<style type="text/css"> ';
    l_body_html := l_body_html||'body{font-family: Arial, Helvetica, sans-serif;';
    l_body_html := l_body_html||'font-size:10pt;';
    l_body_html := l_body_html||'margin:30px;';
    l_body_html := l_body_html||'background-color:#ffffff;} ';
    l_body_html := l_body_html||' ';
    l_body_html := l_body_html||'span.sig{font-style:italic; ';
    l_body_html := l_body_html||'   font-weight:bold; ';
    l_body_html := l_body_html||'   color:#811919;} ';
    l_body_html := l_body_html||'</style>';
    l_body_html := l_body_html||'</head> ';
    l_body_html := l_body_html||'</html>'||utl_tcp.crlf;

    l_body_html := l_body_html||'<body>';
    l_body_html := l_body_html||' <h3>Message from RDM Email Process</h3> ';
    l_body_html := l_body_html ||'<p>'||P_MESSAGE||'</p>'||utl_tcp.crlf;
    l_body_html := l_body_html ||'<br /><br /><br />'||utl_tcp.crlf;
    l_body_html := l_body_html ||'  Sincerely,<br />';
    l_body_html := l_body_html ||'  <span class="sig">The '||P_FROM||'</span><br />'||utl_tcp.crlf;
    l_body_html := l_body_html||'</body>'||utl_tcp.crlf||utl_tcp.crlf;

    SQL_STMT := 'INSERT INTO RDM.RDM_EMAIL_OUTBOX@'||vDB_LINK||' ( TEAM, APP, SUBJECT, EBODY, EBODY_HTML)';
    SQL_STMT := SQL_STMT||' VALUES ( :1, :2, :3, :4, :5) ' ;

    EXECUTE IMMEDIATE SQL_STMT USING  P_TEAM,P_FROM,P_SUBJECT,l_body,l_body_html;
    commit;

END;
/********************************************************************
   validate numbers
  *******************************************************************/

  FUNCTION validate_no (p_number VARCHAR2) RETURN NUMBER
  IS

    retval  PLS_INTEGER;

    BEGIN



    retval := CASE   WHEN p_number IS NULL THEN 0
                     WHEN LENGTH(TRIM(TRANSLATE(p_number, ' 0123456789',' '))) is not null THEN 0
                     WHEN TRIM(TRANSLATE(p_number, ' 0123456789',' ')) IS NULL THEN 1
                     END;

    RETURN retval;


    EXCEPTION
         WHEN OTHERS THEN
              retval := SQLCODE;
  END;

  PROCEDURE XCL_CHL_RESULTS_FILE
  IS

    SQL_STMT1      VARCHAR2(32000);
    SQL_STMT2      VARCHAR2(32000);
    UPD_STMT       VARCHAR2(32000);
    LINEOUT        VARCHAR2(200);
    PROC           VARCHAR2(100);
    SubProc        VARCHAR2(100);
    RCODE          NUMBER;
    FILENAME       VARCHAR2(100);
    MSG            VARCHAR2(300);
    V_BATCH_DATE   DATE;

    f             UTL_FILE.FILE_TYPE;
    mv            UTL_FILE.FILE_TYPE;
    R             XCL_RESULTS_FILES;
    L             XCL_LAYOUT;
    GC1           GenRefCursor;
    GC2           GenRefCursor;
    BAD_NEWS      EXCEPTION;

  BEGIN
          PROC      := 'XCL CHL';
          SubProc   := 'RESULTS_FILE';
          vTEAM     := 'SYS Cancel';

          SQL_STMT1 := 'select   MAX(BATCH_DATE)  from rdm.XCL_RESULTS_FILES@'||vDB_LINK||' WHERE RESULT_FILE_SENT IS NULL AND CLIENT_NAME = ''CHL'' ';

        EXECUTE IMMEDIATE SQL_STMT1 INTO V_BATCH_DATE;

          IF  ( V_BATCH_DATE IS NULL ) THEN
              SEND_EMAIL  ( vTEAM, proc||':'||SubProc,'RDM.XCL.Create_InspectionResults@'||vDB_LINK , 'Did not run today when expected');
              RAISE BAD_NEWS;
          END IF;

          IF  (TRUNC(V_BATCH_DATE) != TRUNC(SYSDATE) ) THEN
              SEND_EMAIL  ( vTEAM,PROC||':'||SubProc,'RDM.XCL.Create_InspectionResults@'||vDB_LINK , 'Did not run today when expected');
              RAISE BAD_NEWS;
          END IF;


          mv := UTL_FILE.FOPEN('XCL_CHL_OUT_DIR', 'move_results.txt', 'w');

          UPD_STMT  := 'UPDATE RDM.XCL_RESULTS_FILES@'||vDB_LINK||' SET RESULT_FILE_SENT = ''YES'' WHERE PID = :P';

          SQL_STMT1 := 'select  PID, BATCH_DATE, RESULT_FILE_SENT, RESULT_FILE_SIZE, RESULT_FILE_NAME  from rdm.XCL_RESULTS_FILES@'||vDB_LINK||' WHERE RESULT_FILE_SENT IS NULL AND CLIENT_NAME = ''CHL'' ORDER BY BATCH_DATE';
          open gc1 for SQL_STMT1;
          loop
             FETCH GC1 BULK COLLECT INTO r.pid,
                                         r.batch_date,
                                         r.result_file_sent,
                                         r.result_file_size,
                                         r.result_file_name
                                         limit 1;
            exit when r.pid.count = 0;
                for x in 1..r.pid.count loop
                      f := UTL_FILE.FOPEN('XCL_CHL_OUT_DIR', r.result_file_name(x), 'w');

                       FILENAME := r.result_file_name(x);


                        SQL_STMT2 := 'select A.PID, A.ORDNUM, A.DETID, A.LNUM, A.PROCESS_DT, A.DELIMITERS_57, A.CANCELED, A.DELIMITERS_6 from RDM.XCL_LAYOUT@'||vDB_LINK||' A ';
                        SQL_STMT2 := SQL_STMT2||' left join ( select results_pid, pid from RDM.XCL_tracking@'||vDB_LINK||' ) t  on ( a.pid = t.pid) ';
                        SQL_STMT2 := SQL_STMT2||' left join  ( select pid from RDM.XCL_RESULTS_FILES@'||vDB_LINK||' )  f on ( f.pid = T.RESULTS_PID) ';
                        SQL_STMT2 := SQL_STMT2||' where t.results_pid = f.pid ';
                        SQL_STMT2 := SQL_STMT2||' and a.pid = t.pid';
                        SQL_STMT2 := SQL_STMT2||' AND F.PID = :4';

                         open gc2 for SQL_STMT2 using r.pid(x);
                          loop
                              FETCH GC2 BULK COLLECT INTO L.PID,
                                                          L.ORDNUM,
                                                          L.DETID,
                                                          L.LNUM,
                                                          L.PROCESS_DT,
                                                          L.DELIMITERS_57,
                                                          L.CANCELED,
                                                          L.DELIMITERS_6
                                                          limit 1000;
                            exit when L.pid.count = 0;
                            for y in 1..L.pid.count loop
                                LINEOUT :=  L.ORDNUM(y)||'|'||L.DETID(y)||'|'||L.LNUM(y)||'|'||L.Process_DT(y)||'||'||L.Process_DT(y)||'|'||L.delimiters_57(y)||'|'||L.CANCELED(y)||'|'||L.delimiters_6(y);
                                UTL_FILE.PUT_LINE(f,lineout);
                            end loop;
                                LINEOUT := r.result_file_size(x);
                                UTL_FILE.PUT_LINE(f,lineout);
                          end loop;
                          close gc2;

                          UTL_FILE.FCLOSE(f);

                          EXECUTE IMMEDIATE UPD_STMT USING r.pid(x);
                          COMMIT;


                         lineout := 'C:\Users\christian.gardner\boaData\CHL\OUT_BOX\'||FILENAME||'|'||vDAISY_CANCEL_RESULTS||FILENAME;

                          UTL_FILE.PUT_LINE(mv,lineout);


                end loop;
                         begin
                          UTL_FILE.FCLOSE(f);

                         exception
                              when others then
                                 null;
                         end;

                         UTL_FILE.FCLOSE(mv);


                  INSERT INTO BOA_PROCESS_LOG
                  (
                    PROCESS,
                    SUB_PROCESS,
                    ENTRYDTE,
                    ROWCOUNTS,
                    MESSAGE
                  )
                  VALUES ( proc, SubProc,SYSDATE, 0, FILENAME||': CREATED');

                  COMMIT;

                  SEND_EMAIL  ( vTEAM, PROC||':'||SubProc, FILENAME||' was Created', 'The XCL CHL Results file has been created');


          end loop;

          close gc1;

          UTL_FILE.FCLOSE(mv);

      INSERT INTO BOA_PROCESS_LOG
      (
        PROCESS,
        SUB_PROCESS,
        ENTRYDTE,
        ROWCOUNTS,
        MESSAGE
      )
      VALUES ( proc, SubProc,SYSDATE, 0, 'Process complete' );

      COMMIT;

      SEND_EMAIL  ( vTEAM, PROC||':'||SubProc, 'XCL CHL process notification','XCL CHL Results file Process is now complete');

  exception
      when bad_news then
            null;
      WHEN OTHERS THEN

       MSG   := SQLERRM;
       RCODE := SQLCODE;

      INSERT INTO BOA_PROCESS_LOG
      (
        PROCESS,
        SUB_PROCESS,
        ENTRYDTE,
        ROWCOUNTS,
        MESSAGE
      )
      VALUES ( proc, SubProc,SYSDATE, RCODE, FILENAME||': '||MSG);

      COMMIT;

      SEND_EMAIL  (vTEAM, PROC||':'||SubProc, 'FILE_ERRORS',  MSG);

  END;


  PROCEDURE XCL_CHL_RESULTS_FILE( P_flag number)
  IS

    SQL_STMT1      VARCHAR2(32000);
    SQL_STMT2      VARCHAR2(32000);
    UPD_STMT       VARCHAR2(32000);
    LINEOUT        VARCHAR2(200);
    PROC           VARCHAR2(100);
    SubProc        VARCHAR2(100);
    RCODE          NUMBER;
    FILENAME       VARCHAR2(100);
    MSG            VARCHAR2(300);
    V_BATCH_DATE   DATE;

    f             UTL_FILE.FILE_TYPE;
    mv            UTL_FILE.FILE_TYPE;
    R             XCL_RESULTS_FILES;
    L             XCL_LAYOUT;
    GC1           GenRefCursor;
    GC2           GenRefCursor;
    BAD_NEWS      EXCEPTION;

  BEGIN
          PROC      := 'XCL CHL';
          SubProc   := 'RESULTS_FILE';
          vTEAM     := 'SYS Cancel';
/*
          SQL_STMT1 := 'select   MAX(BATCH_DATE)  from rdm.XCL_RESULTS_FILES@'||vDB_LINK||' WHERE RESULT_FILE_SENT IS NULL AND CLIENT_NAME = ''CHL'' ';

        EXECUTE IMMEDIATE SQL_STMT1 INTO V_BATCH_DATE;

          IF  ( V_BATCH_DATE IS NULL ) THEN
              SEND_EMAIL  ( vTEAM, proc||':'||SubProc,'RDM.XCL.Create_InspectionResults@'||vDB_LINK , 'Did not run today when expected');
              RAISE BAD_NEWS;
          END IF;

          IF  (TRUNC(V_BATCH_DATE) != TRUNC(SYSDATE) ) THEN
              SEND_EMAIL  ( vTEAM,PROC||':'||SubProc,'RDM.XCL.Create_InspectionResults@'||vDB_LINK , 'Did not run today when expected');
              RAISE BAD_NEWS;
          END IF;
*/

          mv := UTL_FILE.FOPEN('XCL_CHL_OUT_DIR', 'move_results.txt', 'w');

          UPD_STMT  := 'UPDATE RDM.XCL_RESULTS_FILES@'||vDB_LINK||' SET RESULT_FILE_SENT = ''YES'' WHERE PID = :P';

          SQL_STMT1 := 'select  PID, BATCH_DATE, RESULT_FILE_SENT, RESULT_FILE_SIZE, RESULT_FILE_NAME  from rdm.XCL_RESULTS_FILES@'||vDB_LINK||' WHERE RESULT_FILE_SENT IS NULL AND CLIENT_NAME = ''CHL'' ORDER BY BATCH_DATE';
          open gc1 for SQL_STMT1;
          loop
             FETCH GC1 BULK COLLECT INTO r.pid,
                                         r.batch_date,
                                         r.result_file_sent,
                                         r.result_file_size,
                                         r.result_file_name
                                         limit 1;
            exit when r.pid.count = 0;
                for x in 1..r.pid.count loop
                      f := UTL_FILE.FOPEN('XCL_CHL_OUT_DIR', r.result_file_name(x), 'w');

                       FILENAME := r.result_file_name(x);


                        SQL_STMT2 := 'select A.PID, A.ORDNUM, A.DETID, A.LNUM, A.PROCESS_DT, A.DELIMITERS_57, A.CANCELED, A.DELIMITERS_6 from RDM.XCL_LAYOUT@'||vDB_LINK||' A ';
                        SQL_STMT2 := SQL_STMT2||' left join ( select results_pid, pid from RDM.XCL_tracking@'||vDB_LINK||' ) t  on ( a.pid = t.pid) ';
                        SQL_STMT2 := SQL_STMT2||' left join  ( select pid from RDM.XCL_RESULTS_FILES@'||vDB_LINK||' )  f on ( f.pid = T.RESULTS_PID) ';
                        SQL_STMT2 := SQL_STMT2||' where t.results_pid = f.pid ';
                        SQL_STMT2 := SQL_STMT2||' and a.pid = t.pid';
                        SQL_STMT2 := SQL_STMT2||' AND F.PID = :4';

                         open gc2 for SQL_STMT2 using r.pid(x);
                          loop
                              FETCH GC2 BULK COLLECT INTO L.PID,
                                                          L.ORDNUM,
                                                          L.DETID,
                                                          L.LNUM,
                                                          L.PROCESS_DT,
                                                          L.DELIMITERS_57,
                                                          L.CANCELED,
                                                          L.DELIMITERS_6
                                                          limit 1000;
                            exit when L.pid.count = 0;
                            for y in 1..L.pid.count loop
                                LINEOUT :=  L.ORDNUM(y)||'|'||L.DETID(y)||'|'||L.LNUM(y)||'|'||L.Process_DT(y)||'||'||L.Process_DT(y)||'|'||L.delimiters_57(y)||'|'||L.CANCELED(y)||'|'||L.delimiters_6(y);
                                UTL_FILE.PUT_LINE(f,lineout);
                            end loop;
                                LINEOUT := r.result_file_size(x);
                                UTL_FILE.PUT_LINE(f,lineout);
                          end loop;
                          close gc2;

                          UTL_FILE.FCLOSE(f);

                          EXECUTE IMMEDIATE UPD_STMT USING r.pid(x);
                          COMMIT;


                         lineout := 'C:\Users\christian.gardner\boaData\CHL\OUT_BOX\'||FILENAME||'|'||vDAISY_CANCEL_RESULTS||FILENAME;

                          UTL_FILE.PUT_LINE(mv,lineout);


                end loop;
                         begin
                          UTL_FILE.FCLOSE(f);

                         exception
                              when others then
                                 null;
                         end;

                         UTL_FILE.FCLOSE(mv);


                  INSERT INTO BOA_PROCESS_LOG
                  (
                    PROCESS,
                    SUB_PROCESS,
                    ENTRYDTE,
                    ROWCOUNTS,
                    MESSAGE
                  )
                  VALUES ( proc, SubProc,SYSDATE, 0, FILENAME||': CREATED');

                  COMMIT;

                  SEND_EMAIL  ( vTEAM, PROC||':'||SubProc, FILENAME||' was Created', 'The XCL CHL Results file has been created');


          end loop;

          close gc1;

          UTL_FILE.FCLOSE(mv);

      INSERT INTO BOA_PROCESS_LOG
      (
        PROCESS,
        SUB_PROCESS,
        ENTRYDTE,
        ROWCOUNTS,
        MESSAGE
      )
      VALUES ( proc, SubProc,SYSDATE, 0, 'Process complete' );

      COMMIT;

      SEND_EMAIL  ( vTEAM, PROC||':'||SubProc, 'XCL CHL process notification','XCL CHL Results file Process is now complete');

  exception
      when bad_news then
            null;
      WHEN OTHERS THEN

       MSG   := SQLERRM;
       RCODE := SQLCODE;

      INSERT INTO BOA_PROCESS_LOG
      (
        PROCESS,
        SUB_PROCESS,
        ENTRYDTE,
        ROWCOUNTS,
        MESSAGE
      )
      VALUES ( proc, SubProc,SYSDATE, RCODE, FILENAME||': '||MSG);

      COMMIT;

      SEND_EMAIL  (vTEAM, PROC||':'||SubProc, 'FILE_ERRORS',  MSG);

  END;

  PROCEDURE PKG_SETUP
  IS

  BEGIN
      --- LIBR_DB_LINK
      --- DEV_DB_LINK
      select VAR_VALUE
      INTO   vDB_LINK
      FROM   SCHEMA_VARIABLES
      WHERE  VAR_NAME  = 'DEV_DB_LINK';

      select VAR_VALUE
      INTO   vUSER_home
      FROM   SCHEMA_VARIABLES
      WHERE  VAR_NAME  = 'USER_HOME';
      --- C:\boaData\CHL\IN_BOX\SAVE\SWEEP\
      SELECT VAR_VALUE
      INTO   vDAISY_MV_FROM
      FROM   SCHEMA_VARIABLES
      WHERE  VAR_NAME  = 'DAISY_MV_FROM';
      --- C:\boaData\CHL\IN_BOX\SAVE\SWEEP\SAVE\
      SELECT VAR_VALUE
      INTO   vDAISY_MV_TO
      FROM   SCHEMA_VARIABLES
      WHERE  VAR_NAME  = 'DAISY_MV_TO';
      --- C:\boaData\SPI_DAISY\SAVE\
      SELECT VAR_VALUE
      INTO   vDAISY_CPY_FROM
      FROM   SCHEMA_VARIABLES
      WHERE  VAR_NAME  = 'DAISY_CPY_FROM';
      ------ Z:\ SCRUB REPORTS
     SELECT VAR_VALUE
     INTO   vDAISY_SCRUB_REPORTS
     FROM   SCHEMA_VARIABLES
     WHERE  VAR_NAME  = 'DAISY_SCRUB_REPORT';

     SELECT VAR_VALUE
     INTO   vDAISY_CANCEL_RESULTS
     FROM   SCHEMA_VARIABLES
     WHERE  VAR_NAME  = 'DAISY_CANCEL_RESULTS';


  END;

BEGIN

     PKG_SETUP;
end;

/
