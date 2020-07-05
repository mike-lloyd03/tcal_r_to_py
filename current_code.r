I am using Microsoft SQL Server 2016.

I have a table with a primary key.

I want to create a new table that only has the columns that are in the primary key, and that has the same primary key.

Please advise.

Ideas:
Copy the table, then subset it
https://www.tutorialrepublic.com/sql-tutorial/sql-cloning-tables.php
CREATE TABLE new_table LIKE original_table;
INSERT new_table SELECT * FROM original_table;

Use ALTER COLUMN to add or delete columns?
----



DECLARE @TableName varchar(max)
DECLARE @TableID varchar(max)
DECLARE @SQLQuery varchar(max)
SET @TableName = 'mydatabase.dbo.a_temp2'
SET @TableID   = OBJECT_ID(@TableName)
USE mydatabase
SET @SQLQuery = 'SELECT TOP 1 * FROM ' + OBJECT_NAME (@TableID)
EXECUTE(@SQLQuery)

https://stackoverflow.com/questions/20678725/how-to-set-table-name-in-dynamic-sql-query/29082755#29082755	




https://www.google.com/search?q=sql+loop+variable+table+name
https://stackoverflow.com/questions/42559375/loop-through-different-tables-with-variable-names

Table names cannot be supplied as parameters, so you'll have to construct the SQL string manually like this:

SET @SQLQuery = 'SELECT * FROM ' + @TableName + ' WHERE EmployeeID = @EmpID' 
https://docs.microsoft.com/en-us/sql/t-sql/functions/object-id-transact-sql?view=sql-server-ver15

SET @TableName = '<[db].><[schema].>tblEmployees'
SET @TableID   = OBJECT_ID(TableName) --won't resolve if malformed/injected.
https://docs.microsoft.com/en-us/sql/t-sql/functions/object-id-transact-sql?view=sql-server-ver15

SET @TableName = 'xpressfeed.dbo.a_temp2'
SET @TableID   = OBJECT_ID(TableName)



...
SET @SQLQuery = 'SELECT * FROM ' + OBJECT_NAME(@TableID) + ' WHERE EmployeeID = @EmpID' 







--don't need lag 1q date in sheet; just use eomonth (dateadd) when joining
--check data by comparing with annual data?
--maybe move data into R so SQL is not needed after a certain point?




Calculating Single Qtr Cash Flow Data
-fq calc. calc months since prior fy end. divide by 3.
-ensure data i ssorted with more reent data on top; create table that has data from 1 month [below]; then set data to NA if: not same "record" (gvd+iid+qf0date+fyr), if fiscal qrtr == 1; then calc delta from (q at time t) - (q at time t-1); then sub into this data where month (datadate) == 1; then calc tr12sum; then check that tr12sum == data where fqtr = 4; then sub data where fqtr =4


--for each df, we would need a "key" consisting of gvkey+sequential qtrly date 1 thru 12, starting from qtrdate0 and going back in 3 month increments from there

--if we wanted to use SQL, we'd need a table with 1 row, with gvkey, and qtrdate, for each gvkeym and for each of 12 seqntial qtr dates. We can maybe create this with a loop within SQL, similar to loop we are using in R, for 0 : 12 ... dateadd(3, month, co_ifndqmaxdatefor each gvkey) 

--in R, we'll want to figure out when to 

 all qtrly data, and of ever want to use a combination of annual and qtrly data (ex. for companies that have for any fiscal year reported annual data but not 4 qtrs of qtrly data) one possibility is using coalesce (tr36months, tr3yrs)

------------


Compustat XF To Do's
-create a doc with checks on data, ex. missing primiss's, missing or zero ajexms, have daily prccdm but not prccm, etc.

-add "inactive flag to query that finds missing primiss's

-keep notes and FAQ's re CS & XF

- keep notes re R, SQL




Backtest Data
--create a query we'll run in SQL or R that using this table, obtains data we need from coafndq ...we'llprobably want to get 12 different R dataframes, for 12 different quarters of data. 
--An alternative to downloading 12 tables is to create a way to downlaod all data in co_ifndq in 1 R table, and then transform this table in R to what we need. 


--a future featire could be figuring out when tr12 data is for a fiscal year, and using coalesce (tr1yr, tr12 mos)

--maybe start with a "report" showing how many sequential qrtrs of quarterly data each co has ( 0 - ~20), and also how much seqnetial annual data each has (0 - 3 )

--prob in R, we'll want to "transform" all quarterly data reported in cumulative format (ex. cash flow data) to tr12 format


--http://www.dpriver.com/pp/sqlformat.htm
-------------------------
--//create subset of co_ifndq with quarterly fundamentla data we will use in our "daily snapshot report"
--#calc fqdatemax, the "max" fiscal quarter end date in co_ifndq for each gvkey+fyr combination
--#screen for when fqdatemax is "too old"

-------------------------
--//create subset of co_ifndq with quarterly fundamentla data we will use in our "daily snapshot report"
--#calc fqdatemax, the "max" fiscal quarter end date in co_ifndq for each gvkey+fyr combination
--#screen for when fqdatemax is "too old"


--Please, what does it mean when in co_ifndq, for the same datadate, there are more than 1 fyr's? Thanks! A company changed the month of its fiscal year end.
 
--check for when there is more than 1 fyr for same datadate
--SELECT TOP 10 t.* FROM
--(SELECT f.gvkey, f.datadate, COUNT(f.gvkey) gv_n, MAX(f.fyr) fyr_mx, MIN(f.fyr) fyr_mn 
--FROM co_ifndq f
--WHERE f.INDFMT='INDL' and f.CONSOL='C' and  f.POPSRC='D' and f.DATAFMT='STD' AND f.datadate > DATEADD(YEAR, -5,  GETDATE() ) AND COALESCE (f.cshfdq, f.cshprq) > 0 AND f.atq > 0
--GROUP BY f.gvkey, f.datadate
--) t
--WHERE t.gv_n > 1 
--ORDER BY t.datadate DESC, t.gvkey 

--SELECT * FROM co_ifndq f WHERE f.gvkey = '031006' AND f.datadate = '9/30/2018' AND f.INDFMT='INDL' and f.CONSOL='C' and  f.POPSRC='D' and f.DATAFMT='STD' 

--
--START sql startsql
USE xpressfeed; -- set db to xpressfeed

-- Drop all tables in the xpressfeed db that begin with "a_". These are "user-created' tables.
USE xpressfeed; 
DECLARE @cmd varchar(4000)
DECLARE cmds CURSOR FOR
SELECT 'drop table [' + Table_Name + ']'
FROM INFORMATION_SCHEMA.TABLES
WHERE LEFT(Table_Name, 2) = 'a_' --WHERE Table_Name LIKE 'a_%' 

OPEN cmds
WHILE 1 = 1
BEGIN
    FETCH cmds INTO @cmd
    IF @@fetch_status != 0 BREAK
    EXEC(@cmd)
END
CLOSE cmds;
DEALLOCATE cmds
---
--calculates the most recent date where for gvkey 6066 (IBM) we have a not null datadate in the sec_dprc table and saves in table a_maxdateprc
DROP TABLE IF EXISTS a_maxdateprc 
SELECT max(datadate) maxdateprc 
INTO   a_maxdateprc 
FROM   sec_dprc 
WHERE  gvkey = 6066 
AND    iid = '01'
GO

SELECT maxdateprc FROM   a_maxdateprc --2020-06-02 00:00:00.000
---       
-- create subset of sec_dprc where prccd > 0 AND datadate = a_maxdateprc..maxdateprc  
USE xpressfeed; 
DROP TABLE IF EXISTS a_sec_dprc_ss
SELECT * 
INTO   a_sec_dprc_ss 
FROM   sec_dprc d 
WHERE  d.curcdd = 'USD' 
       AND d.prccd > 0 
       AND d.datadate = (SELECT maxdateprc FROM  a_maxdateprc) 
GO 

--add primary key to table 
USE xpressfeed; 
GO 

ALTER TABLE a_sec_dprc_ss 
  ALTER COLUMN gvkey VARCHAR(6) NOT NULL 

ALTER TABLE a_sec_dprc_ss 
  ALTER COLUMN iid VARCHAR(3) NOT NULL 

ALTER TABLE a_sec_dprc_ss 
  ALTER COLUMN datadate DATETIME NOT NULL; 

ALTER TABLE a_sec_dprc_ss 
  ALTER COLUMN curcdd VARCHAR(3) NOT NULL; 

GO 

--- 
ALTER TABLE a_sec_dprc_ss 
  ADD PRIMARY KEY(gvkey, iid, datadate, curcdd); 

GO 

------


--DECLARE @maxdateprc datetime;
--SET @maxdateprc = SELECT maxdateprc FROM   a_maxdateprc



--creates a subset of the co_ifndq table, so we do not needto use this WHERE clause more than once
USE xpressfeed; 
DROP TABLE IF EXISTS a_co_ifndq_ss 
SELECT f1.* , ci.fqtr
INTO   a_co_ifndq_ss 
FROM   co_ifndq f1 
LEFT JOIN co_idesind ci ON ci.gvkey = f1.gvkey AND ci.datadate = f1.datadate AND ci.INDFMT='INDL' AND ci.CONSOL = 'C' AND  ci.POPSRC='D' AND ci.fyr = f1.fyr AND ci.DATAFMT = 'STD'      
WHERE  f1.indfmt='INDL' 
AND    f1.consol='C' 
AND    f1.popsrc='D' 
AND    f1.datafmt='STD' 
AND    COALESCE (f1.cshfdq, f1.cshprq) > 0 
AND    f1.atq > 0 
--AND   f1.datadate > dateadd(year, -5, getdate() ) --only include data less than 5 years from getdate()
AND   f1.datadate > dateadd(year, -5, (SELECT maxdateprc FROM   a_maxdateprc)  ) --only include data less than 5 years from maxdateprc
--for all datadates, ~1.3 million rows; takes ~3 minutes (1268671 row(s) affected)
--for only datadates within prior 5 years, ~
GO 

--creates a subset of the co_ifndytd table, so we do not needto use this WHERE clause more than once
USE xpressfeed; 
DROP TABLE IF EXISTS a_co_ifndytd_ss 
SELECT y.* , f1.fqtr
INTO   a_co_ifndytd_ss 
FROM   a_co_ifndq_ss f1 
--LEFT JOIN co_idesind ci ON ci.gvkey = f1.gvkey AND ci.datadate = f1.datadate AND ci.INDFMT='INDL' AND ci.CONSOL = 'C' AND  ci.POPSRC='D' AND ci.fyr = f1.fyr AND ci.DATAFMT = 'STD'        
LEFT JOIN co_ifndytd y  ON  y.gvkey = f1.gvkey AND y.datadate = f1.datadate AND y.INDFMT='INDL' AND y.CONSOL = 'C' AND  y.POPSRC='D' AND y.fyr = f1.fyr AND y.DATAFMT = 'STD'        
GO 

--Primary Key Analysis
--  gvkey
--  datadate
--y indfmt
--y consol
--y popsrc
--  fyr
--y datafmt

--(145478 row(s) affected)


--add primary key to table 
USE xpressfeed; 
--go 

ALTER TABLE a_co_ifndq_ss 
  ALTER COLUMN gvkey VARCHAR(6) NOT NULL 

ALTER TABLE a_co_ifndq_ss 
  ALTER COLUMN datadate DATETIME NOT NULL; 

ALTER TABLE a_co_ifndq_ss 
  ALTER COLUMN fyr TINYINT NOT NULL; 

--go 

--- 
ALTER TABLE a_co_ifndq_ss 
  ADD PRIMARY KEY(gvkey, datadate, fyr); 
--go 
--- 


--check for when there is more than 1 record and fyr for same datadate
--SELECT t.* FROM
--(SELECT f.gvkey, f.datadate, COUNT(f.gvkey) gv_n, MAX(f.fyr) fyr_mx, MIN(f.fyr) fyr_mn 
--FROM a_co_ifndq_ss f
--GROUP BY f.gvkey, f.datadate) t
--WHERE t.gv_n > 1 ORDER BY t.datadate DESC, t.gvkey
---109 rowsout of ~148,537  6/5/2020 

--- 

---
DROP VIEW IF EXISTS vw_co_ifndq0 
GO


--FOR QRLTY BACKETST DATA
--create tables with rows for all dates, then try to fidn a way to add ~7000 roes for most recent prccd
--create view that adds 1 or more iid for each row in vw_co_ifndq_ss
--add date of monthly security price we will try to ontain to each row to vw_co_ifndq_ss "dd_mp_f6mo"
--find primiss_iid for this gvkey on the datadate = dd_mp_f6mo



--FOR DAILY DATA CONTINUE HERE!
DROP VIEW IF EXISTS vw_co_ifndq_ss_datemax 
GO
CREATE VIEW vw_co_ifndq_ss_datemax AS
--//Create view from vw_co_ifndq_ss, with gvkey, fyr_mx, fqdatemax, WHERE fqdatemax is not "too old" to be our "fq0date"; if more than 1 fyr in a_co_ifndq_ss we will use fyr_mx to join with other tables and views when calculating mcapd, obtaining quarterly anbd annual fundametnals, etc.
SELECT t2.gvkey, t2.fqdatemax, t2.fyr_mx
, n.iid_n, iid_mx , CASE WHEN iid_n = 1 THEN iid_mx ELSE NULL END AS iid2 
FROM 

--t2 --limits
(SELECT t1.gvkey, t1.fqdatemax, MAX (a.fyr) AS fyr_mx
FROM
--t1
--t1 find all unique gvkeys and each gvkey's max datadate (a fiscal quarterly date) in a_co_ifndq_ss
(SELECT f.gvkey, MAX (f.datadate) AS fqdatemax 
FROM a_co_ifndq_ss f 
WHERE f.INDFMT='INDL' and f.CONSOL='C' and  f.POPSRC='D' and f.DATAFMT='STD'
GROUP BY f.gvkey) t1 --10115

--t2 join t1 on a_co_ifndq_ss so we can get data from co_ifndq where datadate = fqdatemax 
LEFT JOIN a_co_ifndq_ss a ON t1.gvkey = a.gvkey AND t1.fqdatemax = a.datadate
GROUP BY t1.gvkey, t1.fqdatemax) t2 --10115 

LEFT JOIN ( SELECT gvkey, COUNT (iid) AS iid_n , MAX (iid) AS iid_mx  FROM a_sec_dprc_ss d GROUP BY d.gvkey) n ON n.gvkey = t2.gvkey
WHERE t2.fqdatemax > DATEADD(MONTH, -19, (SELECT maxdateprc FROM a_maxdateprc) ) --only use rows where fqdatemax is within 19 months of a_maxdateprc
GO

SELECT COUNT(*) FROM vw_co_ifndq_ss_datemax --7907

----------------------------

----------------------------
DROP TABLE IF EXISTS a_mcapd 
GO
--//identify primary issue, and calc company level daily market cap
-- add notes to explain iid, iid2, primiss_iid, iidn, and why some primiss_iid and iid2 and primiss_iid are NULL
--iid          from a_sec_dprc_ss.iid
--primiss_iid  from sec_idhist.itemvalue AND idpr.item='PRIHISTUSA'
--iid_n        from vw_co_ifndq_ss_datemax.iid_n [this needs elaboration]
--iid2         from vw_co_ifndq_ss_datemax.iid2 [this needs elaboration]

SELECT t.gvkey, t.fqdatemax AS fq0date, t.fyr_mx, t.iid_n , t.iid2, f.cshfdq, f.cshprq , f.fqtr, adj2.adjex AS adjex_fq0, adj1.adjex AS adjex_dp
, d.datadate, d.qunit , d.adrrc, d.curcdd
, idpr.itemvalue AS primiss_iid , d.datadate AS dateprc, d.iid, d.prccd, d.ajexdi, adj2.adjex AS adjex_fq
, CASE WHEN d.iid NOT LIKE '%W' THEN d.prccd ELSE d.prccd / ISNULL(d.qunit,1) END AS prccd_qunit 
, ISNULL ( f.cshfdq, f.cshprq)  * (adj2.adjex / adj1.adjex) * ( ( CASE WHEN d.iid NOT LIKE '%W' THEN d.prccd ELSE d.prccd / ISNULL(d.qunit,1) END  ) ) AS mcapd 
, s.tpci , c.stko, sm.mkvalincl
--mp.prccm / mp.ajexm AS  prccm_adj
INTO a_mcapd
FROM vw_co_ifndq_ss_datemax t
LEFT JOIN a_co_ifndq_ss f ON
f.gvkey = t.gvkey AND f.datadate = t.fqdatemax AND f.fyr = t.fyr_mx AND f.INDFMT='INDL' and f.CONSOL='C' and  f.POPSRC='D' and f.DATAFMT='STD'
LEFT JOIN co_adjfact adj2 ON f.gvkey = adj2.gvkey AND f.datadate BETWEEN adj2.EFFDATE AND adj2.THRUDATE AND adj2.adjex IS NOT NULL --match on date of qtrly fundamentals 
LEFT JOIN sec_idhist idpr ON f.gvkey = idpr.gvkey AND idpr.item='PRIHISTUSA' AND (SELECT maxdateprc FROM a_maxdateprc)  between idpr.efffrom AND idpr.effthru --AND d.iid = idpr.iid  
LEFT JOIN a_sec_dprc_ss d ON d.gvkey = f.gvkey and d.iid = ISNULL(idpr.itemvalue, t.iid2) --AND d.datadate = (SELECT maxdateprc FROM a_maxdateprc) AND d.curcdd = 'USD'
LEFT JOIN co_adjfact adj1 ON d.gvkey = adj1.GVKEY AND d.datadate BETWEEN adj1.EFFDATE AND adj1.THRUDATE AND adj1.adjex IS NOT NULL --match on date of price
LEFT JOIN security s ON s.gvkey = d.gvkey and s.iid = d.iid
LEFT JOIN company  c ON c.gvkey = t.gvkey
LEFT JOIN sec_mshare sm ON sm.gvkey = d.gvkey and sm.iid = d.iid AND sm.datadate = EOMONTH ( d.datadate , -1 )  
--LEFT JOIN a_co_ifndq_ss flag3mo ON flag3mo.gvkey = t.gvkey AND flag3mo.datadate = EOMONTH( DATEADD(QUARTER,-1,t.fqdatemax) ) AND flag3mo.fyr = t.fyr_mx AND flag3mo.INDFMT='INDL' and flag3mo.CONSOL='C' and  flag3mo.POPSRC='D' and flag3mo.DATAFMT='STD'
WHERE d.gvkey IS NOT NULL
GO


--add primary key to table
USE xpressfeed; 
GO  
ALTER TABLE a_mcapd
ALTER COLUMN gvkey varchar(6) NOT NULL
ALTER TABLE a_mcapd
ALTER COLUMN iid varchar(3) NOT NULL 
ALTER TABLE a_mcapd
ALTER COLUMN fq0date datetime NOT NULL;  
ALTER TABLE a_mcapd
ALTER COLUMN fyr_mx tinyint NOT NULL;   
ALTER TABLE a_mcapd
ALTER COLUMN datadate datetime NOT NULL;  
ALTER TABLE a_mcapd
ALTER COLUMN curcdd varchar(3) NOT NULL;  
ALTER TABLE a_mcapd
ALTER COLUMN iid_n int NOT NULL
GO

---
ALTER TABLE a_mcapd
ADD PRIMARY KEY(gvkey, iid, fq0date, fyr_mx, datadate, curcdd); 
GO
---
SELECT COUNT(*) FROM vw_co_ifndq_ss_datemax --7689 6/5/2020

--ensure counts below are same
SELECT COUNT(*) FROM a_mcapd --6661
SELECT COUNT(*) FROM (SELECT gvkey FROM a_mcapd GROUP BY gvkey) t --6661
--
SELECT conm, a.* 
FROM a_mcapd a 
LEFT join company c ON c.gvkey = a.gvkey
ORDER BY mcapd DESC--6661

----------------------------
-- calc tr 1yr and tr 3 yr div and cheqv
-- The daily equivalents of dvpsxm and cheqvm are div and cheqv
-- start with a_mcapd
-- join on table with daily div data, using div.datadate <= dateprc and div.datadate > dateprc - 3 years; 
--create loop for 1 y and 3 y
--get tr12 month split adjusted dividends
--get tr36 month split adjusted dividends

DROP VIEW IF EXISTS vwd_div_cheqv_1yd
GO
CREATE VIEW vwd_div_cheqv_1yd AS
SELECT m.gvkey, m.fq0date, DATEADD (year, -1, m.datadate) AS dd_1y, m.fyr_mx, m.datadate, m.curcdd 
--, COALESCE( SUM(d.div / dp.ajexdi), 0 ) AS div_1y , COALESCE( SUM(d.cheqv / dp.ajexdi), 0 ) AS cheqv_1y 
, SUM(d.div / dp.ajexdi) AS div_1y , SUM(d.cheqv / dp.ajexdi) AS cheqv_1y 
, MAX(dp.ajexdi) AS adjexdi_max , MIN(dp.ajexdi) AS adjexdi_min
FROM a_mcapd m --a_mcapd.datadate is a daily datadate from daily price table
LEFT JOIN sec_divid d --gvkey, iid, datadatecurcddv
ON d.gvkey = m.gvkey AND d.iid = m.primiss_iid AND d.curcddv = m.curcdd AND d.datadate <= m.datadate and d.datadate > DATEADD (year, -1, m.datadate)
LEFT JOIN sec_dprc dp --gvkey, iid, datadatecurcddv ON
ON dp.gvkey = d.gvkey AND dp.iid = d.iid AND dp.curcdd = d.curcddv AND dp.datadate = d.datadate
GROUP BY m.gvkey, m.fq0date, m.fyr_mx, m.datadate, m.curcdd -- 
GO

DROP VIEW IF EXISTS vwd_div_cheqv_3yd
GO
CREATE VIEW vwd_div_cheqv_3yd AS
SELECT m.gvkey, m.fq0date, DATEADD (year, -3, m.datadate) AS dd_3y, m.fyr_mx, m.datadate, m.curcdd 
--, COALESCE( SUM(d.div / dp.ajexdi), 0 ) AS div_3y , COALESCE( SUM(d.cheqv / dp.ajexdi), 0 ) AS cheqv_3y 
, SUM(d.div / dp.ajexdi) AS div_3y , SUM(d.cheqv / dp.ajexdi) AS cheqv_3y 
, MAX(dp.ajexdi) AS adjexdi_max , MIN(dp.ajexdi) AS adjexdi_min
FROM a_mcapd m
LEFT JOIN sec_divid d --gvkey, iid, datadatecurcddv
ON d.gvkey = m.gvkey AND d.iid = m.primiss_iid AND d.curcddv = m.curcdd AND d.datadate <= m.datadate and d.datadate > DATEADD (year, -3, m.datadate)
LEFT JOIN sec_dprc dp --gvkey, iid, datadatecurcddv ON
ON dp.gvkey = d.gvkey AND dp.iid = d.iid AND dp.curcdd = d.curcddv AND dp.datadate = d.datadate
GROUP BY m.gvkey, m.fq0date, m.fyr_mx, m.datadate, m.curcdd
GO

----------------------------
DROP TABLE IF EXISTS a_secd_q0
DROP TABLE IF EXISTS dbo.a_temp
--DROP TABLE IF EXISTS dbo.a_checktable
--SELECT 0 AS mycheck INTO a_checktable 
GO
---
DECLARE @MyCounter INT
SET @MyCounter = 0

DECLARE @MyCounterVC varchar(10);
DECLARE @MySQLQueryVw varchar(max);
DECLARE @MySQLQuery varchar(max);

WHILE @MyCounter >= -12

BEGIN

SET @MyCounterVC = CAST(@MyCounter as varchar(10))

SET @MySQLQueryVw = 
'CREATE VIEW vw_a_temp AS
SELECT f1.gvkey AS gvk, f1.iid, f1.fq0date, f1.fyr_mx AS fyr0, f1.fqtr AS fqtr0, f1.datadate AS dateprc, '
+ @MyCounterVC + ' AS qlag
, EOMONTH(DATEADD(QUARTER, ' + @MyCounterVC + ' , f1.fq0date)) AS qtrnext
FROM a_mcapd f1'

PRINT @MyCounter
PRINT @MySQLQueryVw
DROP VIEW IF EXISTS vw_a_temp
EXECUTE(@MySQLQueryVw)

IF OBJECT_ID('dbo.a_temp', 'U') IS NULL 
SET @MySQLQuery = 'SELECT * INTO a_temp FROM vw_a_temp'
ELSE 
SET @MySQLQuery = 'INSERT INTO a_temp SELECT * FROM vw_a_temp'

EXECUTE(@MySQLQuery)

SET @MyCounter = @MyCounter - 1
END

--Cleanup
--DROP TABLE IF EXISTS dbo.a_checktable
GO

--Check work
SELECT COUNT(*) FROM a_temp --86593
SELECT count(*) FROM a_mcapd --6661
SELECT cast( (SELECT COUNT(*) FROM a_temp)  / (SELECT count(*) FROM a_mcapd) AS DECIMAL (10,2) )
--DROP TABLE IF EXISTS dbo.a_temp

--get ytd data for each row in a_temp
DROP TABLE IF EXISTS a_data_co_ifndytd
GO
--SELECT a.*, f.* 
--INTO a_data_co_ifndytd FROM a_temp a
--LEFT JOIN a_co_ifndytd_ss f
--ON a.gvk = f.gvkey AND a.qtrnext = f.datadate AND a.fyr0 = f.fyr
--AND    f.indfmt='INDL' 
--AND    f.consol='C' 
--AND    f.popsrc='D' 
--AND    f.datafmt='STD' 

--SELECT COUNT(*) FROM a_data_co_ifndytd
--SELECT TOP 1 * FROM a_data_co_ifndytd


--next join on co_afndq and co_afndytd to get data
USE xpressfeed
DECLARE @MyTableName varchar(max);
DECLARE @MyTableID varchar(max);
DECLARE @MyJoinOnDate varchar(max);
DECLARE @MySQLQuery varchar(max);
DECLARE @MyCounter INT;
DECLARE @MyNewTablename varchar(max);
DECLARE @MyNewTableID varchar(max);

SET @MyCounter = 1
WHILE @MyCounter <= 4

BEGIN
PRINT @MyCounter

IF @MyCounter = 1 SET @MyTableName = 'co_ifndq'

IF @MyCounter = 2 SET @MyTableName = 'co_idesind'

IF @MyCounter = 3 SET @MyTableName = 'co_ifndytd'

IF @MyCounter = 4 SET @MyTableName = 'co_ifndytd'

PRINT @MyTableName

SET @MyTableID   = OBJECT_ID(@MyTableName)

IF @MyCounter <=3 SET @MyJoinOnDate = 'a.qtrnext' ELSE SET @MyJoinOnDate = 'EOMONTH(DATEADD(QUARTER, -1, a.qtrnext))'
PRINT @MyJoinOnDate

IF @MyCounter <=3 SET @MyNewTablename = 'a_data_' + OBJECT_NAME ( @MyTableID ) ELSE SET @MyNewTablename = 'a_data_' + OBJECT_NAME ( @MyTableID ) + '_lag1q'
PRINT @MyNewTablename
	
SET @MySQLQuery = 'DROP TABLE IF EXISTS ' + @MyNewTableName
PRINT @MySQLQuery
EXECUTE(@MySQLQuery)

--
--SET @MyTableID   = OBJECT_ID(@MyTableName)

SET @MySQLQuery =
'SELECT a.*, f.* 
INTO ' + @MyNewTableName +
' FROM a_temp a
LEFT JOIN ' + OBJECT_NAME (@MyTableID) + ' f
ON a.gvk = f.gvkey AND ' + @MyJoinOnDate + ' = f.datadate AND a.fyr0 = f.fyr
AND    f.indfmt=''INDL'' 
AND    f.consol=''C'' 
AND    f.popsrc=''D'' 
AND    f.datafmt=''STD'' '

PRINT @MySQLQuery
EXECUTE(@MySQLQuery)

SET @MyNewTableID   = OBJECT_ID(@MyNewTableName)
PRINT @MyNewTableID
PRINT OBJECT_NAME (@MyNewTableID)

--add primary key to table
SET @MySQLQuery = 
'ALTER TABLE ' + OBJECT_NAME (@MyNewTableID) +
' ALTER COLUMN gvk varchar(6) NOT NULL';
--PRINT @MySQLQuery
EXECUTE(@MySQLQuery)

SET @MySQLQuery = 
'ALTER TABLE ' + OBJECT_NAME (@MyNewTableID) +
' ALTER COLUMN iid varchar(3) NOT NULL';
EXECUTE(@MySQLQuery)

SET @MySQLQuery = 
'ALTER TABLE ' + OBJECT_NAME (@MyNewTableID) +
' ALTER COLUMN fq0date datetime NOT NULL';  
EXECUTE(@MySQLQuery)

SET @MySQLQuery = 
'ALTER TABLE ' + OBJECT_NAME (@MyNewTableID) +
' ALTER COLUMN fyr0 tinyint NOT NULL';   
EXECUTE(@MySQLQuery)

SET @MySQLQuery = 
'ALTER TABLE ' + OBJECT_NAME (@MyNewTableID) +
' ALTER COLUMN qlag int NOT NULL';  
EXECUTE(@MySQLQuery)

SET @MySQLQuery = 
'ALTER TABLE ' + OBJECT_NAME (@MyNewTableID) +
' ALTER COLUMN dateprc datetime NOT NULL';  
EXECUTE(@MySQLQuery)

SET @MySQLQuery = 
'ALTER TABLE ' + OBJECT_NAME (@MyNewTableID) +
' ADD PRIMARY KEY(gvk, iid, fq0date, fyr0, qlag)'; 
EXECUTE(@MySQLQuery)

SET @MyCounter = @MyCounter + 1
END


--Data for Daily Report
--Note: Excel has 256 columns
--tables: a_secd_q0. security, sec_dprc, company
--create screen using data in sec_dprc and also mcap (prccd, mcap). 
--sec_dprc and mcapdm data: stko, tpci, inactive co flag, inactive sec flag, dlrs, dlrsni, all data to clc use, 
--maybe later: date of most recent prccd, date of most recent prccm,

DROP TABLE IF EXISTS a_mcapd_q0_t0data
GO
SELECT
s.tic, c.conm, c.sic
, sd.gvkey, sd.iid, sd.datadate AS dateprc, sd.fq0date, sd.fyr_mx --, sd.gv_n --(all sd fields)
, a.iid2, a.primiss_iid, a.cshfdq, a.cshprq, a.adjex_fq0, a.adjex_dp, a.qunit qunit_mc , a.adrrc adrcc_mc, a.prccd prccd_mc, a.prccd_qunit prccd_qumc , a.mcapd, a.tpci, a.stko, a.mkvalincl
, dp.cshoc, dp.prcstd, dp.cshtrd, dp.ajexdi, dp.qunit, dp.adrrc, dp.prccd, dp.dvi , dp.eps, dp.epsmo
, c.dldte, c.dlrsn, s.dldtei, s.dlrsni
INTO a_mcapd_q0_t0data
FROM a_mcapd sd
LEFT JOIN a_mcapd a ON a.gvkey = sd.gvkey AND a.dateprc = sd.datadate and a.fyr_mx = sd.fyr_mx
LEFT JOIN a_sec_dprc_ss dp ON dp.gvkey = sd.gvkey AND dp.iid = sd.iid and dp.datadate = sd.datadate
LEFT JOIN company c ON c.gvkey = sd.gvkey
LEFT JOIN security s ON s.gvkey = sd.gvkey AND s.iid = sd.iid
GO


--add useqd_sql to a_mcapd_q0_t0data
-- atq, (cshfdq, cshpriq), prccd, etc. - check use from annual code
--DATA NEEDED TO CALC "USE": primiss_flag <- is.na( t0data$primissh) == T | fn.nar( t0data$primiss_sm, -99 ) =='P'| fn.nar(t0data$primiss_sih, -99) == t0data$iid
--use_0_prlm1 <- primiss_flag ==T & fn.nar(t0data$at_0) > 0 & fn.nar(t0data$cshtrm) > 0 & fn.nar(t0data$prccm) > 2 & ( fn.nar (t0data$sich, t0data$sic) < 6000 | fn.nar (t0data$sich, t0data$sic) > 6999 ) & ( is.na( t0data$mkvalincl) == T | fn.nar(t0data$mkvalincl) == 'Y') & fn.nar(t0data$stko,-99) == '0'  & is.na( t0data$uvltd_0 ) == F & fn.nar(t0data$tpci,-99) == '0' & is.na(prccm_cumdv_3y$datenyp) ==F & prccm_cumdv_3y$datenyp > t0data$dateprc & t0data$mcap > 25
--use_0_prlm2 <- use_0_prlm1 == T & ( is.na(t0data$dpltd) == T | (t0data$dpltd >= t0data$uvltd_0) ) #this "catches" ~1.7% of sample we will not use because entry price seems to be from middle of month, rather than from the last tradingn date of the month 










SELECT COUNT(*) FROM a_mcapd_q0_t0data --6601 6/5/2020

select * from a_mcapd_q0_t0data
WHERE (sic < 6000 OR sic > 6999 ) AND (iid = primiss_iid OR primiss_iid IS NULL) AND eps/prccd > 0.1 AND dvi/prccd > 0.03--and tic = 'MMM' --iid = ISNULL(primiss_iid, iid2) --WHERE adrrc IS NOT NULL --WHERE iid = ISNULL(primiss_iid, iid2)
ORDER BY eps/prccd desc --84 rows 6/5/2020
 

--myquery = 'SELECT * FROM a_data_co_ifndq_ss WHERE qlag >= -11 ORDER BY gvd, iid, fq0date, fyr0, qlag DESC '
--#--in SQL, try: SELECT SUM (*) FROM V GROUP BY gvkey, iid, fq0date, fyr0
--ERROR MESSAGE: "Operand data type varchar is invalid for sum operator."

#############################
#startrcode rcodestart r-code
ch <- odbcConnect("xpressfeed", uid = "xpressfeed", pwd = "xpressfeed") 
setwd(r_ri.num)

#import data from SQL to R
#download SQL tables as r dfs
myviews <- c( 'a_data_co_idesind', 'a_data_co_ifndq', 'a_data_co_ifndytd', 'a_data_co_ifndytd_lag1q')
#mytables <- paste('a0_', substr(myviews, 4, nchar(myviews) ), sep = '')
mytables <- myviews

for ( i in 1:length(mytables) ) {
fn.asc(mytables[i], sqlQuery(ch, stringsAsFactors = F , query = paste( 'SELECT * FROM ', myviews[i], ' ORDER BY gvk, iid, fq0date, fyr0, qlag ', sep = '') ) ) 
} #end i
i

#primary key = gvk, iid, fq0date, fyr0, qlag


rm(myviews,mytables)

#subset a_data_co_idesind for qlag = 0 then save as a_data_co_idesind.0
fn.as('a_data_co_idesind.0', subset ( fn.flg('a_data_co_idesind'), a_data_co_ifndytd$qlag == 0) )
dim(a_data_co_idesind.0)


#calc single qtr data from ytd tables ytd = "year to date" and can contain 1 to 4 quarters of data
fn.flga('a_data_co_ifndytd')
fn.flga('a_data_co_ifndytd_lag1q')
head(a_data_co_ifndytd)[1:10]
head(a_data_co_ifndytd_lag1q)[1:10]
a_data_co_ifndytd_sq <- data.frame( sapply(fn.flg('a_data_co_ifndytd'), FUN= as.numeric)) - data.frame( sapply(fn.flg('a_data_co_ifndytd_lag1q'), FUN= as.numeric))

#cols_isnum <- sapply(a_data_co_ifndytd, FUN= is.numeric)
#a_data_co_ifndytd_sq[cols_isnum == F] <- a_data_co_ifndytd[cols_isnum == F]

#put back in primary key information
a_data_co_ifndytd_sq$gvk <- a_data_co_ifndytd$gvk
a_data_co_ifndytd_sq$iid <- a_data_co_ifndytd$iid
a_data_co_ifndytd_sq$fq0date <- a_data_co_ifndytd$fq0date
a_data_co_ifndytd_sq$fyr0 <- a_data_co_ifndytd$fyr0
a_data_co_ifndytd_sq$qlag <- a_data_co_ifndytd$qlag #qlag is how many quarters the data is lagged from the most recent quarter (q0)


###
#calculate the tr4 quarter sums of the data in dataframes 'a_data_co_ifndytd_sq' , 'a_data_co_ifndq', for tr 4 periods ending in quarter 0 thru -2
#calc tr4 data,then save as a_data_co_ifndytd_tr4.0, a_data_co_ifndq_tr4q.0, etc.

myfiles <- c('a_data_co_ifndytd_sq' , 'a_data_co_ifndq')
myqs <- 0:-2

#add f loop for 2 data files a_data_co_ifndytd_sq & a_data_co_ifndq

for ( f in 1: length(myfiles) ) {
mydata <- data.frame( sapply( fn.flg(myfiles[f] ), FUN= as.numeric) ) #sapply runs a functions accross all columns; this converst all data in all columns in this dataframe to numeric 

#add q loop for qlag 0 - m3, m4-m7, m8-m11
for ( k in 0: (length(myqs)-1) ) {

q_st <- 0 + (k * -4)
q_end <- -3 + (k * -4)

dim(mydata)
dim(a_data_co_ifndytd)

mydata_ss <- subset ( mydata, a_data_co_ifndytd$qlag <= q_st  & a_data_co_ifndytd$qlag >= q_end) #this includes in mydata_ss only the rows with qlg between q_st and q_end
dim(mydata_ss)
head(mydata_ss)[1:10]	

date()
#this sums the rows, grouped by gvk + iid + fq0date + fyr0 (i.e. all elementsof primary key except qlag);if an NA is present it is included and the aggregate function returns NA
myaggs <- aggregate( . ~ gvk + iid + fq0date + fyr0, data = mydata , FUN = 'sum', na.rm = F, simplify = TRUE, na.action=NULL) 

date()
dim(myaggs)
#
#this sums the rows, grouped by gvk + iid + fq0date + fyr0 ; if an NA is present it is removed/ignored
myaggs_nna <- aggregate( . ~ gvkey + iid + fq0date + fyr0, data = mydata , FUN = sum, na.rm = T, simplify = TRUE, na.action=NULL)  
date()
dim(myaggs)
#

fn.asr(paste(myfiles[f],'_tr4q.',k,sep='') , myaggs) #this saves then removes the files
fn.asr(paste(myfiles[f],'_nna_tr4q.',k,sep='') , myaggs_nna) #this saves then removes the files
} # end q loop
rm(mydata); gc()
} # end f loop
#

#run query to get t0data from sql ... see sql query above
SELECT COUNT(*) FROM a_mcapd_q0_t0data --6601 6/5/2020



#get t0date with prccm, mkt cap, pvol, etc.










#calc pvold30, trailing 30 day cshtrd cshtrd30










#myaggs_use_screen <- aggregate(trets ~ dates + screen, mydata, FUN = function(x, na.rm) c(mn = mean(x), md = median(x), n = length(x), ct = fn.count(x)), na.rm = T, simplify = TRUE, na.action=NULL, subset = use & screen)

#temp <- aggregate(mydata_qf_temp ~ gvkey + iid + fq0date + qlag, FUN = function(x, na.rm) c(mn = mean(x), md = median(x), n = length(x), ct = fn.count(x)), na.rm = T, simplify = TRUE, na.action=NULL, subset = use & screen)



