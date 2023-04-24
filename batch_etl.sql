//setup Snowflake configuration
use role accountadmin;
create role if not exists ETL_PROCESSOR;
create or replace user etl_1 COMMENT="Creating for ETL DB2 Pipeline";
//alter user etl_1 set rsa_public_key='<Paste Your Public Key Here>';

use database VHOL_ENG_CDC;

grant role ETL_PROCESSOR to user etl_1;
grant usage on warehouse DEMO_WH to role ETL_PROCESSOR;
grant usage on database VHOL_ENG_CDC to role ETL_PROCESSOR;

use role vhol;
create or replace schema DB2;
grant usage on schema DB2 to role ETL_PROCESSOR;
grant CREATE TABLE,CREATE STAGE,CREATE PROCEDURE,CREATE FILE FORMAT on schema DB2 to role ETL_PROCESSOR;

use role ETL_PROCESSOR;
use VHOL_ENG_CDC.DB2;


// Simulate Initial Load of DB2 Tables
create or replace table MYTEST ("KEY" int, "VALUE" varchar);
INSERT into MYTEST VALUES(10,'oldvalue');
INSERT into MYTEST VALUES(11,'ok');

create or replace table MYTEST2 ("KEY" int, "VALUE" varchar);
INSERT into MYTEST2 VALUES(10,'oldvalue');
INSERT into MYTEST2 VALUES(11,'ok');

select * from mytest;


// Create ETL History Table to track which tables to sychronized and when it last occurred
create or replace table ETL_HISTORY("TABLENAME" VARCHAR, "PRIMARYKEY" VARCHAR,"LASTRUNTIME" TIMESTAMP,"LASTRUNINFO" VARCHAR);
INSERT into ETL_HISTORY VALUES('MYTEST','KEY','2023-01-01',null);
INSERT into ETL_HISTORY VALUES('MYTEST2','KEY','2023-01-01',null);
select * from ETL_HISTORY;


-- Create Procedure that performs ELT Merge from updated source table to target
CREATE OR REPLACE PROCEDURE mergeTables(sourcetable string, targettable string,keycol string) 
  RETURNS Variant
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.8'
  PACKAGES = ( 'snowflake-snowpark-python')
  HANDLER = 'main'
  EXECUTE AS CALLER
as
$$
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import when_matched, when_not_matched
def main(session: snowpark.Session,sourcetable,targettable,keycol):
    target = session.table(targettable)
    source = session.table(sourcetable)
    
    #column mapping(auto-map as tables should be identical)
    cols = {}
    for c in target.schema.names: cols[c] = source[c]

    result=target.merge(source, (target[keycol] == source[keycol]),[
        when_matched().update(cols), 
        when_not_matched().insert(cols)
    ])
    
    #MergeResultArray[inserted,updated,deleted]
    return result
$$;

//Show out-of-date table contents
select * from mytest;


//*** Run Python Batch Script to Sychronized **//

//Show updated/current table contents
select * from mytest;
//show etl history
select * from ETL_HISTORY;

