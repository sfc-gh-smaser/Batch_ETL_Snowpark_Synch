#This sample does batch ETL Operations for On-Premise to Snowflake table Sychronizations

# DB2
#from sqlalchemy import create_engine
# Snowflake
from snowflake.snowpark.session import Session,Row
    #from snowflake.snowpark import functions as F
    #from snowflake.snowpark.types import *
# The Rest
import configparser
import pandas as pd
from datetime import datetime
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization

#----------SETUP---------------------------------------------------
class db2:
    session=None
    conf=None
    def __init__(self,configFile):
        config = configparser.RawConfigParser()
        config.read(configFile)
        self.conf = dict(config.items('DB2'))
        print("DB2 conf:  "+str(self.conf))
        #self.session = create_engine("db2:///?Server="+db2_conf["server"]+"&;Port="+db2_conf["port"]+"&User="+db2_conf["user"]+"&Password="+db2_conf["password"]+"&Database="+db2_conf["database"])
        self.session=""
        print("DB2 session created")
        print(str(self.session))

class sf:
    session=None
    conf=None
    def __init__(self,configFile):
        config = configparser.RawConfigParser()
        config.read(configFile)
        self.conf = dict(config.items('SNOWFLAKE'))
        print("Snowflake conf:  "+str(self.conf))
        with open(self.conf["private_key"], "rb") as key:
            p_key= serialization.load_pem_private_key(
                key.read(),
                #password=os.environ['PRIVATE_KEY_PASSPHRASE'].encode(),
                password=None,
                backend=default_backend()
            )

        pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption())
        self.conf["private_key"]=pkb

        self.session = Session.builder.configs(self.conf).create()
        print("Snowflake session created")
        setup=self.session.sql('select current_warehouse(), current_database(), current_schema()').collect()
        print("Session:  "+str(setup[0]))
        

def synchTable(s,d,table,keycol,lastRunTime,nowTime):
    try:
        #----------DB2-(Get Change Data via query)----------------------------------------------------
        #df = pandas.read_sql("select * from "+table+" where last_changed_timestamp>lastRunTime AND last_changed_timestamp<="+nowTime, d.session)
        ## Simulate data instead
        data = [[10,"newvalue"] , [12,"newrow"]]
        df = pd.DataFrame(data, columns=['KEY', 'VALUE'])
        
        # if no changes, can stop here
        if(len(df.index)==0): return str([0,0,0])
        
        #-----------SNOWFLAKE-----------------------------------------------
        #Create Snowflake dataframe from Pandas dataframe that contains DB2 data
        changes = s.session.create_dataframe(df)

        #Save DB2 data into Snowflake Temp Table
        t=table+"_TEMP"
        changes.write.save_as_table(t, mode="overwrite", table_type="temporary")

        # Merge Changes into Matching Primary Table (call stored procedure)
        result=s.session.call("mergeTables",t,table,keycol)
        #Results of Merge (Inserts, Updates, Deletes)
        return result
    except Exception as e:
        print(e)
        return "Failed:  "+e.message


if __name__ == "__main__":
    db2_conn=db2("etl.properties")
    sf_conn=sf("etl.properties")
    etl_history_table="ETL_HISTORY"
    nowTime=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("Cutoff Time for Synchronization:  "+nowTime)

    #get list of tables to synchronize from Snowflake (contains table name, primarykey column name, lastRunTime)
    history = sf_conn.session.table(etl_history_table).collect()

    #create status list
    results=[]

    #loop through the list of tables to sychronize (one at a time)
    for row in history:
        tablename=row["TABLENAME"]
        keycol=row["PRIMARYKEY"]
        lastRunTime=row["LASTRUNTIME"]
        print("  Starting Table Sychronization for table: "+tablename+"["+keycol+","+str(lastRunTime)+"]")
        result=synchTable(sf_conn,db2_conn,tablename,keycol,lastRunTime,nowTime)
        print("  Table "+tablename+" Sychronized:  "+str(result).replace("\n",''))
        
        #log result of sychronize
        results.append([tablename,keycol,nowTime,result])
    print("Table Sychronization Completed")
    
    summary=sf_conn.session.create_dataframe(results, schema=["tablename", "primarykey","lastRunTime","lastRunInfo"])
    #Sychronize table status in Snowflake
    summary.write.save_as_table(etl_history_table+"_TEMP", mode="overwrite", table_type="temporary")
    etl_results=sf_conn.session.call("mergeTables",etl_history_table+"_TEMP",etl_history_table,"TABLENAME")
    print("Table Sychronization Recorded for "+str(len(results))+" Tables")



#https://community.snowflake.com/s/article/How-to-create-a-session-via-Snowpark-python-using-key-pair-authentication-in-jupyter
#Using AWS?  Can store certificate in Secrets Mgr:
#https://community.snowflake.com/s/article/How-to-retrieve-key-pair-authentication-from-AWS-Secrets-Manager-when-using-Snowflake-Python-Connector