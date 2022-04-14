import gzip
import json
import logging
import os
import shutil
import time
from base64 import encode
from datetime import date

import numpy as np
import pandas as pd
import pyodbc
import sqlalchemy
import sys

#%% LIBRARIES CONFIGURATION
logging.basicConfig(level='DEBUG')  
logging.getLogger("urllib3").setLevel(logging.WARNING)
pd.options.mode.chained_assignment = None


#%% SET DIRECTORIES
BASEDIR = os.getcwd()
CSVDIR = os.getcwd() + '/csv'
GZDIR = os.getcwd() + '/gzip'


with open(BASEDIR + '/src/config/exercise-atlax360.json') as json_file:
    data     = json.load(json_file)
    host     = data["HOST"]
    port     = data["PORT"]
    database = data["DATABASE"]
    user     = data["USER"]
    password = data["PASSWORD"]

class GenerateCSV():
    def __init__(self):
        try:
            self.conn = pyodbc.connect('DRIVER={SQL Server Native Client 11.0};SERVER=%s;PORT=%s;DATABASE=%s;UID=%s;PWD=%s;' % (host, port, database, user, password))
            
        except Exception as e:
            logging.error('Error creating SQL Server connection:')
            logging.error(e)
        
        self.dtype_customers = {
            'CustomerId': sqlalchemy.BIGINT,
            'CustomerName': sqlalchemy.VARCHAR(500)
        }
        self.dtype_items = {
            'ItemId': sqlalchemy.BIGINT,
            'VersionNbr': sqlalchemy.INT,
            'DeletedFlag': sqlalchemy.INT,
            'ItemDocumentNbr': sqlalchemy.VARCHAR(500),
            'CustomerId': sqlalchemy.BIGINT,
            'CreateDate': sqlalchemy.DATETIME,
            'UpdateDate': sqlalchemy.DATETIME
        }
    
    
    def db_exec(self, connection, command = "", commit = 0):
        # Initializes cursor object
        try:
            logging.debug('Initializing cursor...')
            cursor = connection.cursor()
        
        except Exception as e:
            logging.error('Cursor initialization error:')
            logging.error(e)
            raise
        
        logging.debug('Cursor initialized.')
        

        # Executes command passed from the command parameter
        try:
            logging.debug(f'Executing command: \n {command}')
            tic = time.time()
            cursor.execute(command)
        except Exception as e:
            logging.error('Command execution error:')
            logging.error(e)
            raise
        
        logging.debug('Command executed. (%.2f seconds)' % (time.time()-tic))
        

        # Commits if needed
        if commit == 0:
            return cursor

        else:
            
            try:
                tic = time.time()
                cursor.commit()
            except Exception as e:
                logging.error('Error committing to DB:')
                logging.error(e)
                raise
            
        logging.debug(f'Changes Committed. {cursor.rowcount} rows affected. ({(time.time()-tic)})')

        cursor.close()
        return
        
        
    def check_customer_table(self):
        sql = """
            IF NOT EXISTS (
                SELECT * 
                FROM INFORMATION_SCHEMA.TABLES
                WHERE table_name = 'Customer'
            )
            CREATE TABLE [ATLAX360_HI_DB].[dbo].[Customer](
                [CustomerId] [bigint] NOT NULL,
                [CustomerName] [varchar](500) NOT NULL,
            PRIMARY KEY CLUSTERED 
            (
                [CustomerId] ASC
            )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
            ) ON [PRIMARY]    
        """
        self.db_exec(self.conn, sql, commit=1)
        
        
    def check_items_table(self):
        sql = """
            IF NOT EXISTS (
                SELECT * 
                FROM INFORMATION_SCHEMA.TABLES
                WHERE table_name = 'Item'
            )
            CREATE TABLE [ATLAX360_HI_DB].[dbo].[Item](
                [ItemId] [bigint] NOT NULL,
                [VersionNbr] [int] NOT NULL,
                [DeletedFlag] [tinyint] NOT NULL,
                [ItemDocumentNbr] [varchar](500) NOT NULL,
                [CustomerId] [bigint] NULL,
                [CreateDate] [datetime] NOT NULL,
                [UpdateDate] [datetime] NOT NULL
            ) ON [PRIMARY]

            ALTER TABLE [dbo].[Item]  WITH CHECK ADD FOREIGN KEY([CustomerId])
            REFERENCES [dbo].[Customer] ([CustomerId])
        """
        
        self.db_exec(self.conn, sql, commit=1)
    
    
    def get_data_from_database(self):
        sql = """
            SELECT i.ItemId,
                i.ItemDocumentNbr,
                c.CustomerName,
                i.CreateDate,
                i.UpdateDate,
                i.DeletedFlag,
                i.VersionNbr
            FROM ATLAX360_HI_DB.dbo.Item i 
                JOIN Customer c ON i.CustomerId = c.CustomerId
            WHERE i.DeletedFlag = 0 AND
                i.ItemId  BETWEEN 1 AND 20 and 
                i.VersionNbr = (
                    SELECT MAX(VersionNbr)
                    FROM ATLAX360_HI_DB.dbo.ItemCopy
                    WHERE DeletedFlag = 0 AND
                    ItemId = i.ItemId
                )
            ORDER BY ItemId           
        """
        df = pd.read_sql(sql, self.conn)
        df['ItemSource'] = np.where(df['CustomerName'].str[:2] == '99', 'Local', 'External')
        
        logging.debug(df)
        return df
    
    def export_csv(self, df):
        current_date = date.today().strftime("%Y-%m-%d")
        
        # Obtener fecha de export
        filename = 'items-' + current_date + '.csv'
        filepath = CSVDIR + filename
        df.to_csv(filepath, sep=';', index=False, encode="utf-8", line_terminator="CRLF") # Quotechar is omitted because default values is " 
        
        if not os.path.exists(filepath):
            raise Exception("\n\n Error creating file \n\n")
        
        logging.info("\n\n File created! \n\n")
        
        return filename
        
    
    def compress_file(self, filename):
        
        filepath = GZDIR + filename
        
        if not os.path.exists(GZDIR):
            os.mkdir(GZDIR)
        
        with open(filepath, 'rb') as file_in:
            with gzip.open(filepath + '.gz', 'wb') as file_out:
                shutil.copyfileobj(file_in, file_out)
        
        if not os.path.exists(filepath):
            raise Exception("\n\n Error creating gzip file!! \n\n")
    
    def main(self):
        try:
            # Check if table exist
            self.check_customer_table()
            self.check_items_table()
            
            # Get dataframe with table rows
            df_items = self.get_data_from_database()
            
            # Export dataframe to csv
            filename = self.export_csv(df_items)
            
            # Compress csv file into gzip
            self.compress_file(filename)
            
        except Exception as e:
            logging.error(e)
            return 1


if __name__ == '__main__':
    generateCSV = GenerateCSV()
    generateCSV.main()
