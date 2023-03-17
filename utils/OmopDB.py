import config
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.engine import Connection
import pandas as pd
from tqdm import tqdm

from utils.Database import Database

class OmopDB():

    _session=None

   
    def getSession(self):
        """This method starts the connection with OMOPDb """ 
        
        if self._session is None:
            return self._create_stream_connection()
        return self._session

    def getConnection(self):
        """This method creates the connection with the database"""

        return self._create_connection()


    def _create_stream_connection(self) -> Connection:
        """This method creates the connection with the database"""
        return self._create_connection().execution_options(stream_results=True)

    def _create_connection(self) -> Connection:
        """This method creates the connection with the OMOP database"""
        omop_engine=self._create_engine()
        omop_connection=omop_engine.connect()
        return omop_connection

    def _getBulkEngine(self):
        db = Database('omop')
        return db

    def _create_engine(self) -> Engine:
        """ This method starts the connection with the database """

        omop_engine=create_engine(config.omop_connection_string)
        return omop_engine

    def close(self):
        """ This method closes the connection with the database """
        if self._session is not None:
            self._session.close()

        self._session = None

    def chunker(self, seq, size):
        """ This method splits the dataframe """
        # from http://stackoverflow.com/a/434328
        return (seq[pos:pos + size] for pos in range(0, len(seq), size))

    def insert_with_progress(self, df, model, columns=None): 
        """ This methods inserts data from the dataframe and it also shows a bar graphic with the task progress """
        #funcion que inserta el dataframe
        chunksize = int(len(df) / 10) # 10%
        if chunksize==0:
            chunksize=1

        
        with tqdm(total=len(df)) as pbar:
            for i, cdf in enumerate(self.chunker(df, chunksize)):
               

                if config.useBulkInsert:
                    db = self._getBulkEngine()
                    db.bulk_insert(cdf, model.__tablename__,model, schema=config.omop_db_schema ,index=False,columns=columns)
                else:
                    conn = self.getSession()
                    cdf.to_sql(con=conn, schema=config.omop_db_schema ,name=self.model_name.lower(), if_exists='append', index=False)
                    conn.close()
                
               
                pbar.update(chunksize)
        
    
    def getLastPK(self, model):
        con=self.getConnection()
        table='omop.%s'%model.__tablename__
        id_name=model.__table__.primary_key.columns.values()[0].description

        query='select max(%s) from %s'%(id_name,table)

        df=pd.read_sql(query, con)

        for index,row in df.iterrows():
            return row['max']

   