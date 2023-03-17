
import config
from sqlalchemy import create_engine



class Database():
    
    type=None

   
    
    def __new__(cls, *args):
        if cls is Database:
            cls.type=args[0]
            if config.omop_db_type =='postgresql':
                from utils.postgres import Postgres
                return Postgres(*args)
            if config.omop_db_type =='redshift':
                from utils.redshift import Redshift
                return Redshift(*args)
        return object.__new__(cls)
    
    def create_engine(self):
        """ This method starts the conextion with the databases"""

        if self.type=='origin':
            return self.create_omop_engine()
        if self.type=='omop':
            return self.create_omop_engine()
    
    
    def create_origin_engine( self):
        """ This method starts the conextion with the source database """

        origin_engine = create_engine(config.origin_connection_string)
        origin_engine=origin_engine.connect().execution_options(stream_results=True)  
        return origin_engine
    
    def create_omop_engine(self):
        """ This method starts the connection with Omop databases"""

        omop_engine=create_engine(config.omop_connection_string)
        omop_engine=omop_engine.connect().execution_options(stream_results=True)
        return omop_engine