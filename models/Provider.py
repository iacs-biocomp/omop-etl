from models.Base import Base
from models.models import Provider as PROVIDER
import config
import os
import pandas as pd
import utils.cache.cache as cache
from utils.OmopDB import OmopDB

class Provider(Base):

    def __init__(self, caches):
        self.model=PROVIDER
        self.domain='Provider'
        self.caches=caches

    def mapData(self, df):

        lastPK=self.getLastPK()
        df['provider_id']=range(lastPK+1,lastPK+1+len(df))

        df=self.mapLocation(df)

        return super().mapData(df)

    def cleanData(self, df, module_name):
        return super().cleanData(df, module_name)

    def insertData(self, df):
        return super().insertData(df)

    def process(self, df):
        return super().process(df)


    def mapLocation(self, df):
        """ This method is mapping the location_id and insert the right location in the table Provider """
        
        if 'location' not in df.columns:
            return df
        
        if 'location' not in self.caches:
            import utils.cache.cache as cache
            
            self.caches.set('location',cache.load_cache('location',self.getOmopConnection()))
            


        df['location_id']=df.location.map(self.caches['location'].get)
        df['location_id']=df['location_id'].astype('Int64')   
        return df