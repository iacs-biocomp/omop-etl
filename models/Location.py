from models.Base import Base
from models.models import Location as LOCATION

import config
import os
import pandas as pd
import utils.cache.cache as cache
from utils.OmopDB import OmopDB

class Location(Base):
    """This class transform and insert the data that represent a physical location of patients or heatlhcare places (hospitals, offices, clinics) """
    
    def __init__(self, cache):
        self.model=LOCATION
        self.domain='Location'
        self.caches=cache

    def mapData(self, df):

        lastPK=self.getLastPK()
        df['location_id']=range(lastPK+1,lastPK+1+len(df))

        return df

    def cleanData(self, df, module_name):
        return df

    def insertData(self, df):
        return super().insertData(df)

    def process(self, df):
        return super().process(df)