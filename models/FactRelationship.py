from models.Base import Base
from models.models import t_fact_relationship as FactRelationship

class Fact_Relationship(Base):
    """ This class creates a relationship between two tables that does not have commum data"""
    
    def __init__(self, caches):
        self.model=FactRelationship
        self.caches=caches
        

    def cleanData(self, df):  
        return df

    def insertData(self, df):
        return super().insertData(df)