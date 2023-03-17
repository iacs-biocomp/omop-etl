from models.Base import Base
from models.models import DeviceExposure 
import pandas as pd


class Device(Base):
    """ This class transform all the data that capture information about person's exposure to a foreign physical object
    """
    def __init__(self, caches):
        self.model=DeviceExposure
        self.domain='device'
        self.caches=caches


    def mapData(self, df):

        domain=self.domain.lower()
        print("\t Mapping %s"%domain)

        primary_key=None

        if hasattr(self.model, '__table__' ):
            primary_key = self.model.__table__.primary_key
        #TODO null value in column "device_exposure_id" of relation "device_exposure" violates not-null constraint
        lastPK=self.getLastPK()
        df['device_exposure_id']=range(lastPK+1,lastPK+1+len(df))
           
            
        
        df=self.create_default_domain_columns(df, domain)

        if 'concept_id' in df.columns:
            mask= pd.isna(df['%s_concept_id'%domain])
            df.loc[mask,'%s_concept_id'%domain]=df['concept_id']
            df['%s_concept_id'%domain]=df['%s_concept_id'%domain].astype('Int64')

        if 'concept' in df.columns:
            mask= pd.isna(df['%s_source_value'%domain])
            df.loc[mask,'%s_source_value'%domain]=df['concept']

        if 'source_concept_id' in df.columns:
            mask= pd.isna(df['%s_source_concept_id'%domain])
            df.loc[mask,'%s_source_concept_id'%domain]=df['source_concept_id']
            df['%s_source_concept_id'%domain]=df['%s_source_concept_id'%domain].astype('Int64')
            
        df['device_type_concept_id']= df['type_concept_id']

       
        df['device_exposure_start_datetime']=df['start_timestamp']
        df['device_exposure_start_date']=df['device_exposure_start_datetime'].dt.date

        df['device_exposure_end_datetime']=df['end_timestamp']
        df['device_exposure_end_date']=df['device_exposure_end_datetime'].dt.date

        return super().mapData(df)


    def cleanData(self, df, module_name):
        

        df=self.reviewErrors(df, 'person_id', module_name, dropErrors=True)
        df=self.reviewErrors(df, 'observation_date', module_name, dropErrors=True)
        df=self.reviewErrors(df, 'observation_concept_id', module_name, dropErrors=False)
        
       
        return df

    def insertData(self, df):
        return super().insertData(df)

    def process(self, df):
        return super().process(df)