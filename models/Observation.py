from models.Base import Base
from models.models import Observation as OBSERVATION
import pandas as pd


class Observation(Base):
    """ This class transform and insert records that captures clinical facts about a Person 
    Also, this class contains an instruction to remove the duplicates of person_id, observation_date and observation_concept_id
     """
    def __init__(self, caches):
        self.model=OBSERVATION
        self.domain='Observation'
        self.caches=caches


    def mapData(self, df):

        domain=self.domain.lower()
        print("\t Mapping %s"%domain)

        primary_key = self.model.__table__.primary_key
    
        if primary_key is not None:
            lastPK = self.getLastPK()
        
            mask=df['domain']==self.domain
            df.loc[mask, 'id']=range(lastPK+1,lastPK+1+len(df[mask]))
            df['observation_id']=df.id
            
        
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


        if 'value_id' in df.columns and 'value_as_concept_id' not in df.columns:
            df['value_as_concept_id']=df['value_id'].astype('Int64')


        df['%s_type_concept_id'%domain]= df['type_concept_id']

       
        df['%s_datetime'%domain]=df['start_timestamp']
        df['%s_date'%domain]=df['%s_datetime'%domain].dt.date

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