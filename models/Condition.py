from functools import cache
from models.Base import Base
from models.models import ConditionOccurrence
import pandas as pd

class Condition(Base):
    """ This class is inserting all the records of  events of a person
    suggesting the presence of a disease or medical condition stated as a diagnosis, a sign or symtom
     """
   
    def __init__(self, caches):
        self.model=ConditionOccurrence
        self.domain='Condition'
        self.caches=caches



    def mapData(self, df):
        
        domain=self.domain.lower()
        print("\t Mapping %s"%domain)

        primary_key=None

        if hasattr(self.model, '__table__' ):
            primary_key = self.model.__table__.primary_key
    
        if primary_key is not None:
            lastPK = self.getLastPK()

            mask=df['domain']==self.domain
            df.loc[mask, 'id']=range(lastPK+1,lastPK+1+len(df[mask]))
            df['condition_occurrence_id']=df.id
            
        
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
        
        df['%s_type_concept_id'%domain]= df['type_concept_id']

        df['%s_start_datetime'%domain]=df['start_timestamp']
        df['%s_start_date'%domain]=df['%s_start_datetime'%domain].dt.date
        df['%s_end_datetime'%domain]=df['end_timestamp']
        df['%s_end_date'%domain]=df['%s_end_datetime'%domain].dt.date
        df['%s_type_concept_id'%domain]= df['type_concept_id']

   


      

        return super().mapData(df)

    def cleanData(self, df, module_name):
        

        df=self.reviewErrors(df, 'person_id', module_name, dropErrors=True)
        df=self.reviewErrors(df, 'condition_start_date', module_name, dropErrors=True)
        df=self.reviewErrors(df, 'condition_concept_id', module_name, dropErrors=False)
        
        checkList=[('person_id', True),('condition_start_date', True) ,('condition_start_date', False)]
       
        return df

    def insertData(self, df):
        return super().insertData(df)