from models.Base import Base
import pandas as pd
import importlib
from config import visit_scripts, log_dir
from models.models import  VisitOccurrence2 as VisitOccurrence
from utils.OmopDB import OmopDB


class Visit(Base):
    """ This is the class for Visit_occurrence,  the table contains Events where Persons engage with the healthcare system for a duration of time.
    Aslo, this class does the mapping the vocabularies to standard OMOP """

   
    subjobs = []
    model=VisitOccurrence
    model_name='visit_occurrence2'
    """ The records will be allocated in the table VISIT_OCCURRENCE  """
    
        

    def mapData(self, df):
        if 'visit' in df.columns:
            df['visit_source_value']=df['visit']
        
        lastPK=self.getLastPK()
        df['visit_occurrence_id']=range(lastPK+1,lastPK+1+len(df))

        df['visit_start_datetime']=df['start_timestamp']
        df['visit_start_date']=df['visit_start_datetime'].dt.date
        df['visit_end_datetime']=df['end_timestamp']
        df['visit_end_date']=df['visit_end_datetime'].dt.date
        df['visit_type_concept_id']= df['type_concept_id']

        return super().mapData(df)
    


        
   
    def cleanData(self, df, module_name='visit'):
        df=self.reviewErrors(df,'person_id', module_name, True)
        return super().cleanData(df, module_name)


    def getSnomedVisitDomain(self):
        """ This method checks the Snomed domains and inserts the ones that are having the target_domain_id as visit """
        
        vocab=self.getVocab('SNOMED')
        mask=vocab['TARGET_DOMAIN_ID']=='Visit'
        cdf=vocab[mask]
        return pd.Series(cdf['TARGET_CONCEPT_ID'],index=cdf['SOURCE_CONCEPT_ID'].astype(str)).to_dict()
