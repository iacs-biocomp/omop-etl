from models.Base import Base
from models.models import DrugExposure
import pandas as pd



class Drug(Base):
    """ This class transform and insert all the dta records to a Drug ingested or introduced into the body. 
    Also, this class identify the errors about person_id, drug_exposure_start_date and drug_concept_id
    """
    def __init__(self, caches):
        self.model=DrugExposure
        self.domain='Drug'
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
            df['drug_exposure_id']=df.id
            
        
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
        
        df['%s_concept_id'%domain]= df['concept_id'].astype('Int64')

        df['drug_exposure_start_datetime']=df['start_timestamp']
        df['drug_exposure_start_date']=df['drug_exposure_start_datetime'].dt.date
        df['drug_exposure_end_datetime']=df['end_timestamp']
        df['drug_exposure_end_date']=df['drug_exposure_end_datetime'].dt.date
        df['drug_type_concept_id']= df['type_concept_id']

   


        # if 'care_site' in df.columns:
        #     df['care_site_id']=df['care_site'].map(self.load_cache('care_site')).astype('Int64')  
        # if 'provider' in df.columns:
        #     df['provider_id']=df['provider'].map(self.load_cache('provider')).astype('Int64')


        return super().mapData(df)

    def cleanData(self, df, module_name):
        

        df=self.reviewErrors(df, 'person_id', module_name, dropErrors=True)
        df=self.reviewErrors(df, 'drug_exposure_start_date', module_name, dropErrors=True)
        df=self.reviewErrors(df, 'drug_concept_id', module_name, dropErrors=False)
        
       
        return df

    def insertData(self, df):
        return super().insertData(df)

    def mapRoutes(self, df):
      df['route_concept_id']=None
      if 'ROUTES' not in self.caches:
        vocab=self.getVocab('ROUTES')  

        self.caches.set('ROUTES',pd.Series(vocab['TARGET_CONCEPT_ID'].values,index=vocab['SOURCE_CODE'].astype(str)).to_dict())
    
      df['route_concept_id']=df['route_concept_id'].fillna(df.route.map(self.caches['ROUTES']))
      df['route_concept_id']=df['route_concept_id'].astype('Int64') 
      df['route_source_value']=df['route']
      df['route_source_concept_id']=0

      return df



    



