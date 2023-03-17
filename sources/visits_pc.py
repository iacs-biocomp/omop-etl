import pandas as pd
from models.Base import Base
from models.Visit import Visit
from models.Person import Person
from time import gmtime, strftime 
import os
import config 
from utils.casandra import Cassandra
import numpy as np


class visits_pc(Base):

    """ This class contains the logic of reading, extrating and transforming
     patient information to pass it to the OMOP CDM model """

   

  


    def run(self):
        """ Run the job"""
        print("launch %s"%self.source_module)
        print("Getting data from source")     
        print(strftime("%H:%M:%S", gmtime()))  
        
        cassandra=Cassandra()

        df=pd.DataFrame()

        query = 'select distinct event_yr, event_mth from visits_pc '
        cdf=cassandra.cas2pandas(query)
        cdf=cdf[(cdf.event_yr >=config.startyear) & (cdf.event_yr <=config.endyear)]
        cdf=cdf.reset_index()
        
       

        for index, c in cdf.iterrows():
          query = "select * from visits_pc  where  event_yr=%s and event_mth=%s "%(c.event_yr, c.event_mth )
          visit=cassandra.cas2pandas(query)
          df=pd.concat([df, visit])

          if len(df)>=config.chunksize or index == cdf.index[-1]:  
        
            print("Mapping data")
            print(strftime("%H:%M:%S", gmtime())) 
            
            df['origin_source_data_provider']='biganlake'
            df['origin_source_name']='visits_pc'
            df['origin_source_value']='No pk'

            
            df.rename(columns={'person_id':'person'}, inplace=True)
            df['type_concept_id']=32817

            df['attending_cias_cd']=df['attending_cias_cd'].replace(r'^\s*$', np.nan, regex=True)
            df['provider']=df['attending_cias_cd']
            df['care_site']=df['patient_cias_cd']
            df=self.mapCare_Site(df)
  
            #TODO REVISAR en origen si faltan providers
            p=Person(self.caches)
            providerMap=p.getProviderMap()
            df=self.mapPerson(df)
            df['provider_id']=None
            df['provider_id']= df['provider_id'].fillna(df.person_id.astype(str).map(providerMap.get))
            df['provider_id']=df['provider_id'].astype('Int64')

            caresiteMap=p.getCareSiteMap()
            df['care_site_id']=None
            df['care_site_id']= df['care_site_id'].fillna(df.person_id.astype(str).map(caresiteMap.get))
            df['care_site_id']=df['care_site_id'].astype('Int64')


            df['start_timestamp']=pd.to_datetime(df.event_dt.astype(str),errors='coerce', infer_datetime_format=True, utc=True).dt.tz_convert("Europe/Madrid")
           
            #clean errors
            df=self.reviewErrors(df, 'start_timestamp',self.source_module, dropErrors=True)

            df['end_timestamp']=df['start_timestamp']
            
            
            df['visit_concept_id']=df['visittype_cd'].str.strip().map({'004':581476, '005':581476, '007':581476, }.get)
            df['visit_concept_id']=df['visit_concept_id'].fillna(9202)
            df['visit_concept_id']=df['visit_concept_id'].astype('Int64')
            df['visit_source_concept_id']=0
            df['visit_source_value']=df['visittype_cd']
            #df.loc[df['visit_concept_id']==9202, 'visit_source_value']='visits_pc'



            print("Procesing Visits")
            print(strftime("%H:%M:%S", gmtime())) 
            
            vs=Visit(self.caches)
            vs.source_module=self.source_module
            df=vs.process(df)
          

            df=pd.DataFrame()



            
            
            

         