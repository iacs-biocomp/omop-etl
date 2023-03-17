import pandas as pd
from models.Base import Base
from models.Visit import Visit
from time import gmtime, strftime 
import os
import config 
from utils.casandra import Cassandra


class visits_sc(Base):

    """ This class contains the logic of reading, extrating and transforming
     patient information to pass it to the OMOP CDM model """

 

  


    def run(self):
        """ Run the job"""
        print("launch %s"%self.source_module)
        print("Getting data from source")     
        print(strftime("%H:%M:%S", gmtime()))  
        
                
        cassandra=Cassandra()

        df=pd.DataFrame()

        query = 'select distinct event_yr, event_mth from visits_sc '
        cdf=cassandra.cas2pandas(query)
        cdf=cdf[(cdf.event_yr >=config.startyear) & (cdf.event_yr <=config.endyear)]
        cdf=cdf.reset_index()
        
       
        #TODO hacer un script para poder comprobar en origen
        for index, c in cdf.iterrows():
          query = "select * from visits_sc  where  event_yr=%s and event_mth=%s "%(c.event_yr, c.event_mth )
          visit=cassandra.cas2pandas(query)
          df=pd.concat([df, visit])

          if len(df)>=config.chunksize or index == cdf.index[-1]: 

            df['origin_source_data_provider']='biganlake'
            df['origin_source_name']='visits_sc'
            df['origin_source_value']=df['visit_id']

            print("Mapping data")
            print(strftime("%H:%M:%S", gmtime())) 
            
            
            df['visit_source_value']='visit_sc'
            df.rename(columns={'person_id':'person'}, inplace=True)
            df['type_concept_id']=32817

            df['provider']= df['facility_id'] + df['filler_cd']
            df['provider']=df['provider'].fillna(df['facility_id'])
           
            df['start_timestamp']=pd.to_datetime(df.event_dt.astype(str),errors='coerce', infer_datetime_format=True, utc=True).dt.tz_convert("Europe/Madrid")
           
            #clean errors
            df=self.reviewErrors(df, 'start_timestamp',self.model_name, dropErrors=True)

            df['end_timestamp']=df['start_timestamp']
            
            
            df['visit_concept_id']=9202 
            df['visit_source_concept_id']=0

            
            print("Procesing Visits")
            print(strftime("%H:%M:%S", gmtime())) 
            vs=Visit(self.caches)
            vs.source_module=self.source_module
            df=vs.process(df)
          

            df=pd.DataFrame()



            
            
            

         