from models.Measurement import Measurement
import pandas as pd
from models.Base import Base
from time import gmtime, strftime 
from re import compile as re_compile
import os
import config 
from  utils.casandra import Cassandra



class measurement_laboratory_cdm(Base):

    """ This class contains the logic of reading, extrating and transforming
     patient information to pass it to the OMOP CDM model """

    numberRegex = re_compile("^\d+?\.\d+?$")    

  


    def run(self):
        """ Run the job"""
        print("launch %s"%self.source_module)
        print("Getting data from source")     
        print(strftime("%H:%M:%S", gmtime()))  
        
                
        cassandra=Cassandra()

        df=pd.DataFrame()

        query = 'select distinct event_yr, event_mth, pobs_id from laboratory_cdm'
        cdf=cassandra.cas2pandas(query, keyspace='bigancdm')
        cdf=cdf[(cdf.event_yr >=config.startyear) & (cdf.event_yr <=config.endyear)]
        cdf=cdf.reset_index()
        
       

        for index, c in cdf.iterrows():
          query = "select * from laboratory_cdm  where  event_yr=%s and event_mth=%s  and pobs_id ='%s' "%(c.event_yr, c.event_mth, c.pobs_id )
          measurement=cassandra.cas2pandas(query, keyspace='bigancdm')
          df=pd.concat([df, measurement])

          if len(df)>=config.chunksize or index == cdf.index[-1]:

            print("Mapping data")
            print(strftime("%H:%M:%S", gmtime())) 
            
            df['origin_source_data_provider']='bigancdm'
            df['origin_source_name']='laboratory_cdm'
            df['origin_source_value']=df['obs_id']

            
            df.rename(columns={'person_id':'person'}, inplace=True)
            
            

            df['concept']=df.std_observable_cd
            df['type_concept_id']=32817

            df['start_timestamp']=pd.to_datetime(df.obs_result_dt.astype(str),errors='coerce', infer_datetime_format=True, utc=True).dt.tz_convert("Europe/Madrid")
            df=self.reviewErrors(df, 'start_timestamp',self.model_name, dropErrors=True)

            df['end_timestamp']=df['start_timestamp']
            
            df['value_as_number']=df.obs_value_nm
            #TODO hablar con Minerva, existen numeros que superan los 50 caracteres
            #df['value_source_value']=df.obs_value_nm 
            df['unit']=df.unit_st
            
            
            df['visit']=df.req_id
            self.mapVisit(df, 'all')
          

            df=self.mapConcepts(df,vocabularios=['LOINC'] )
            c=Measurement(self.caches)
            df=c.mapUnits(df, vocabulariosUnidades=['UCUM','SNOMED', 'LOINC', 'Nebraska Lexicon', 'unidades'])
            
            df['domain']=df['domain'].fillna('Measurement')
            self.sendToDomains(df)

            

            df=pd.DataFrame()




         