from models.Measurement import Measurement
import pandas as pd
from models.Base import Base
from time import gmtime, strftime 
import os
import config 
from utils.casandra import Cassandra
import utils.cache.cache as cache

class measurement_er_clinic(Base):

    """ This class contains the logic of reading, extrating and transforming
     patient information to pass it to the OMOP CDM model """


    def run(self):
        """ This method runs the job """
        
        print("launch %s"%self.source_module)
        print("Getting data from source")     
        print(strftime("%H:%M:%S", gmtime()))  
        
                
        cassandra=Cassandra()

        df=pd.DataFrame()

        query = 'select distinct event_yr from er_clinic '
        cdf=cassandra.cas2pandas(query)
        cdf=cdf[(cdf.event_yr >=config.startyear) & (cdf.event_yr <=config.endyear)]
        cdf=cdf.reset_index()
        
       

        for index, c in cdf.iterrows():
          query = "select * from er_clinic  where  event_yr=%s"%(c.event_yr )
          measurement=cassandra.cas2pandas(query)
          df=pd.concat([df, measurement])

          if len(df)>=config.chunksize or index == cdf.index[-1]:

            print("Mapping data")
            print(strftime("%H:%M:%S", gmtime())) 
            

            
            
            
            df.rename(columns={'person_id':'person'}, inplace=True)
            
            

            df=df.melt(id_vars=['person', 'id', 'facility_cd', 'event_dt', 'event_yr'], var_name='concept')
            df=df.dropna()
            df['value_as_number']=df['value']
          

            #TODO REVISAR implementar mapeos de unidades desde el stcm
            df=self._mapUnitStcm(df)
            df['unit_source_value'] = df['concept']

            df['visit']=df['id']
            self.mapVisit(df, 'visits_er')

            df['type_concept_id']=32817

            
            df['start_timestamp']=pd.to_datetime(df.event_dt.astype(str),errors='coerce', infer_datetime_format=True, utc=True).dt.tz_convert("Europe/Madrid")
            

            #clean errors
            df=self.reviewErrors(df, 'start_timestamp',self.model_name, dropErrors=True)

            df['end_timestamp']=df['start_timestamp']
            
            df=self.mapConcepts(df,vocabularios=['er_measurement'] )
            c=Measurement(self.caches)
            df=c.mapUnits(df, vocabulariosUnidades=['UCUM','SNOMED', 'LOINC', 'Nebraska Lexicon', 'unidades'])
            
            df['domain']=df['domain'].fillna('Measurement')

            df['origin_source_data_provider']='biganlake'
            df['origin_source_name']='er_clinic'
            df['origin_source_value']=df['id']
            self.sendToDomains(df)

            

            df=pd.DataFrame()


    def _mapUnitStcm(self, df):
      df['unit_concept_id']=None
      if 'er_stcm_unit' not in self.caches:
        vocab=self.getVocab('er_measurement_unit')  

        self.caches.set('er_stcm_unit',pd.Series(vocab['TARGET_CONCEPT_ID'].values,index=vocab['SOURCE_CODE'].astype(str)).to_dict())
    
      df['unit_concept_id']=df['unit_concept_id'].fillna(df.concept.map(self.caches['er_stcm_unit']))
      df['unit_concept_id']=df['unit_concept_id'].astype('Int64') 
     #TODO donde esta unit_source_value?? 

      return df