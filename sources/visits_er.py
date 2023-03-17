from datetime import date
import numpy
from models.Condition import Condition
import pandas as pd
from models.Base import Base
from models.Visit import Visit
from time import gmtime, strftime 
import os
import config 
from utils.casandra import Cassandra


class visits_er(Base):

    """ This class contains the logic of reading, extrating and transforming
     patient information to pass it to the OMOP CDM model """

   

  


    def run(self):
        """ Run the job"""
        print("launch %s"%self.source_module)
        print("Getting data from source")     
        print(strftime("%H:%M:%S", gmtime()))  
        
                
          
        cassandra=Cassandra()
        df=pd.DataFrame()

        query = 'select distinct event_yr from visits_er '
        cdf=cassandra.cas2pandas(query)
        cdf=cdf[(cdf.event_yr >=config.startyear) & (cdf.event_yr <=config.endyear)]
        cdf=cdf.reset_index()
   
        for index, c in cdf.iterrows():
            
            query = 'select id, person_id, event_dt,eradmitorigin_cd, erdischargetype_cd, discharge_accept_dt,facility_cd,diag1_cie_cd, diag2_cie_cd,diag3_cie_cd from visits_er  where  event_yr=%s'%(c.event_yr)
            visit=cassandra.cas2pandas(query)
            df=pd.concat([df, visit])

            if len(df)>=config.chunksize or index == cdf.index[-1]:  

                print("Mapping data")
                print(strftime("%H:%M:%S", gmtime())) 
                
                df['origin_source_data_provider']='biganlake'
                df['origin_source_name']='visits_er'
                df['origin_source_value']=df['id']

                
                df['visit_source_value']='visits_er'
                df.rename(columns={'person_id':'person'}, inplace=True)
                df['type_concept_id']=32817

            
                df['care_site']=df['facility_cd']

                df['start_timestamp']=pd.to_datetime(df.event_dt.astype(str),errors='coerce', infer_datetime_format=True, utc=True).dt.tz_convert("Europe/Madrid")
                df=self.reviewErrors(df, 'start_timestamp',self.source_module, dropErrors=True)

                df['end_timestamp']=pd.to_datetime(df.discharge_accept_dt.astype(str),errors='coerce', infer_datetime_format=True, utc=True).dt.tz_convert("Europe/Madrid")
                df=self.reviewErrors(df, 'end_timestamp',self.source_module, dropErrors=True)    
                
                df['visit_concept_id']=9203 
                df['visit_source_concept_id']=0


                print("Procesing Visits")
                print(strftime("%H:%M:%S", gmtime())) 
                vs=Visit(self.caches)
                vs.source_module=self.source_module

                #TODI revisar por que no aparece discharge_to_source_value
                #TODO nos tienen que enviar mapeos de admited_concept_id
                visitConceptIDMap = vs.getSnomedVisitDomain()
                df['admitting_source_concept_id']=df['eradmitorigin_cd'].map({'P':3192048, 'H':3196063}.get)
                df['admitting_source_concept_id']=df['admitting_source_concept_id'].fillna(0)
                df['admitting_source_concept_id']=df['admitting_source_concept_id'].astype('Int64')
                df['admitting_source_value']=df['eradmitorigin_cd']  


                #TODO REVISAR ELLOS 306689006 Alta a domicio, segun la documentacion = 0
                # para el resto de codigos obtendremos los valores que indican en la documentacion(Dominio Visit + Standard) e intentaremos mapear
                #para la query buscamos un maps to cuyo target domain=visit
                visitConceptIDMap = vs.getSnomedVisitDomain()
                visitConceptIDMap['306689006']=0
                df['discharge_to_concept_id']=df['erdischargetype_cd'].astype(str).map(visitConceptIDMap.get)
                df['discharge_to_concept_id']=df['discharge_to_concept_id'].astype('Int64')
                df['discharge_to_source_value']=df['erdischargetype_cd']
                
                
                df=vs.process(df)



                c=Condition(self.caches)
                c.source_module=self.source_module
                df=df[['person', 'start_timestamp', 'end_timestamp', 'type_concept_id','visit_occurrence_id' ,'diag1_cie_cd', 'diag2_cie_cd','diag3_cie_cd','origin_source_data_provider', 'origin_source_name','origin_source_value']]
                
                df=df.melt(id_vars=['person', 'start_timestamp', 'end_timestamp','type_concept_id', 'visit_occurrence_id','origin_source_data_provider', 'origin_source_name','origin_source_value'], value_name='concept')
                df=df.dropna()
                df.concept=df.concept.astype(str)

                # Mapeamos primero a ICD9CM
                df = c.mapConcepts(df, vocabularios=["ICD9CM"])
                # df.start_timestamp > 2016-01-01 00:00:00
                # poner a null las columnas de concept_id 
                fecha = date(2016,1,1)
                df.loc[(df['start_timestamp'].dt.date > fecha), 'concept_id'] = None
                df.loc[(df['start_timestamp'].dt.date > fecha), 'domain'] = None
                
                df=c.mapConcepts(df,vocabularios=['ICD10CM', 'ICD10', 'ICD9CM', 'Condition'] )

                diag_map={'diag1_cie_cd':32902}
                df['condition_status_concept_id']=df['variable'].map(diag_map.get)
                df['condition_status_concept_id']=df['condition_status_concept_id'].fillna(32908)
                df['condition_status_concept_id']=df['condition_status_concept_id'].astype('Int64')
                df['condition_status_source_value']=df['variable']



                df['domain']=df['domain'].fillna('Condition')
                
                self.sendToDomains(df)

                

                df=pd.DataFrame()



            
            
            

         