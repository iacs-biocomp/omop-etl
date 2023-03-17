import pandas as pd
from models.Base import Base
from models.Visit import Visit
from time import gmtime, strftime 
import os
import config 
from utils.casandra import Cassandra
import numpy as np
import glob
from utils.OmopDB import OmopDB

class mstest(Base):

    """ This class contains the logic of reading, extrating and transforming
     patient information to pass it to the OMOP CDM model """

   

  


    def run(self):
        """ Run the job"""
  

        print("launch %s"%self.source_module)
        print("Getting data from source")     
        print(strftime("%H:%M:%S", gmtime()))  
        
        cassandra=Cassandra()

        df=pd.DataFrame()

        query = "select * from visits_sc  where  event_yr=2019 and event_mth=201906 "
        visit=cassandra.cas2pandas(query)   
        visit=visit[visit['visit_id'].astype(str)=='19631286']

        for row in visit.iterrows():
            print(row)


        x=0

        # query = 'select distinct event_yr, event_mth from visits_pc '
        # cdf=cassandra.cas2pandas(query)
        # cdf=cdf[(cdf.event_yr >=config.startyear) & (cdf.event_yr <=config.endyear)]
        # cdf=cdf.reset_index()
        
       

        # for index, c in cdf.iterrows():
        #   query = "select * from visits_pc  where  event_yr=%s and event_mth=%s "%(c.event_yr, c.event_mth )
        #   visit=cassandra.cas2pandas(query)
        #   visit=visit[visit['person_id']=='2fjcd59wKzXZsTMDNIfkTg==']
        #   df=pd.concat([df, visit])

        #   if len(df)>=config.chunksize or index == cdf.index[-1]:  
        
        #     print("Mapping data")
        #     print(df)
        #     print(strftime("%H:%M:%S", gmtime())) 
            
            # df['origin_source_data_provider']='biganlake'
            # df['origin_source_name']='visits_pc'
            # df['origin_source_value']='No pk'

            
            # df.rename(columns={'person_id':'person'}, inplace=True)
            # df['type_concept_id']=32817

            # df['attending_cias_cd']=df['attending_cias_cd'].replace(r'^\s*$', np.nan, regex=True)
            # df['provider']=df['attending_cias_cd']

            # #TODO si no exite atending_cias, añadimos como provider el del paciente
            # #person=cache.

            # #TODO añadiremos el caresite del provider

            # df['start_timestamp']=pd.to_datetime(df.event_dt.astype(str),errors='coerce', infer_datetime_format=True)
           
            # #clean errors
            # df=self.reviewErrors(df, 'start_timestamp',self.source_module, dropErrors=True)

            # df['end_timestamp']=df['start_timestamp']
            
            
            # df['visit_concept_id']=df['visittype_cd'].str.strip().map({'004':581476, '005':581476, '007':581476, }.get)
            # df['visit_concept_id']=df['visit_concept_id'].fillna(9202)
            # df['visit_concept_id']=df['visit_concept_id'].astype('Int64')
            # df['visit_source_concept_id']=0
            # df['visit_source_value']=df['visittype_cd']
            # #df.loc[df['visit_concept_id']==9202, 'visit_source_value']='visits_pc'



            # print("Procesing Visits")
            # print(strftime("%H:%M:%S", gmtime())) 
            
            # vs=Visit(self.caches)
            # vs.source_module=self.source_module
          

            # df=pd.DataFrame()



            
            
            

         