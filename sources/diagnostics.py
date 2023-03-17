import pandas as pd
from models.Base import Base
from time import gmtime, strftime 
import os
import config
from utils.OmopDB import OmopDB
from utils.casandra import Cassandra
import utils.omop.omop as omop

class diagnostics(Base):

      """ This class contains the logic of reading, extrating and transforming
      patient information to pass it to the OMOP CDM model. 
      A mapping of all the necessary data is carried out in the table condition_occurrence
      This table contains records of activities or processes ordered by, or carried out by, 
      a healthcare provider on the patient with a diagnostic or therapeutic purpose
      For more information on records, visit https://ohdsi.github.io/CommonDataModel/cdm54.html#condition_OCCURRENCE  """

  
      def run(self):
        """ This method runs the jobs """

        print("launch %s"%self.__class__.__module__)
        print("Getting data from source")     
        print(strftime("%H:%M:%S", gmtime()))  
        
        cassandra=Cassandra()
        df=pd.DataFrame()

        query = 'select distinct event_yr from diagnostic '
        cdf=cassandra.cas2pandas(query)
        cdf=cdf.reset_index()
       

        for index, c in cdf.iterrows():
          query = "select * from diagnostic  where  event_yr=%s"%(c.event_yr)
          condition=cassandra.cas2pandas(query)
          df=pd.concat([condition, df])


          df.discharge_dt= df.discharge_dt.apply(lambda x:x.date() if not pd.isna(x) else x)
          df['discharge_dt']=pd.to_datetime(df.discharge_dt,errors='coerce', infer_datetime_format=True)
          
          df=df[(df.discharge_dt.dt.year >= config.startyear) | (pd.isna(df.discharge_dt)) ]
          if(len(df)==0):
                continue

          if len(df)>=config.chunksize or index == cdf.index[-1]:  
        
                print("Mapping data")
                print(strftime("%H:%M:%S", gmtime()))  
               
                df['origin_source_data_provider']='biganlake'
                df['origin_source_name']='diagnostic'
                df['origin_source_value']='No pk'
            
                              
                df.rename(columns={'person_id':'person'}, inplace=True)
                df.rename(columns={'diag_cd':'concept'}, inplace=True) 

                df['person_id']=None
                
                
                #df.diag_dt= df.diag_dt.apply(lambda x:x.date())
                df['start_timestamp']=pd.to_datetime(df.diag_dt.astype(str),errors='coerce', format='%Y-%m-%d')
                #df.discharge_dt= df.discharge_dt.apply(lambda x:x.date() if x is not None else x)
                df['end_timestamp']=pd.to_datetime(df.discharge_dt.astype(str),errors='coerce', format='%Y-%m-%d')
                #df['end_timestamp']=pd.to_datetime(df.discharge_dt,errors='coerce', infer_datetime_format=True)
                
                df['type_concept_id']=32817  #EHR


                df=self.create_default_columns(df)


                df=self.mapCp(df,'CIAP')

                #set default domain for unmapped concepts
                df.domain=df.domain.fillna('Condition')

                #known harcoded values
                df['condition_status_concept_id']=32902 # Primary diagnosis


                print("Sending to domains")
                print(strftime("%H:%M:%S", gmtime())) 
                domains=df.domain.unique()
                for domain in domains:
                        import importlib
                        module = importlib.import_module('models.'+domain)
                        class_ = getattr(module, domain)
                        instance = class_(self.caches)
                        instance.source_module=self.source_module
                        df=instance.mapData(df) #no utilizamos procces, ya que nos interesa mantener todos los codigos pero insertar cada uno en su dominio
                        df=instance.cleanData(df, self.__class__.__module__)
                        domain_columns=instance.insertData(df[df.domain==domain].copy()).columns
                        domain_columns=[column for column in domain_columns if column in df.columns and column not in ['person_id','origin_source_data_provider', 'origin_source_name','origin_source_value']]
                        df.drop(labels=domain_columns, axis=1, inplace=True)

            
                df.dropna(subset = ['id'], inplace=True)


                ##mapeos propios de Notes
                df.rename(columns={'diag_st':'note_text'}, inplace=True)  

                df['note_class_concept_id']=3031733
                df['encoding_concept_id']=32678
                df['language_concept_id']=4182511
                df['note_source_value']=0

                from  models.Note import Notes
                n = Notes(self.caches)
                n.source_module=self.source_module
                df=n.mapData(df)
                df =n.cleanData(df, self.__class__.__module__)
                n.insertData(df.copy())


                print("Processing Fact_Relationship")
                print(strftime("%H:%M:%S", gmtime())) 
                ##mapeos de fact_relationship
                df= df[['id', 'note_id','domain', 'note_class_concept_id', 'origin_source_data_provider', 'origin_source_name','origin_source_value']]
                domains= omop.getfullVocab('Domain')
                domain_id=pd.Series(domains['CONCEPT_ID'].values,index=domains['CONCEPT_NAME'].astype(str)).to_dict()
                df['domain_concept_id_1']=df['domain'].map(domain_id.get)
                df['fact_id_1']=df['id']
                df['domain_concept_id_2']='Note'
                df['domain_concept_id_2']=df['domain_concept_id_2'].map(domain_id.get)
                df['fact_id_2']=df['note_id']
                df['relationship_concept_id']=1176046

                from models.FactRelationship import Fact_Relationship

               
                f=Fact_Relationship(self.caches)
                f.source_module=self.source_module
                f.insertData(df)

                df=pd.DataFrame()

        
