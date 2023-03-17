import pandas as pd
from models.Base import Base
from models.Provider import Provider
from time import gmtime, strftime 
import config


class provider(Base):

    """ This class contains the logic of reading, extrating and transforming
     patient information to pass it to the OMOP CDM model """

   

  


    def run(self):
        """ Run the job"""
        print("launch %s"%self.source_module)
        print("Getting data from source")     
        print(strftime("%H:%M:%S", gmtime()))  
        
                
        origin_engine = self.getPgOriginConnection() 
        chunklist=pd.read_sql_query("select * from mfm.mf_provider", origin_engine, chunksize=config.chunksize)
        
        for df in chunklist:
            """ A mapping of all the necessary data is carried out in the table condition_occurrence
            This table contains records of activities or processes ordered by, or carried out by, 
            a healthcare provider on the patient with a diagnostic or therapeutic purpose
            For more information on records, visit https://ohdsi.github.io/CommonDataModel/cdm54.html#condition_OCCURRENCE  """

            # df=df[(df.fecalt.dt.year >= config.startyear) | (pd.isna(df.fecalt)) ]
            # if(len(df)==0):
            #     continue

            print("Mapping data")
            print(strftime("%H:%M:%S", gmtime())) 
            
            df['origin_source_data_provider']='bigandata'
            df['origin_source_name']='mfm.mf_provider'
            df['origin_source_value']=df['code']

            df['provider_name']=df['facility_st'] + " - " + df['speciality_st']
            df['provider_source_value']=df["code"] 
            df['care_site'] = df['location_code']
            df['specialty_concept_id']=0 
            df['specialty_source_value']=df["speciality_cd"]
          
            p=Provider(self.caches)
            p.process(df)





            
            
            

         