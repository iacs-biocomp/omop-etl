import pandas as pd
from models.Base import Base
from models.Location import Location
from models.Care_Site import Care_Site
from time import gmtime, strftime 
import os
import config 


class location(Base):

    """ This class contains the logic of reading, extrating and transforming
     patient information to pass it to the OMOP CDM model """

   

  


    def run(self):
        """ Run the job"""
        print("launch %s"%self.source_module)
        print("Getting data from source")     
        print(strftime("%H:%M:%S", gmtime()))  
        
                
        origin_engine = self.getPgOriginConnection()
        chunklist=pd.read_sql_query("select * from mfm.mf_location", origin_engine, chunksize=config.chunksize)
        
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
            df['origin_source_name']='mfm.mf_location'
            df['origin_source_value']=df['codigo']

            
            df['location_source_value']=df["codigo"]
          
            l=Location(self.caches)
            l.source_module=self.source_module
            l.process(df)


            mapaTipo={'AE':38004515, 'AP':38004247}
        

            df['care_site_name']=df["nombre"]
            df['place_of_service_concept_id']=df["tipo_actividad"].map(mapaTipo.get)
            df['location']=df["codigo"]
            df['place_of_service_source_value']=df["tipo_actividad"]
          
            c=Care_Site(self.caches)
            c.source_module=self.source_module
            c.process(df)

            df=pd.DataFrame()



            
            
            

         