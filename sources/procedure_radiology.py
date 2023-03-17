from models.Procedure import Procedure
import pandas as pd
from models.Base import Base
from time import gmtime, strftime 
import os
import config 
from utils.casandra import Cassandra

class procedure_radiology(Base):

    """ This class contains the logic of reading, extrating and transforming
     patient information to pass it to the OMOP CDM model """

  

  


    def run(self):
        """ This method runs the job """
        
        print("launch %s"%self.source_module)
        print("Getting data from source")     
        print(strftime("%H:%M:%S", gmtime()))  
      
        cassandra=Cassandra()

        df=pd.DataFrame()

        query = 'select distinct event_yr, event_mth from radiology '
        cdf=cassandra.cas2pandas(query)
        cdf=cdf[(cdf.event_yr >=config.startyear) & (cdf.event_yr <=config.endyear)]
        cdf=cdf.reset_index()
        
       

        for index, c in cdf.iterrows():
          query = "select * from radiology  where  event_yr=%s and event_mth=%s "%(c.event_yr, c.event_mth )
          procedure=cassandra.cas2pandas(query)
          df=pd.concat([df, procedure])

          if len(df)>=config.chunksize or index == cdf.index[-1]:

            print("Mapping data")
            print(strftime("%H:%M:%S", gmtime())) 
            
            df['origin_source_data_provider']='biganlake'
            df['origin_source_name']='radiology'
            df['origin_source_value']=df['rad_id']
            
      
            df.rename(columns={'person_id':'person'}, inplace=True)
            df.rename(columns={'prest_cd':'concept'}, inplace=True)
            df['type_concept_id']=32817

            #df.event_dt = df.event_dt.apply(lambda x:x.date())
            df['start_timestamp']=pd.to_datetime(df.event_dt.astype(str),errors='coerce', infer_datetime_format=True, utc=True).dt.tz_convert("Europe/Madrid")
            

            #clean errors
            errors=df[df['start_timestamp'].isnull()]
            if(len(errors)>0):
                        output_path=config.log_dir+"/radiology.event_dt.csv"
                        errors.to_csv(output_path, mode='a', sep=';', index=False , header=not os.path.exists(output_path))
                        df.dropna(subset = ['start_timestamp'], inplace=True) 

            df['end_timestamp']=df['start_timestamp']
            
            
           


            c=Procedure(self.caches)
            df=c.mapConcepts(df,vocabularios=['SERAM'] )
            
            df['domain']=df['domain'].fillna('Procedure')
            self.sendToDomains(df)

            

            df=pd.DataFrame()



            
            
            

         