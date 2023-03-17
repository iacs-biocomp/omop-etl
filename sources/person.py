from copy import copy
import numpy
import pandas as pd
from sqlalchemy import false, null
from models.Base import Base
import os
import config
from utils.OmopDB import OmopDB
import utils.omop.omop as omop
from models.Person import Person
from models.Death import Death
from models.Observation import Observation
from google_translate_py import Translator
from tqdm import tqdm


class person(Base):

    """ This class contains the logic of reading, extrating and transforming
     patient information to pass it to the OMOP CDM model """
    
    source='mf_usuarios'
   
  

    def run(self):
            
            """ This function extracts information from the DataBase source to DB OMOP""" 
                        
            print("launch person_job")
            
                
            origin_engine = self.getPgOriginConnection() 
            chunklist=pd.read_sql("select u.*, mc.descrip as pais_naciemiento, mc2.descrip as pais_nacionalidad from mfm.mf_usuarios u left join mfm.mf_countries mc on mc.code=u.paisnac  left join mfm.mf_countries mc2 on mc2.code=u.nacionalidad  where u.active=true and mc.active =true and mc2.active = true",
                                        origin_engine,chunksize=config.chunksize )

            for df in chunklist:

                df['origin_source_data_provider']='bigandata'
                df['origin_source_name']='mfm.mf_usuarios'
                df['origin_source_value']=df['usuario_id']


                df['person']=df.usuario_id            
                df['type_concept_id']=32817  

                p=Person(self.caches)
                p.source_module=self.source_module
                df['care_site']=df['zbs']
                df['provider']=df['cias_cd']
                df['location']=df['zbs']

                df=p.process(df)

              

                ### Insertamos observacion "Country of birth"
                o=Observation(self.caches)
                o.source_module=self.source_module
            
                df.drop(labels='person', axis=1, inplace=True)
                df.provider_id=None
                df['concept']=df['pais_naciemiento'].str.lower()
                # Obtenemos países de omop.source_to_concept_map, y cruzamos por nombre del país
                cdf=self.getCountries()
                cdict=pd.Series(cdf['TARGET_CONCEPT_ID'].values,index=cdf['SOURCE_CODE'].astype(str).str.lower()).to_dict()


                df['country_id']=df['concept'].map(cdict.get).astype('Int64')
                df['nacimiento_dt']=pd.to_datetime(df.nacimiento_dt,errors='coerce', infer_datetime_format=True)
                df['start_timestamp']=df['nacimiento_dt']
                df['concept_id']=4197735 # Country of birth
                df['domain']='Observation'
                df['value_as_concept_id']=df['country_id']
                df['observation_source_concept_id']=0

                #### PRUEBAS ponemos como concept el codigo orignal para paises que tienen
                df['concept'] = df['paisnac']
                o.process(df)


                ### Insertamos observacion "Country of birth"
                ### Una copia para las nacionalidades. Pues tenemos que hacer otro registro
                copydf = df.copy()
                # En este caso, el concept sera el codigo orignal que tiene para la nacionalidad
                copydf['concept_id'] = 40769139
                copydf['observation_concept_id'] = 40769139
                copydf['value_as_concept_id']=copydf['pais_nacionalidad'].str.lower().map(cdict.get).astype('Int64')
                copydf['concept'] = copydf['nacionalidad']
                copydf['observation_source_value']=copydf['nacionalidad']
                copydf['observation_source_concept_id']=0
                o.process(copydf)


                ### Insertamos fallecimiento
                d=Death(self.caches)
                d.source_module=self.source_module
                df.dropna(subset = ['exitus_dt'], inplace=True)  

                df['death_datetime']=pd.to_datetime(df['exitus_dt'], format="%Y-%m-%d", errors='coerce')
                df['death_date'] = df['death_datetime'].dt.date
                df['death_type_concept_id']=32817  
                df=d.process(df)




   
    def limpiafecha(self, df):
        """ This method is removing the date of birth with errors """
       
        errors=df[df['birth'].isnull()]
        output_path=config.log_dir+"/person.birth.csv"
        errors.to_csv(output_path, mode='a', sep=';', index=False , header=not os.path.exists(output_path))
        df.dropna(subset = ['birth'], inplace=True) 
          

        return df


    def getCountries(self):
        """ This method has been used for mapping the countries used for nacionality and country of born"""

        if 'countries' in self.caches:
            return self.caches['countries']
        

        query="""select source_code, target_concept_id from omop.source_to_concept_map where  source_vocabulary_id ='Countries'; """
                        
        
        db=OmopDB()
        conn = db.getSession()
        df=pd.read_sql_query(query, conn )
            
            
        df.columns = [x.upper() for x in df.columns]
        
        self.caches['countries']=df
            
        return df
