from models.Measurement import Measurement
import pandas as pd
from pandas import DataFrame
from models.Base import Base
from time import gmtime, strftime 
from re import compile as re_compile
import os
import config 
from utils.casandra import Cassandra


class measurement_gpd(Base):

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
        
        query = 'select distinct event_yr, event_mth, gpd_cd from gpd '
        cdf=cassandra.cas2pandas(query)
        cdf=cdf[(cdf.event_yr >=config.startyear) & (cdf.event_yr <=config.endyear)]
        cdf=cdf.reset_index(drop=True)
        cdf=cdf.sort_values(by=['gpd_cd'])

        origin_engine = self.getPgOriginConnection() 
        dgps=pd.read_sql_query("select * from  mfm.mf_dgps_chronic",origin_engine  )
        
        dgpmask = cdf.gpd_cd.isin(dgps.dgp_id)
        cdf=cdf[dgpmask]
       
        for index, c in cdf.iterrows():
            query = "select * from gpd  where  event_yr=%s and event_mth=%s and gpd_cd='%s' "%(c.event_yr, c.event_mth,c.gpd_cd )
            measurement=cassandra.cas2pandas(query)
            df=pd.concat([df,measurement])
            
            
            if len(df)>=config.chunksize or index == cdf.index[-1]:

                print("Mapping data")
                print(strftime("%H:%M:%S", gmtime())) 
                
                df['origin_source_data_provider']='biganlake'
                df['origin_source_name']='gpd'
                df['origin_source_value']=df['episode_id']
                
                df.rename(columns={'person_id':'person'}, inplace=True)
                
                

                df['concept']=df.gpd_cd
                df['type_concept_id']=32817

                df['start_timestamp']=pd.to_datetime(df.event_dt.astype(str),errors='coerce', infer_datetime_format=True, utc=True).dt.tz_convert("Europe/Madrid")
                #clean errors
                df=self.reviewErrors(df, 'start_timestamp',self.model_name, dropErrors=True)

                df['end_timestamp']=df['start_timestamp']
                
                df['value_as_string']=df.gpd_st
                df['value_as_number']=df.gpd_st
                df['value_as_concept_id']=None

                    
                stringMask=df.gpd_st.apply(self.isNumber)
                df.loc[stringMask, 'value_as_string']=None
                df.loc[~stringMask, 'value_as_number']=None
                
                
                
                df['unit']=None

                
                #TODO añadir unit_source_value=gpd_cd donde unit_concept_id != None
                df=self._unitsFromGPD(df)   
                df['unit_soruce_value']=df['gpd_cd']
                df.loc[pd.isna(df.unit_concept_id), 'unit_source_value']=None



                df=self.mapConcepts(df,vocabularios=['gpd'] ) # Me va a duplicar las filas ... 
                df = self.__filtrar_valores_invalidos(df)
                df['source_concept_id']=df['source_concept_id'].fillna(0)
               





                c=Measurement(self.caches)
                #df=c.mapUnits(df, vocabulariosUnidades=['UCUM','SNOMED', 'LOINC', 'Nebraska Lexicon', 'unidades', 'UNIDADES_DGP'])
                
                df['domain']=df['domain'].fillna('Measurement')
                self.sendToDomains(df)


                

                df=pd.DataFrame()



            
    def isNumber(self, s):
       
        if pd.notna(s) and self.numberRegex.match(s) is None:
            return s.isdigit()
        return True

    def _unitsFromGPD(self, df: DataFrame) -> DataFrame:
        units=self._getUnitsFromGPD()
        df['unit_concept_id']=df.concept.map(units.get)
        df['unit_concept_id']=df['unit_concept_id'].astype('Int64')
        return df

    def _getUnitsFromGPD(self):
        if 'UNIDADES_DGP' not in self.caches:
            cdf=pd.read_sql_query("select source_code, target_concept_id from omop.source_to_concept_map stcm where source_vocabulary_id='UNIDADES_DGP'", self.getOmopConnection())
            self.caches.set('UNIDADES_DGP',pd.Series(cdf['target_concept_id'].values,index=cdf['source_code'].astype(str)).to_dict())
        
        return self.caches['UNIDADES_DGP']
        


    def __filtrar_valores_invalidos(self, df: DataFrame) -> DataFrame:

        files = "reglas_gpd.csv"
        reglas=pd.read_csv(files, delimiter=',')
    
        gpds=df['gpd_cd'].unique()
        reglas=reglas[reglas['texto'].isin(gpds)]

        for i,regla in reglas.iterrows():
            mask = (df['gpd_cd'] == regla['texto']) & (df['gpd_st'] == regla['valor']) & (df['concept_id'] != regla['codigo'])
            df.loc[mask, 'gpd_cd']=None
            concept_mapper={regla['valor']:regla['codigo']}
            df['value_as_concept_id']=df['value_as_concept_id'].fillna(df['gpd_st'].map(concept_mapper.get).astype('Int64'))
             

        # Codigos duplicados 
        # AA --> 1 (S)
        # AA --> 2 (N)
        # si gpd_cd == 'AA' && gpd_st = 'S' && concept_id == 2  --> None 
        # si gpd_cd == 'AA' && gp_st = 'N' && concept_id == 1 --> None  
    

        # Drop nulls
        df.dropna(subset = ['gpd_cd'], inplace=True)
        df['concept_id'] = df['concept_id'].astype('Int64')
        return df 