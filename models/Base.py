import pandas as pd
from pandas import DataFrame
from pandas import Series
import importlib
import config
from timeit import default_timer as timer
from datetime import timedelta
from sqlalchemy import create_engine
import numpy as np
from utils.Database import Database
from utils.OmopDB import OmopDB
#import utils.omop.utils as utils
from time import gmtime, strftime 
import utils.omop.omop as omop
import os
import diskcache as dc
import utils.cache.cache as cache

class Base():
    """ 
    This class defines all the main logic required for the ETL. All classes containing the specific tranformations required
    for each OMOP CDM table must inherit this class. 


    """
    jobs = []
    caches={}
    totalTimes={}
    model_name=None
    domain=None
    source_module=None
   

    def __init__(self, caches=None):
        self.source_module=self.__class__.__module__
        self.caches=caches
        

    def run(self):
        """ This method runs the ETL """

        for job in self.jobs:
            job.caches=self.caches
            
            startTime=timer()
            job.run()
            self.totalTimes[type(job).__name__]=timer() -startTime
            print(type(job).__name__+' Total Time:'+ str(timedelta(seconds=self.totalTimes[type(job).__name__])))
        
        print("---------------  TOTAL TIMES  -----------------------------")
        for job in self.totalTimes:
            print(job+' Total Time: '+ str(timedelta(seconds=self.totalTimes[job])))  
    
    def insert(self, df: DataFrame) -> DataFrame: 
        """ 
        This method deletes all the columns in the dataframe that are not needed and inserts
        the dataframe in an OMOP CDM table
        """
        
        modelColumns=None
        if hasattr(self.model, '__table__'):
            print("Inserting in %s"%self.model.__table__.name )  
            modelColumns= self.model.__table__.columns
            modelColumnsNames=[c.name for c in self.model.__table__.columns]
        else:
            modelColumns= [c for c in self.model.columns]
            modelColumnsNames=[c.name for c in self.model.columns]
        for c in df.columns: 
            if c not in modelColumnsNames:
                df.drop(labels=c, axis=1, inplace=True)

        
        for c in modelColumns: 
            if c.name not in df.columns and c.server_default is None :
                df[c.name]=None
            
        ordered_colums=[c.name for c in modelColumns if c.server_default is None]
        
        df=df[ordered_colums]

        

        omopdb=OmopDB()
        omopdb.insert_with_progress(df, self.model, columns=ordered_colums)
        omopdb.close()
    
        return df
    
    def load_caches(self, caches=[]):
        """ This method loads the caches used for opening the conexion with the database """

        for c in caches:
            self.loadCache(c, self.getOmopConnection, config.omop_db_schema)
           
    def load_cache(self, cacheName, dbConnection, schema=None) -> Series:
        """ This method loads the caches used for the vocabularies """

        cache_id='%s_id'%cacheName
        cache_source_value='%s_source_value'%cacheName

        cdf=pd.read_sql_table(cacheName, dbConnection, schema=schema,columns=[cache_id, cache_source_value])
        return pd.Series(cdf[cache_id],index=cdf[cache_source_value].astype(str)).to_dict()
       
    def map_column(self, column, df):
        """ This method loads the cache for the structure of the columns"""

        if column not in self.caches:
            self.loadCache(column)

        column_id='%s_id'%column

        df[column_id]=df[column].map(self.caches[column].get)
        df[column_id]=df[column_id].astype('Int64')   
        
        return df
    
    def getVocab(self, vocab):
        """ This method gets the indicated vocabulary """

        if vocab not in self.caches:
                self.caches.set(vocab,omop.sourceToStandard([vocab]))
        return self.caches[vocab]

    def mapConcepts(self, df, vocabularios=[]):
        """ This method is mapping the dataframe with all the vocabularies specified in the vocabulary  """  

        # vocab=vocabularios.pop()
        # self.mapConceptID(df, vocab)

        for vocab in vocabularios:
            
            df=self.mapCp(df,vocab)
        return df

    def mapConceptID(self, df, vocab):
        """  This method is mapping if the target_domain_id has the same domain as the table imported """  
        
        domain=self.domain.lower()
    
        if vocab is not None:
            domainVocab=self.getVocab(vocab)    

            if 'SOURCE_CODE' in df:
                """ Mapping source_code from any table by domain""" 
                df.drop(labels=domainVocab.keys(), axis=1, inplace=True)
            df=df.merge(domainVocab, left_on=domain,  right_on='SOURCE_CODE', how= 'left') 

        
        df['%s_concept_id'%domain]=df['%s_concept_id'%domain].fillna(df['TARGET_CONCEPT_ID'])
       
        df['%s_source_value'%domain]=df[domain]
        df['%s_source_concept_id'%domain]=df['%s_source_concept_id'%domain].fillna(df['SOURCE_CONCEPT_ID'])

        df['%s_concept_id'%domain]=df['%s_concept_id'%domain].fillna(df['TARGET_DOMAIN_ID'])
        df['domain']=df['domain'].fillna(df['TARGET_DOMAIN_ID'])

        return df

    def mapCp(self, df, vocab):
        """ This method is mapping if the target_domain_id has the same domain as the table imported """ 

        df=self.create_default_columns(df)
        #mapeamos solo los que falten
        mask=pd.isna(df['concept_id'])

        cdf=df[mask]        
        if vocab is not None and len(cdf)>0:
            print("Mapping %s"%vocab )
            domainVocab=self.getVocab(vocab)    

                
            cdf=cdf.merge(domainVocab, left_on='concept',  right_on='SOURCE_CODE', how= 'left') 
            # domainVocab introduce el TARGET_CONCEPT_ID
            
            # Relleno de los nulos con la columan TARGET_CONCEPT_ID del df 
            cdf['concept_id']=cdf['concept_id'].fillna(cdf['TARGET_CONCEPT_ID'])
        
            cdf['source_value']=cdf['concept']
            cdf['source_concept_id']=cdf['source_concept_id'].fillna(cdf['SOURCE_CONCEPT_ID'])
                
            cdf['domain']=cdf['domain'].fillna(cdf['TARGET_DOMAIN_ID'])
            cdf['value_id']=cdf['value_id'].fillna(cdf['TARGET_VALUE'])
            cdf.drop(labels=domainVocab.keys(), axis=1, inplace=True)

            df=pd.concat([df[~mask], cdf])

        return df
    
    def getOriginConnection( self):
        """ This method is opening the connection with the origin_source SQL database """

        origin_engine = create_engine(config.origin_connection_string)
        origin_engine=origin_engine.connect().execution_options(stream_results=True)  
        return origin_engine
    
    def getPgOriginConnection( self):
        """ This method is opening the connection with the origin_source Postgress database """

        origin_engine = create_engine(config.pg_origin_connection_string)
        origin_engine=origin_engine.connect().execution_options(stream_results=True)  
        return origin_engine

    def getOmopConnection(self):
        """ This method is opening the connection with the OMOP database """

        return self.getOmopDb().getSession()

    def getOmopDb(self):
        """ This method is opening the connection with the the OMOP database  """

        db = OmopDB()
        return db
    
    def sendToDomains(self, df):
        """ This method is checking the domains and asigning it in the right table  """

        print("Sending to domains")
        print(strftime("%H:%M:%S", gmtime())) 
        domains=df.domain.unique()
        for domain in domains:
            if domain not in [np.nan, None]:
                import importlib
                module = importlib.import_module('models.'+domain)
                class_ = getattr(module, domain)
                instance = class_(self.caches)
                instance.source_module=self.source_module
                cdf=df[df.domain==domain].copy()
                cdf=instance.mapData(cdf) 
                cdf=instance.cleanData(cdf, self.__class__.__module__)
                domain_columns=instance.insertData(cdf).columns

    def sendToCorrectDomain(self, df):
        """ This method is checking the right domains and sending the concept_id to the right table """

        dfWrongDomain=df.loc[df['TARGET_DOMAIN_ID'] != self.domain].copy()
        domains=dfWrongDomain['TARGET_DOMAIN_ID'].unique()
      
        for domain in domains:
            if domain not in [np.nan,  None]: 
                module = importlib.import_module('jobs.'+domain)
                class_ = getattr(module, domain)
                instance = class_()
                ndf=instance.map(dfWrongDomain[dfWrongDomain['TARGET_DOMAIN_ID']==domain].copy(), source=self.domain.lower())
                ndf=instance.limpiaDatos(ndf) 
                instance.insert(ndf)

                df.drop(df[df['TARGET_DOMAIN_ID']== domain].index, inplace=True)

    def process(self, df, module_name=''):
        """ This method runs the subjobs """

        df=self.mapData(df)
        df=self.cleanData(df, module_name)
        self.insertData(df.copy())
        return df

    def reviewErrors(self, df, column, module_name, dropErrors=False):
        """ This method checks if exists null in the columns and save it in a csv """

        sourceColumn=None
        cls=column.split('_')
        if cls[0] == 'concept':
            sourceColumn='%s_source_value'%cls[0]

        if column in df.columns:
            
            df[column]=df[column].replace(r'', ' ')
            errors=df[(df[column].isnull())]
            if sourceColumn is not None:
                errors=errors[~errors[sourceColumn].isnull()]

            if len(errors) > 0:
                output_path=config.log_dir+"/"+self.source_module+'_'+column+"_errors.csv"
                errors.to_csv(output_path, mode='a', sep=';', index=False , header=not os.path.exists(output_path))

                if(dropErrors):
                    df.dropna(subset = [column], inplace=True)
                else:
                    df[column]=df[column].fillna(0)

        return df

    def create_default_columns(self,df):
        """ This method creates the columns standard for the vocabularies """

        if 'id' not in df.columns:
            df['id']=None
        
        if 'concept_id' not in df.columns:
            df['concept_id']=None
       
        if 'source_value'not in df.columns:
            df['source_value']=None
            
        if 'source_concept_id' not in df.columns:
            df['source_concept_id']=None

        if 'domain' not in df.columns:
            df['domain']=None

        if 'value_id' not in df.columns:
            df['value_id']=None

        return df

    def create_default_domain_columns(self,df, domain):
        """ This method checks if the domains asigned for the values of the columns are right """

        if 'id' not in df.columns:
            df['id']=None
        
        if '%s_concept_id'%domain not in df.columns:
            df['%s_concept_id'%domain]=None
       
        if '%s_source_value'%domain not in df.columns:
            df['%s_source_value'%domain]=None
            
        if '%s_source_concept_id'%domain not in df.columns:
            df['%s_source_concept_id'%domain]=None

        if 'domain' not in df.columns:
            df['domain']=None
        
        if 'value_id' not in df.columns:
            df['value_id']=None

        return df

    def mapData(self, df):
        """ This method defines the mappig for the values of the tables that proceed from Person, Provider and Care_site """

        df=self.mapPerson(df)
        df=self.mapProvider(df)
        df=self.mapCare_Site(df)
        #df=self.mapVisit(df)

        return df

    def cleanData(self, df, module_name=''):
        """ This method deletes the data are not able to be inserted into the tables """

        return df
    

    def insertData(self, df):
        """ This method inserts the dato into OMOP CDM DB """

        self.insert(df)
        return df
        
    def getLastPK(self):
        """ This method checks for the PK in the OMOP table """
        omopDb= OmopDB()
        lastPK=omopDb.getLastPK(self.model)
        omopDb.close()
        if lastPK==None:
            lastPK=1

        return lastPK

    def read_table(self, table, dbConnection, schema='omop'):
        """ This method reads the data for source_value that has been saved in the cache """

        cache_id='%s_id'%table
        cache_source_value='%s_source_value'%table

        cdf=pd.read_sql_table(table, dbConnection, schema=schema,columns=[cache_id, cache_source_value])
        return pd.Series(cdf[cache_id].values,index=cdf[cache_source_value].astype(str)).to_dict()

    def mapPerson(self, df):
        """ This method checks for the person_id """

        if 'person' not in df.columns:
            return(df)
        
        if 'person_id' not in df.columns:
            df['person_id']=None

        if 'person' not in self.caches:
            self.caches.set('person',cache.load_cache('person',self.getOmopConnection()))
            


        df['person_id']=df['person_id'].fillna(df.person.map(self.caches['person']))
        df['person_id']=df['person_id'].astype('Int64')   
        return df

    def mapVisit(self, df, source):
        """ This method returns the visit_occurence_id data saved in cache   """
        if 'visit' not in df.columns or 'visit_occurrence_id' in df.columns:
            return(df)
        
        if 'visit_occurrence' not in self.caches:
            self.caches.set('visit_occurrence', dc.Cache())

        sources=[source]
        if source=='all':
            sources=[]
            odf=pd.read_sql_query("select distinct origin_source_name from omop.visit_occurrence where origin_source_value !='No pk' ",
                                 self.getOmopConnection())  

            sources=odf.origin_source_name.unique()
        
        
        
        if 'visit_occurrence_id' not in df.columns:
            df['visit_occurrence_id']=None      
        
        
        for s in sources:
            if s not in self.caches['visit_occurrence']:
                cache.load_cache('visit_occurrence',self.getOmopConnection(), cache=self.caches, source=s)
    
            df['visit_occurrence_id']=df['visit_occurrence_id'].fillna(df.visit.map(self.caches['visit_occurrence'][s].get))


        df['visit_occurrence_id']=df['visit_occurrence_id'].astype('Int64')   
        return df
   
    def mapCare_Site(self, df):
        """ This method returns the provider_id data saved in the table care_site_id   """

        if 'care_site' not in df.columns:
            if 'provider_id' in df.columns:
                self.caches.set('provider_idTocare_site_id',cache.load_cache('provider_idTocare_site_id',self.getOmopConnection()))
                df['care_site_id']=df.provider_id.astype(str).map(self.caches['provider_idTocare_site_id'].get)
                df['care_site_id']=df['care_site_id'].astype('Int64')   

            return(df)
        
        if 'care_site_id' in df.columns:
            return df

        if 'care_site' not in self.caches:            
            self.caches.set('care_site',cache.load_cache('care_site',self.getOmopConnection()))
            


        df['care_site_id']=df.care_site.map(self.caches['care_site'].get)
        df['care_site_id']=df['care_site_id'].astype('Int64')   
        return df
    
    def mapProvider(self, df):
        """ This method returns the provider_id data saved in cache   """   

        if 'provider_id' in df.columns or  'provider' not in df.columns:
            return df

               
        if 'provider' not in self.caches:
            
            self.caches.set('provider',cache.load_cache('provider',self.getOmopConnection()))
            


        df['provider_id']=df.provider.map(self.caches['provider'].get)
        df['provider_id']=df['provider_id'].astype('Int64')   
        return df