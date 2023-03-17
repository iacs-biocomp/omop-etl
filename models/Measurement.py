from models.Base import Base
from models.models import Measurement as MEASUREMENT
from utils.OmopDB import OmopDB
import pandas as pd
import  config 

class Measurement(Base):
    """ This class transform and insert records of Measurements, i.e, structure values , laboratory tests, vital signs
    Also, this class map the local vocabularies to standard OMOP 
    """
    
    def __init__(self, cache):
        self.model=MEASUREMENT
        self.domain='Measurement'
        self.caches=cache


    def mapData(self, df):
        
        domain=self.domain.lower()
        print("\t Mapping %s"%domain)

        primary_key=None

        if hasattr(self.model, '__table__' ):
            primary_key = self.model.__table__.primary_key
    
        if primary_key is not None:
            lastPK = self.getLastPK()

            mask=df['domain']==self.domain
            df.loc[mask, 'id']=range(lastPK+1,lastPK+1+len(df[mask]))
            df['measurement_id']=df.id
            
        
        df=self.create_default_domain_columns(df, domain)

        if 'concept_id' in df.columns:
            mask= pd.isna(df['%s_concept_id'%domain])
            df.loc[mask,'%s_concept_id'%domain]=df['concept_id']
            df['%s_concept_id'%domain]=df['%s_concept_id'%domain].astype('Int64')

        if 'concept' in df.columns:
            mask= pd.isna(df['%s_source_value'%domain])
            df.loc[mask,'%s_source_value'%domain]=df['concept']

        if 'source_concept_id' in df.columns:
            mask= pd.isna(df['%s_source_concept_id'%domain])
            df.loc[mask,'%s_source_concept_id'%domain]=df['source_concept_id']
            df['%s_source_concept_id'%domain]=df['%s_source_concept_id'%domain].astype('Int64')

        if 'value_id' in df.columns and 'value_as_concept_id' not in df.columns:
            
            df['value_as_concept_id']=df['value_id'].astype('Int64')
        
        
        df['%s_type_concept_id'%domain]= df['type_concept_id']

      
        df['%s_datetime'%domain]=df['start_timestamp']
        df['%s_date'%domain]=df['%s_datetime'%domain].dt.date


        return super().mapData(df)



    def cleanData(self, df, module_name):

        df=self.reviewErrors(df, 'person_id', module_name, dropErrors=True)
        df=self.reviewErrors(df, 'measurement_start_date',module_name,  dropErrors=True)
        df=self.reviewErrors(df, 'measurement_concept_id',module_name,  dropErrors=False)
        
       
        return df

    def insertData(self, df):
        return super().insertData(df)


    def mapUnits(self, df, vocabulariosUnidades):
        """ This method is mapping the units to Standard codes """
        
        for i,vocab in enumerate(vocabulariosUnidades):
            if 'unit_concept_id' not in df.columns:
                df['unit_concept_id']=None
                df['unit_source_value']=None
                df['unit_source_concept_id']=None


            if len(df[pd.isna(df['unit_concept_id'])])==0:
                return df

            print("maping units in %s"%vocab)
            
            if vocab not in self.caches:
                unit_vocab='%s_unit'%vocab
                self.caches.set(unit_vocab,self.getUnitVocab(vocab))
                

            
            df['unit_source_value']=df['unit']

            
            
            target=pd.Series( self.caches[unit_vocab].CONCEPT_ID.values,index= self.caches[unit_vocab].CONCEPT_CODE.astype(str)).to_dict()
            target_name=pd.Series( self.caches[unit_vocab].CONCEPT_ID.values,index= self.caches[unit_vocab].CONCEPT_NAME.astype(str)).to_dict()

            df['unit_concept_id']=df['unit_concept_id'].fillna(df['unit'].astype(str).map(target.get)).astype('Int64')
            df['unit_source_concept_id']=df['unit_source_concept_id'].fillna(df['unit'].astype(str).map(target.get)).astype('Int64')


            
            df['unit_concept_id']=df['unit_concept_id'].fillna(df['unit'].astype(str).map(target_name.get)).astype('Int64')
            df['unit_source_concept_id']=df['unit_source_concept_id'].fillna(df['unit'].astype(str).map(target_name.get)).astype('Int64')
        
  
        return df

    def getUnitVocab(self, vocab):
        """ This method defines the query that insert the standard units codes into OMOP table """

        query="""SELECT *
                    FROM @vocabulary.CONCEPT C
					where C.invalid_reason IS NULL
                and C.DOMAIN_ID='Unit' and c.vocabulary_id= '%s';"""%vocab

        db=OmopDB()
        conn = db.getSession()
        df=pd.read_sql_query(query.replace('@cdm', config.omop_cdm_schema).replace('@vocabulary', config.omop_vocabulary_schema), conn )
            
            
        df.columns = [x.upper() for x in df.columns]
        df['CONCEPT_ID']=df['CONCEPT_ID'].astype(int)
            
        return df