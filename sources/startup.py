from sqlalchemy.sql import text as sa_text
from sqlalchemy import MetaData
import os
import glob
from config import log_dir
from config import omop_db_schema
from models.Base import Base
from utils.OmopDB import OmopDB
from models.models import t_source_to_concept_map as Source_To_Concept_Map
from datetime import date
import pandas as pd
import config
 

class startup(Base):
    """ This is the class for truncate the tables OMOP"""
   
        
    def run(self):
        self.delete_logs()
        self.drop_tables()
        self.create_tables()
        self.loadVocabs()
        self.load_stcm()


    def delete_logs(self):
        print("Cleaning log files")
        
        files = glob.glob(log_dir+'/*.csv')
        for f in files:
            os.remove(f)

    def drop_tables(self):
        print("Cleaing Database")

        db=OmopDB()
        con = db.getConnection()
        trans = con.begin()

        file = open('./sql/drop_all_tables.sql')

        """
        lines = file.readlines()
        for line in lines:
            sql_query = line
            if sql_query != '\n' and sql_query[0:1] != '--':
                print(sql_query)
                con.execute(sa_text(sql_query.replace('@cdmDatabaseSchema', omop_db_schema)))
        """

        


        sql_query = file.read()
        
        con.execute(sa_text(sql_query.replace('@cdmDatabaseSchema', omop_db_schema)))
        
        file.close()
        trans.commit()
        con.close()


    def create_tables(self):
        db=OmopDB()
        con = db.getConnection()
        trans = con.begin()

        file = open('./sql/OMOPCDM_postgresql_5.3_ddl.sql')
        
        sql_query = file.read()
        
        con.execute(sa_text(sql_query.replace('@cdmDatabaseSchema', omop_db_schema)))
        
        file.close()
        trans.commit()
        con.close()
    

    def loadVocabs(self):
        
        o=OmopDB()
        db = o._getBulkEngine()
        
        print("inserting DOMAIN")
        db.bulk_insert_csv('/home/bahia/CDMV5VOCAB/DOMAIN.csv', 'DOMAIN', sep='\t')
        print("inserting CONCEPT") 
        db.bulk_insert_csv('/home/bahia/CDMV5VOCAB/CONCEPT.csv', 'CONCEPT', sep='\t')
        print("inserting VOCABULARY")
        db.bulk_insert_csv('/home/bahia/CDMV5VOCAB/VOCABULARY.csv', 'VOCABULARY', sep='\t')
        print("inserting CONCEPT_CLASS")
        db.bulk_insert_csv('/home/bahia/CDMV5VOCAB/CONCEPT_CLASS.csv', 'CONCEPT_CLASS', sep='\t')
        print("inserting RELATIONSHIP")
        db.bulk_insert_csv('/home/bahia/CDMV5VOCAB/RELATIONSHIP.csv', 'RELATIONSHIP', sep='\t')
        print("inserting DRUG_STRENGTH")
        db.bulk_insert_csv('/home/bahia/CDMV5VOCAB/DRUG_STRENGTH.csv', 'DRUG_STRENGTH', sep='\t')
        print("inserting CONCEPT_SYNONYM")
        db.bulk_insert_csv('/home/bahia/CDMV5VOCAB/CONCEPT_SYNONYM.csv', 'CONCEPT_SYNONYM', sep='\t')
        print("inserting CONCEPT_ANCESTOR")
        db.bulk_insert_csv('/home/bahia/CDMV5VOCAB/CONCEPT_ANCESTOR.csv', 'CONCEPT_ANCESTOR', sep='\t')
        print("inserting CONCEPT_RELATIONSHIP")
        db.bulk_insert_csv('/home/bahia/CDMV5VOCAB/CONCEPT_RELATIONSHIP.csv', 'CONCEPT_RELATIONSHIP', sep='\t')

       

    def load_stcm(self):
        print("Inserting in stcm from CSV")
        self.load_stcm_csv()
        self.load_stcm_CIAP() 
        self.load_stcm_ROUTES()

    def load_stcm_csv(self):
        print("Inserting in stcm from CSV")
        files = glob.glob('./stcm/*.csv')
        for f in files:
            print(f)
            df=pd.read_csv(f, delimiter=',')

            o=OmopDB()
            conn = o.getConnection()
            df.to_sql(con=conn, schema=config.omop_db_schema ,name='source_to_concept_map', if_exists='append', index=False)
            conn.close()


    def load_stcm_CIAP(self):
        print("Inserting in stcm from BBDD")
       
        ##mapeamos la ciap a la cie9
        origin_engine = self.getPgOriginConnection() 
        df=pd.read_sql("SELECT mp_epi_ciap1, mp_epi_cie9mc2008, mp_epi_descripcion FROM mfm.cnv_ciap_bdcap;", origin_engine )
        
        omop = OmopDB()

        cie9=pd.read_sql_query("""select c.concept_id , c.concept_code , c.vocabulary_id, c1.concept_id as target_concept_id, c1.vocabulary_id as target_vocabulary_id from omop.concept c 
JOIN omop.CONCEPT_RELATIONSHIP CR
                                        ON C.CONCEPT_ID = CR.CONCEPT_ID_1
                                        AND CR.invalid_reason IS NULL
                                        AND lower(cr.relationship_id) = 'maps to'
                            JOIN omop.CONCEPT C1
                                        ON CR.CONCEPT_ID_2 = C1.CONCEPT_ID
                                        AND C1.INVALID_REASON IS NULL
                            
where c.vocabulary_id in ('ICD9', 'ICD9CM') and c1.standard_concept ='S'""", omop.getConnection())

        df['source_code']=df['mp_epi_ciap1']
        df['source_concept_id']=0
        df['source_vocabulary_id']='CIAP'
        df['source_code_description']=df['mp_epi_descripcion']

        df=df.merge(cie9, left_on='mp_epi_cie9mc2008',  right_on='concept_code', how= 'left') 

        df['target_concept_id']=df['target_concept_id'].astype('Int64')

        
        df['valid_start_date']=date.today()
        df['valid_end_date']=date(2099, 1, 1)

        # Revisamos si tiene valor. En caso contrario indica dropErrors que eliminemos las filas
        self.reviewErrors(df,column='target_concept_id',module_name=self.source_module ,dropErrors=True)
        
        self.model=Source_To_Concept_Map
        self.model_name='source_to_concept_map'

        self.insert(df)


    def load_stcm_ROUTES(self):
        print("Inserting in stcm from BBDD")
       
        ##mapeamos la ciap a la cie9
        origin_engine = self.getPgOriginConnection() 
        df=pd.read_sql("select route_cd , route_st , snomed_cd  from mfm.mf_route mr ", origin_engine )
        
        omop = OmopDB()

        cie9=pd.read_sql_query("""select c.concept_id , c.concept_code , c.vocabulary_id, c1.concept_id as target_concept_id, c1.vocabulary_id as target_vocabulary_id from omop.concept c 
JOIN omop.CONCEPT_RELATIONSHIP CR
                                        ON C.CONCEPT_ID = CR.CONCEPT_ID_1
                                        AND CR.invalid_reason IS NULL
                                        AND lower(cr.relationship_id) = 'maps to'
                            JOIN omop.CONCEPT C1
                                        ON CR.CONCEPT_ID_2 = C1.CONCEPT_ID
                                        AND C1.INVALID_REASON IS NULL
                            
where c.vocabulary_id in ('SNOMED') and c1.standard_concept ='S'""", omop.getConnection())

        df['source_code']=df['route_cd']
        df['source_concept_id']=0
        df['source_vocabulary_id']='ROUTES'
        df['source_code_description']=df['snomed_cd']

        df=df.merge(cie9, left_on='snomed_cd',  right_on='concept_code', how= 'left') 

        df['target_concept_id']=df['target_concept_id'].astype('Int64')

        
        df['valid_start_date']=date.today()
        df['valid_end_date']=date(2099, 1, 1)

        # Revisamos si tiene valor. En caso contrario indica dropErrors que eliminemos las filas
        self.reviewErrors(df,column='target_concept_id',module_name=self.source_module ,dropErrors=True)
        
        self.model=Source_To_Concept_Map
        self.model_name='source_to_concept_map'

        self.insert(df)