from sqlalchemy.sql import text as sa_text

import config
from utils.OmopDB import OmopDB
 

class final_steps():
    """ This method runs the derivated tables """
    caches=None
    def __init__(self, caches):
        self.caches=caches
    """ This class is encharge to insert the tables  Observation_period, Drug_era, Condition_era    """ 

    def run(self):
        self.insert_derivates()
        self.create_pks()
        self.create_fks()
        self.generate_index()

    def insert_derivates(self):
        print("Inserting Observation Period")

        db= OmopDB()
        con = db.getConnection()
        trans = con.begin()
        file = open('sql/Observation_period.sql')
        sql_query = file.read()
        con.execute(sa_text(sql_query.replace('@cdmDatabaseSchema', config.omop_db_schema)))
        file.close()
        trans.commit()
        

        print("Inserting Condition Era")
        
        trans = con.begin()
        file = open('sql/condition_era_postgresql.sql')
        sql_query = file.read()
        con.execute(sa_text(sql_query.replace('@cdmDatabaseSchema', config.omop_db_schema)))
        file.close()
        trans.commit()
        

        print("Inserting Drug Era")
        
        trans = con.begin()
        file = open('sql/drug_era_postgresql.sql')
        sql_query = file.read()
        con.execute(sa_text(sql_query.replace('@cdmDatabaseSchema', config.omop_db_schema)))
        file.close()
        trans.commit()
        con.close()


    def create_pks(self):
        print("Inserting Primary Keys")
        db= OmopDB()
        con = db.getConnection()
        trans = con.begin()
        file = open('sql/OMOPCDM_postgresql_5.3_primary_keys.sql')
        sql_query = file.read()
        con.execute(sa_text(sql_query.replace('@cdmDatabaseSchema', config.omop_db_schema)))
        file.close()
        trans.commit()
        con.close()

    def create_fks(self):
        print("Inserting Foreing keys")
        db= OmopDB()
        con = db.getConnection()
        trans = con.begin()
        file = open('sql/OMOPCDM_postgresql_5.3_constraints.sql')
        sql_query = file.read()
        con.execute(sa_text(sql_query.replace('@cdmDatabaseSchema', config.omop_db_schema)))
        file.close()
        trans.commit()
        con.close()

    def generate_index(self):
        print("Generating Indexes")
        db= OmopDB()
        con = db.getConnection()
        trans = con.begin()
        file = open('sql/OMOPCDM_postgresql_5.3_indices.sql')
        sql_query = file.read()
        con.execute(sa_text(sql_query.replace('@cdmDatabaseSchema', config.omop_db_schema)))
        file.close()
        trans.commit()
        con.close()