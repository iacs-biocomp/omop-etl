import config
import psycopg2
from io import StringIO
from sqlalchemy import create_engine
import csv
from utils.Database import Database


class Postgres(Database):
    
    
    def create_omop_engine(self):
        omop_engine=create_engine(config.omop_connection_string)
        return omop_engine
    
        
    def get_connection(self):
        """ This method starts the connection with Postgres DB """
        params_dic = {
                    "host"      : config.omop_db_host,
                    "database"  : config.omop_db_name,
                    "user"      : config.omop_db_user, 
                    "password"  : config.omop_db_pass,
                    "options" :'-c search_path='+config.omop_db_schema
        }
        
        return psycopg2.connect(**params_dic)



    def bulk_insert(self, df, table, table_model, schema=None, index=False, header=True, columns=None):
        """ This method save dataframe in a memory buffer """

        # save dataframe to an in memory buffer
        buffer = StringIO()
        
        #df.to_csv('c:/tmp/inserts.csv', index=index, header=header, sep="|")
        df.to_csv(buffer, index=index, header=header, sep="|")
        buffer.seek(0)
        
        conn = self.get_connection()    
        cursor = conn.cursor()
        try:
            if schema is not None:
                table='%s.%s'%(schema, table)

            sql="COPY %s(%s) FROM STDIN  WITH DELIMITER E'|' CSV HEADER;"%(table,','.join(columns))
            cursor.copy_expert(sql, buffer)
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            conn.rollback()
            cursor.close()
            return 1
    
        cursor.close()

    def bulk_insert_csv(self, ruta, table_name, sep):
        """ This method inserts the data from the csv """

        f = open(ruta, 'r', encoding='utf-8')
     
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
                cursor.execute('truncate table %s'%table_name)
                conn.commit()
                #cursor.copy_from(f, table_name, sep=sep)
                sql="COPY %s FROM STDIN  WITH DELIMITER E'\t' CSV HEADER QUOTE E'\b' ;"%table_name
                cursor.copy_expert(sql, f)
                conn.commit()
                cursor.close()
                conn.close()
        
        except (Exception, psycopg2.DatabaseError) as error:
                
                print("Error: %s" % error)
                conn.rollback()
                cursor.close()
        
            