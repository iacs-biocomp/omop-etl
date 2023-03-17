from email import header
import pandas as pd
from models.Visit import Visit
from models.models import VisitOccurrence
from models.Base import Base
import config
from time import gmtime, strftime 
from sqlalchemy.sql import text as sa_text
from utils.OmopDB import OmopDB


class visit_findPreceding(Base):

    """ This class contains the logic of reading, extrating and transforming
     patient information to pass it to the OMOP CDM model """

    source= " select * from omop.visit_occurrence2 order by person_id, visit_start_datetime, visit_occurrence_id  "
    """ Import the values from the view Procedimiento_Primario """

    #TODO: REvisar pedro
    def chunker(self, seq, size):
        """ This method is  """
        # from http://stackoverflow.com/a/434328
        return (seq[pos:pos + size] for pos in range(0, len(seq), size))


    def run(self):
        
            print("launch find_preceding_job")
            print("Getting data from source")     
            print(strftime("%H:%M:%S", gmtime()))


            db=OmopDB()
            conn = db.getSession()
            

            chunks=pd.read_sql(self.source, conn, chunksize=config.chunksize)

            tail=pd.DataFrame()
            for df in chunks:
                #print(df[['visit_occurrence_id', 'preceding_visit_occurrence_id']].head())
                df=pd.concat([tail, df])
                print("calculating preceding vistis for %s rows"%len(df))
                print(strftime("%H:%M:%S", gmtime()))  
                #añadimos una columna con las visitas de la fila anterior
                df['preceding_visit_occurrence_id']=df['visit_occurrence_id'].shift()

                #añadimos el id de paciente para controlar que son del mismo paciente
                df['tmpP']=df['person_id'].shift()
                #si la cita anterior es de otro paciente ponemos el campo a None
                failMask= df['person_id']!=df['tmpP'] 
                df.loc[failMask, 'preceding_visit_occurrence_id']=None
                df['preceding_visit_occurrence_id']=df['preceding_visit_occurrence_id'].astype('Int64')
                df['discharge_to_concept_id']=df['discharge_to_concept_id'].astype('Int64')

                df['provider_id']=df['provider_id'].astype('Int64')
                df['care_site_id']=df['care_site_id'].astype('Int64')

                
                

                
                if len(tail)>0:
                    df = df.iloc[3:]
                tail=df.tail(3)
                
                v=Visit()
                v.model=VisitOccurrence
                v.model_name='visit_occurrence'
                v.insertData(df)
                
                


            conn.close()

            # db= OmopDB()
            # con = db.getConnection()            
            # trans = con.begin()
            # con.execute(sa_text("Drop table @cdmDatabaseSchema.visit_occurrence2".replace('@cdmDatabaseSchema', config.omop_db_schema)))
            # trans.commit()
            # con.close()



        