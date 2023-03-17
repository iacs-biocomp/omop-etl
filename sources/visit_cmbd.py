import datetime
import pandas as pd
from models.Base import Base
from models.Visit import Visit
from models.Condition import Condition
from models.Procedure import Procedure
import numpy as np         
from time import gmtime, strftime 
import os
import config 

# Cambio de cmbd ---> visit_cmbd
class visit_cmbd(Base):

    """ This class contains the logic of reading, extrating and transforming
     patient information to pass it to the OMOP CDM model """


    def run(self):
        """ Run the job"""
        print("launch %s"%self.source_module)
        print("Getting data from source")     
        print(strftime("%H:%M:%S", gmtime()))  # Calcula mal la hora 
        
                
        origin_engine = self.getPgOriginConnection() 
        chunklist=pd.read_sql_query("select id, cia_obf, hospital, servalt_css, fecing, fecalt, tipalt,p1, p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,d1,d2,d3,d4,d5,d6,d7,d8,d9,d10,d11,d12,d13,d14,d15 from cmbd.cmbd",
                                    origin_engine ,chunksize=(int(config.chunksize/10)))
        
        
        for df in chunklist:
            """ A mapping of all the necessary data is carried out in the table condition_occurrence
            This table contains records of activities or processes ordered by, or carried out by, 
            a healthcare provider on the patient with a diagnostic or therapeutic purpose
            For more information on records, visit https://ohdsi.github.io/CommonDataModel/cdm54.html#condition_OCCURRENCE  """

            df=df[(df.fecalt.dt.year >= config.startyear) | (pd.isna(df.fecalt))]
            if len(df)==0:
                continue

            print("Mapping data")
            print(strftime("%H:%M:%S", gmtime())) 

            df['origin_source_data_provider']='bigandata'
            df['origin_source_name']='cmbd.cmbd'
            df['origin_source_value']=df['id']
            
            
            df['visit_source_value']='cmbd.cmbd'
            df['person']=df.cia_obf

            # El campo de la tabla debe ser condition_type_concept_id ?  
            # BTW 32817: EHR 
            df['type_concept_id']=32817

            df['provider']= df['hospital'] + df['servalt_css']

            df['start_timestamp']=pd.to_datetime(df.fecing.astype(str),errors='coerce', infer_datetime_format=True, utc=True).dt.tz_convert("Europe/Madrid")
            df=self.reviewErrors(df, 'start_timestamp',self.source_module, dropErrors=True)
            df['end_timestamp']=pd.to_datetime(df.fecalt.astype(str),errors='coerce', infer_datetime_format=True, utc=True).dt.tz_convert("Europe/Madrid")
            df=self.reviewErrors(df, 'end_timestamp',self.source_module, dropErrors=True)
 
            
            df['visit_concept_id']=9201 
            df['visit_source_concept_id']=0



            print("Procesing Visits")
            print(strftime("%H:%M:%S", gmtime())) 
            vs=Visit(self.caches)
            vs.source_module=self.source_module
            #Alta a domicio, segun la documentacion = 0
            #para el resto de codigos obtendremos los valores que indican en la documentacion(Dominio Visit + Standard) e intentaremos mapear
            #para la query buscamos un maps to cuyo target domain=visit
            visitConceptIDMap = vs.getSnomedVisitDomain()
            visitConceptIDMap['1']=0
            df['discharge_to_concept_id']=df['tipalt'].astype(str).map(visitConceptIDMap.get)
            df['discharge_to_concept_id']=df['discharge_to_concept_id'].astype('Int64')
            df['discharge_to_source_value']=df['tipalt']
            
            
            df=vs.process(df)
            
            conditions=['d1','d2','d3','d4','d5','d6','d7','d8','d9','d10','d11','d12','d13','d14','d15']
            procedures=['p1', 'p2','p3','p4','p5','p6','p7','p8','p9','p10','p11','p12','p13','p14','p15']
            base_columns=['person', 'cia_obf', 'hospital', 'servalt_css', 'start_timestamp', 'end_timestamp', 'type_concept_id', 'visit_occurrence_id', 'provider_id', 'care_site_id', 'origin_source_data_provider', 'origin_source_name','origin_source_value']
            needed_columns=base_columns+conditions+procedures


            print("Melting data")
            print(strftime("%H:%M:%S", gmtime())) 
            df=df[needed_columns]
            # JOIN con la tabla de conceptos
            # MELT (DATAFRAME, ID_VARS)
            df=df.melt(id_vars=base_columns, value_name='concept')
            df=df.dropna()

            

            print("Procesing conditions")
            print(strftime("%H:%M:%S", gmtime())) 
            cdf=df[df.variable.isin(conditions)].copy()
            # Hacer split del dt en funcion de la fecha de consulta. Anterior a 2016 --> V9 else V10 ??? 

            diag_map={'d1':32902}
            cdf['condition_status_concept_id']=cdf['variable'].map(diag_map.get)
            cdf['condition_status_concept_id']=cdf['condition_status_concept_id'].fillna(32908)
            cdf['condition_status_concept_id']=cdf['condition_status_concept_id'].astype('Int64')
            cdf['condition_status_source_value']=cdf['variable']

            #procesamos primero lo anterior a 2016
            fecha = datetime.datetime(2016,1,1,0,0,0)
            cdfCie9=cdf[cdf['start_timestamp'].dt.tz_localize(None) < fecha].copy()
            # Mapeamos primero a ICD9CM
            cdfCie9 = self.mapConcepts(cdfCie9, vocabularios=["ICD9CM"])
            cie9Mask=pd.isna(cdfCie9['domain'])
            unmaped=cdfCie9[cie9Mask]
            cdfCie9.dropna(subset = ['domain'], inplace=True)
            if len(cdfCie9)>0:
                self.sendToDomains(cdfCie9)
            
            #procesamos el resto y los que no se pudieron mapear ya que hay cie10 en el caso anterior
            cdf=cdf[cdf['start_timestamp'].dt.tz_localize(None) >= fecha].copy()
            cdf=pd.concat([unmaped, cdf])
            cdf=self.mapConcepts(cdf, ['ICD10CM', 'ICD10', 'ICD9CM'])
            cdf['domain']=cdf['domain'].fillna('Condition')
            #print(cdf[cdf.duplicated()])
            self.sendToDomains(cdf)

            
            print("Procesing procedures")
            print(strftime("%H:%M:%S", gmtime())) 
            pdf=df[df.variable.isin(procedures)].copy()

            # Mapeamos primero a ICD9CM
            #procesamos primero lo anterior a 2016
            fecha = datetime.datetime(2016,1,1,0,0,0)
            pdfCie9=pdf[pdf['start_timestamp'].dt.tz_localize(None) < fecha].copy()
            # Mapeamos primero a ICD9CM
            pdfCie9 = self.mapConcepts(pdfCie9, vocabularios=["ICD9Proc"])
            unmaped=pdfCie9[pd.isna(pdfCie9['domain'])]
            pdfCie9.dropna(subset = ['domain'], inplace=True)
            if len(pdfCie9)>0:
                self.sendToDomains(pdfCie9)
       


            pdf=pdf[pdf['start_timestamp'].dt.tz_localize(None) >= fecha]
            pdf=pd.concat([unmaped, pdf])
            pdf=self.mapConcepts(pdf, ['ICD10PCS', 'ICD9Proc'])
            pdf['domain']=pdf['domain'].fillna('Procedure')
            self.sendToDomains(pdf)



            
            
            

         