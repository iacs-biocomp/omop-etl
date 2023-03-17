import pandas as pd
from models.Base import Base
from time import gmtime, strftime
from tqdm import tqdm
import numpy as np
import time
import requests
from utils.casandra import Cassandra
import config
import csv
import os
import diskcache as dc

class drug_phar_disp(Base):

    """ This class contains the logic of reading, extrating and transforming
     patient information to pass it to the OMOP CDM model """
    

    def run(self):
        """ This method runs the jobs """
        print("launch %s"%self.source_module)
        print("Getting data from source")
        print(strftime("%H:%M:%S", gmtime()))

        cassandra=Cassandra()
        df=pd.DataFrame()

        query = 'select distinct event_yr, event_mth from phar_disp '
        cdf=cassandra.cas2pandas(query)
        cdf=cdf[(cdf.event_yr >=config.startyear) & (cdf.event_yr <=config.endyear)]
        cdf=cdf.reset_index()



        for index, c in cdf.iterrows():
          query = "select * from phar_disp  where  event_yr=%s and event_mth=%s "%(c.event_yr, c.event_mth )
          drug=cassandra.cas2pandas(query)
          df=pd.concat([df, drug])

          if len(df)>=config.chunksize  or index == cdf.index[-1]:

            print("Mapping data")
            print(strftime("%H:%M:%S", gmtime()))

            df['origin_source_data_provider']='biganlake'
            df['origin_source_name']='phar_disp'
            df['origin_source_value']=df['disp_id']


            df.rename(columns={'person_id':'person'}, inplace=True)
            df['type_concept_id']=32825

            df['start_timestamp']=pd.to_datetime(df.event_dt.astype(str),errors='coerce', infer_datetime_format=True, utc=True).dt.tz_convert("Europe/Madrid")
            df=self.reviewErrors(df, 'start_timestamp',self.source_module, dropErrors=True)

            df['end_timestamp']=df['start_timestamp']

            
            df['concept']=df.pharactivesubs_cd
            #TODO mirar la route -> Beatriz
            df['route']=None
            df['quantity']=df.dose_qty_nm

            #TODO mirar como funciona dose -> Beatriz
            df['dose_unit_source_value']=df['dose_unit_st']

            df=self.mapConcepts(df,vocabularios=['ATC'] )
            df=self.mapCN(df, vocab='ATC')

            df['domain']=df['domain'].fillna('Drug')
            self.sendToDomains(df)


            df=pd.DataFrame()


    

    def mapCN(self, df, vocab):
        """ This method is mapping the codes that are not ATC, save it in a csv and delete the list codes"""

        df.loc[pd.isna(df['concept_id']), ['concept']]=None
        #si falla utilizamos el pharcode_cd que es codigo nacional

        # buscar las filas drup_concept_id = NULL,
        codigossinATC=df[df['concept_id'].isnull()].copy()
        # obtener los distintos  refer o cn   (unique)
        listaCodigos=codigossinATC['pharcode_cd'].unique()
        # lista de cn y lo mandamos a la funcion codigo (CodigoNacional2Standard(self, cn))

        cn2Atc={}
        #cargamos los codigos de un csv y elimnamos los que esten del listado
        #si la fecha del fichero es > 1 mes lo borramos y volvemos a cargar
        if 'CodNacCache' not in self.caches:
            self.caches.set('CodNacCache', dc.Cache())
        if os.path.exists('cache/atc.csv',) and len(self.caches['CodNacCache'])==0:
            with open('cache/atc.csv', 'r') as csvfile:
                reader= csv.reader(csvfile, delimiter='|')
                for row in reader:
                    self.caches['CodNacCache'].set(row[0], row[1])



        self.CnTotalStat=len(listaCodigos)
        self.CncacheStat=0
        self.CnApiStat=0
        self.CnError=0
        with tqdm(total=len(listaCodigos), desc='Drugs from CIMA') as pbar:
            for refer in listaCodigos:
                if refer not in [None, np.nan]:
                    atc= self.CodigoNacional2Standard(str(refer))
                    cn2Atc[refer]=atc
                    # añadimos el codigo a csv para posteriores cargas
                    with open('cache/atc.csv', 'a') as csvfile:
                        writer= csv.writer(csvfile, delimiter='|')
                        writer.writerow([refer,atc])
                pbar.update(1)

        print("Searching %s codes"%self.CnTotalStat)
        print("%s codes from cache"%self.CncacheStat)
        print("%s codes from api"%self.CnApiStat)
        print("%s codes from Error"%self.CnError)

        df['CodNacATC']=df.pharcode_cd.map(cn2Atc.get)

        df['concept']= df['concept'].fillna(df['CodNacATC'])

        self.mapCp(df, vocab)


        return df




    def CodigoNacional2Standard(self, cn):  #lo van a pasar a ATC
        """ This method is mapping source codes from Codigo Nacional del medicamento to Standard """

    #TODO:Pendiente de recibir la vista  si le paso el refer me devuelve el ATC de los que no existen


        if 'CodNacCache' not in self.caches:
            self.caches.set('CodNacCache', dc.Cache())

        if cn in self.caches['CodNacCache']:
            self.CncacheStat=self.CncacheStat+1
            return self.caches['CodNacCache'][cn]


        #print("Getting %s from cima.aemps.es"%cn)
        time.sleep(0.1)
        #hace una llamada por sg

        try:
            url="https://cima.aemps.es/cima/rest/medicamento?cn=%s"%cn
            r = requests.get(url)
            response=r.json()
            for atc in response["atcs"]:
                if atc['nivel']==5:
                    self.caches['CodNacCache'].set(cn, atc['codigo'])
                    self.CnApiStat=self.CnApiStat+1
                    return atc['codigo']
        except:
              #print( 'error de conexion url=%s'%url)
              self.CnError=self.CnError+1
              self.caches['CodNacCache'].set(cn, None)
              return None