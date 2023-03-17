import config
from tqdm import tqdm

def _chunker(self, seq, size):
        # from http://stackoverflow.com/a/434328
        return (seq[pos:pos + size] for pos in range(0, len(seq), size))
    
def insert_with_progress(self, df, model ,table_name, engine, schema=None,columns=None ): 
        """ This method inserts data from the dataframe and it also shows a bar graphic with the task progress  """  
        
        #funcion que inserta el dataframe
        chunksize = int(len(df) / 10) # 10%
        if chunksize==0:
            chunksize=1
        
        with tqdm(total=len(df)) as pbar:
            for i, cdf in enumerate(self.chunker(df, chunksize)):
               
                #cdf.to_csv("c:\\tmp\\%s%d.csv"%(self.model_name,i), sep=';', index=False)

                if config.useBulkInsert:
                    db = engine
                    db.bulk_insert(cdf, table_name ,model, schema=schema ,index=False,columns=columns)
                else:
                    conn = engine.getOmopConnection()
                    cdf.to_sql(con=conn, schema=schema ,name=table_name, if_exists='append', index=False)
              
                

                pbar.update(chunksize)
