import chunk
import pandas as pd
import diskcache as dc
from tqdm import tqdm


def load_cache(cacheName, dbConnection, schema='omop', cache=None, source=None):
    """This method loads the data saved in cache from the OMOP tables """
    cache_id='%s_id'%cacheName
    cache_source_value='%s_source_value'%cacheName

    if cacheName=='visit_occurrence':
        
            
            print(source)
            cache_source_value='visit_source_value'
            cdf=pd.read_sql_query("select visit_occurrence_id, origin_source_value, origin_source_name from omop.visit_occurrence where origin_source_name='%s' and origin_source_value !='No pk' "%source,
                                    dbConnection)
            
            

            data =pd.Series(cdf['visit_occurrence_id'].values,index=cdf['origin_source_value'].astype(str)).to_dict()
            cache['visit_occurrence'].set(source,data)
        
            return

    if cacheName=='provider_idTocare_site_id':
        cacheName='provider'
        cache_id='care_site_id'
        cache_source_value='provider_id'

    cdf=pd.read_sql_table(cacheName, dbConnection, schema=schema,columns=[cache_id, cache_source_value])
    return pd.Series(cdf[cache_id].values,index=cdf[cache_source_value].astype(str)).to_dict()
    
