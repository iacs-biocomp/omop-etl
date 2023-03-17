from distutils.command.config import config
from unittest import result
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement
from cassandra.concurrent import execute_concurrent
from cassandra.policies import RetryPolicy, RoundRobinPolicy
import pandas as pd 
import time


class Cassandra():

    _session=None

    def pandas_factory(self, colnames, rows):
        """ This method is calling pandas library """
        
        return pd.DataFrame(rows, columns=colnames)

    def getSession(self):
        """ This method starts the session """

        if self._session is None:
            return self.session()
        
        return self._session

    def session(self):
        """ This method set up the details for starting the session """

        cluster = Cluster(
            contact_points=['172.25.3.100','172.25.3.101','172.25.3.102','172.25.3.103'], 
            auth_provider = PlainTextAuthProvider(username='bahia', password='bahia'),
            idle_heartbeat_interval=0
        )
        #cluster.default_retry_policy=RetryPolicy.RETRY_NEXT_HOST

        session= cluster.connect()
        session.set_keyspace('biganlake')
        session.row_factory = self.pandas_factory
        session.fetch_size = None
        session.default_timeout=None

        self._session=session

        return session
    
    def cas2pandas(self, query, param=None, keyspace=None):
        """ This method extracts the data from cassandra """

        

         # fetches a fresh chunk with given page state 
        def fetch_a_fresh_chunk(paging_state = None, query = None, param=None, fetch_size=None, i=1):
            if param is not None:
                query=query+"'%s' "%param
            try:
                statement   = SimpleStatement(query)
                if fetch_size is not None:
                    statement   = SimpleStatement(query, fetch_size = fetch_size)
                results     = session.execute(statement, paging_state=paging_state)
            except:
                
                fetch_size=int(fetch_size/2)
                

                print("Error to connect to cassandra, Retry in %ss. %s , fetch_size=%s"%(i,query, fetch_size))
                time.sleep(i)
                if(fetch_size < 5000):
                    results = fetch_a_fresh_chunk(paging_state, query, param, i=i+1)
                else:
                    results = fetch_a_fresh_chunk(paging_state, query, param, fetch_size=fetch_size , i=i+1)

            return results  



        
        session = self.getSession()
        
        if keyspace is not None:
            session.set_keyspace(keyspace)
       
        df=pd.DataFrame()
        # set fetch size
        fetch_size = 125000

        # It will print first 100 records
        next_page_available = True
        paging_state        = None
        data_count          = 0

        while next_page_available is True:
            # fetches a new chunk with given page state
            results = fetch_a_fresh_chunk(paging_state, query, param, fetch_size=fetch_size)
            paging_state = results.paging_state
            next_page_available=results.has_more_pages
            cdf=results._current_rows

            df=pd.concat([df, cdf])
           
        
        return df

    def cassConcurrent(self,query, params):
        """ This method executes the current session """

        session=self.session()
        select_statement=session.prepare(query)
        statements_and_params=[]
        for p in params:
            
            statements_and_params.append((select_statement, p))
        results= execute_concurrent(session, statements_and_params, raise_on_first_error=True)

        cdf=pd.DataFrame()
        for (succes, result) in results:
            if not succes:
                print(succes)
                continue
            cdf=pd.concat([result._current_rows, cdf])
            print(len(cdf))

        return results
