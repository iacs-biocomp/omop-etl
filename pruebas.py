import pandas as pd
from sqlalchemy import create_engine
import cx_Oracle

# df=pd.read_csv("logs/sources.cmbd_observation_concept_id_errors.csv", delimiter=';')


# print(df)


DIALECT = 'oracle'
SQL_DRIVER = 'cx_oracle'
USERNAME = 'MIRTH' #enter your username
PASSWORD = 'ediQD13Ctrl' #enter your password
HOST = '10.34.3.54' #enter the oracle db host url
PORT = 1521 # enter the oracle port number
SERVICE = 'RIS' # enter the oracle db service name
ENGINE_PATH_WIN_AUTH = DIALECT + '+' + SQL_DRIVER + '://' + USERNAME + ':' + PASSWORD +'@' + HOST + ':' + str(PORT) + '/?service_name=' + SERVICE

cx_Oracle.init_oracle_client(lib_dir=r"C:\\oracle\\instantclient_21_3")

engine = create_engine(ENGINE_PATH_WIN_AUTH)


# finance = "(DESCRIPTION = (ADDRESS = (PROTOCOL = TCP)(HOST = 10.34.3.54)(PORT = 1521))   (CONNECT_DATA =  (SID = RIS)   ) )"

# connection = oracledb.connect(user="MIRTH", password='ediQD13Ctrl', dsn=finance)

# engine=connection
# engine = create_engine(
#     "oracle+cx_oracle://MIRTH:ediQD13Ctrl@10.34.3.54:1521:RIS",
#     max_identifier_length=30)

query="""
 select  trim(i.centro) centro, trim(iddesp.descripcio) modalidad, count(*) as pruebas_realizadas
 from idHCMBD i
 inner join iddact a on a.acto= i.prestacion 
 inner join iddesp on iddesp.CODIGOESP = a.especia
 where grupomut='DKV'
 and i.fecha >= STK_FUNC.FECHA_SQL2CLA(TO_DATE('20220101','YYYYMMDD'))
 and i.centro not in ('CG', 'FII', 'PAR', 'CDBHR', 'CDRZA', 'CDRSG')
 and iddesp.CODIGOESP != 17
 group by i.centro, iddesp.descripcio
order by  1
"""
df=pd.read_sql_query(query, engine)

print(df)

df.to_csv('c:/tmp/completo_sin-rayos.csv', sep=';', header=True, index=False)

centros = df.centro.unique()

for c in centros:
    df[df.centro==c].to_csv('c:/tmp/%s.csv'%c, sep=';', header=True, index=False)