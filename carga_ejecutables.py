import os

# search_path --> Directorio donde buscar los ficheros
# search_domain --> Nombres de los dominios
# search_extension --> Extension de los ficheros a buscar 
# Un fichero que trabaje en un dominio debe comenzar por el nombre del dominio

def cargar_ejecutables() -> list:
    """ Funcion para automatizar la carga de ficheros de un dominio. Los ficheros 
    de un determinado dominio deben comenzar por el nombre del dominio.
    Por ejemplo: location.py, location2.py, location3.py ...
    La funcion devuleve una lista con el nombre de todos los ficheros encontrados
    """

    result = [] # Lista donde se almacenan los ficheros encontrados
    search_path = "/home/bahia/ETL2/sources/"

    # Nombre de los dominios. Los ficheros de un dominio deben comenzar por el nombre del dominio
    search_domain = ["location","care_site","provider","person","visit","condition","procedure","drug","measurement"]
    search_extension = ".py" # Tipo de fichero

    for domain in search_domain:
        for root, dir, files in os.walk(search_path): # Necesarios root y dir
            for file in files:
                if file.startswith(domain) and file.endswith(search_extension):
                    result.append(file)
    return result