import csv
with open ('logs/sources.cmbd_procedure_concept_id_errors.csv') as file:
    filecontent = csv.reader(file,delimiter=';')
    for row in filecontent:
        print(row)