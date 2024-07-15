from streamlod.handlers import MetadataUploadHandler, ProcessDataUploadHandler, MetadataQueryHandler, ProcessDataQueryHandler
from streamlod.mashups import AdvancedMashup
from streamlod.interface import *

"""
Pip install rich for a better experience

"""

metadata_csv_path = 'streamlod/data/meta.csv'
metadata_csv_path_bis = 'streamlod/data/meta_bis.csv'
metadata_csv_path2 = 'streamlod/data/meta2.csv'
process_json_path = 'streamlod/data/process.json'
process_json_path_bis = 'streamlod/data/process_bis.json'
process_json_path2 = 'streamlod/data/process2.json'
metadata_endpoint = 'http://127.0.0.1:9999/blazegraph/sparql'
metadata_endpoint2 = 'http://127.0.0.1:19999/blazegraph/sparql'	# To access the second instance: java -server -Xmx4g -Djetty.port=19999 -jar blazegraph.jar
process_db_path = 'streamlod/databases/relational.db'
process_db_path2 = 'streamlod/databases/relational2.db'


metadata_uh = MetadataUploadHandler()
process_uh = ProcessDataUploadHandler()

metadata_uh2 = MetadataUploadHandler() # Second graph db
process_uh2 = ProcessDataUploadHandler() # Second relational db

metadata_qh = MetadataQueryHandler()
process_qh = ProcessDataQueryHandler()

metadata_qh2 = MetadataQueryHandler() # Second metadata query handler
process_qh2 = ProcessDataQueryHandler() # Second process query handler

mashup = AdvancedMashup()
mashup.addMetadataHandler(metadata_qh)
mashup.addMetadataHandler(metadata_qh2) # Manage multiple metadata query handlers
mashup.addProcessHandler(process_qh)
mashup.addProcessHandler(process_qh2) # Manage multiple process query handlers

metadata_query_path = metadata_qh.setDbPathOrUrl(metadata_endpoint)
process_query_path = process_qh.setDbPathOrUrl(process_db_path)

metadata_query_path2 = metadata_qh2.setDbPathOrUrl(metadata_endpoint2)
process_query_path2 = process_qh2.setDbPathOrUrl(process_db_path2)


title_print('UPLOAD HANDLERS')
print(f"\n{'—' * WIDTH}\n")

print('Metadata Set 1st Path:')
rich_print(metadata_uh.setDbPathOrUrl(metadata_endpoint, reset=True))

print('\nMetadata Set 2nd Path:')
rich_print(metadata_uh2.setDbPathOrUrl(metadata_endpoint2, reset=True))

print('\nMetadata 1st Push:')
rich_print(metadata_uh.pushDataToDb(metadata_csv_path))

print('\nMetadata 2nd Push, same file, same DB:')
rich_print(metadata_uh.pushDataToDb(metadata_csv_path))

print('\nMetadata 3rd Push, different file, same DB:')
rich_print(metadata_uh.pushDataToDb(metadata_csv_path_bis)) # Manage multiple pushes on same db

print('\nMetadata 4th Push, second DB:')
rich_print(metadata_uh2.pushDataToDb(metadata_csv_path2)) # Push on second db

print('\nProcess Set 1st Path:')
rich_print(process_uh.setDbPathOrUrl(process_db_path, reset=True))

print('\nProcess Set 2nd Path:')
rich_print(process_uh2.setDbPathOrUrl(process_db_path2, reset=True))

print('\nProcess 1st Push:')
rich_print(process_uh.pushDataToDb(process_json_path))

print('\nProcess 2nd Push, same file, same DB:')
rich_print(process_uh.pushDataToDb(process_json_path))

print('\nProcess 3rd Push, different file, same DB:')
rich_print(process_uh.pushDataToDb(process_json_path_bis)) # Multiple pushes on same db

print('\nProcess 4th Push, second DB:')
rich_print(process_uh2.pushDataToDb(process_json_path2)) # Push on second db


title_print('METADATA QUERY AND MASHUP')

subtitle_print('IdentifiableEntity pippo:')
single_print(metadata_qh.getById('pippo'))
print(f"{' —' * (WIDTH // 2)}\n")
single_print(metadata_qh2.getById('pippo'))
rich_print(mashup.getEntityById('pippo'))

subtitle_print('IdentifiableEntity 13:')
single_print(metadata_qh.getById('13'))
print(f"{' —' * (WIDTH // 2)}\n")
single_print(metadata_qh2.getById('13'))
rich_print(mashup.getEntityById('13'))

subtitle_print('IdentifiableEntity 15:')
single_print(metadata_qh.getById('15'))
print(f"{' —' * (WIDTH // 2)}\n")
single_print(metadata_qh2.getById('15'))
rich_print(mashup.getEntityById('15'))

subtitle_print('IdentifiableEntity 27:')
single_print(metadata_qh.getById('27'))
print(f"{' —' * (WIDTH // 2)}\n")
single_print(metadata_qh2.getById('27'))
rich_print(mashup.getEntityById('27'))

subtitle_print('IdentifiableEntity 33:')
single_print(metadata_qh.getById('33'))
print(f"{' —' * (WIDTH // 2)}\n")
single_print(metadata_qh2.getById('33'))
rich_print(mashup.getEntityById('33'))

subtitle_print('IdentifiableEntity "alo":')
single_print(metadata_qh.getById('alo'))
print(f"{' —' * (WIDTH // 2)}\n")
single_print(metadata_qh2.getById('alo'))
rich_print(mashup.getEntityById('alo'))

subtitle_print('IdentifiableEntity VIAF:100190422:')
single_print(metadata_qh.getById('VIAF:100190422'))
print(f"{' —' * (WIDTH // 2)}\n")
single_print(metadata_qh2.getById('VIAF:100190422'))
rich_print(mashup.getEntityById('VIAF:100190422'))

subtitle_print('Authors of Object "alo":')
single_print(metadata_qh.getAuthorsOfCulturalHeritageObject('alo'))
print(f"{' —' * (WIDTH // 2)}\n")
single_print(metadata_qh2.getAuthorsOfCulturalHeritageObject('alo'))
rich_print(mashup.getAuthorsOfCulturalHeritageObject('alo'))

subtitle_print('Objects by VIAF:100190422:')
single_print(metadata_qh.getCulturalHeritageObjectsAuthoredBy('VIAF:100190422'))
print(f"{' —' * (WIDTH // 2)}\n")
single_print(metadata_qh2.getCulturalHeritageObjectsAuthoredBy('VIAF:100190422'))
rich_print(mashup.getCulturalHeritageObjectsAuthoredBy('VIAF:100190422'))

subtitle_print('All CulturalHeritageObjects:')
single_print(metadata_qh.getAllCulturalHeritageObjects())
print(f"{' —' * (WIDTH // 2)}\n")
single_print(metadata_qh2.getAllCulturalHeritageObjects())
rich_print(mashup.getAllCulturalHeritageObjects())

subtitle_print('All People:')
single_print(metadata_qh.getAllPeople())
print(f"{' —' * (WIDTH // 2)}\n")
single_print(metadata_qh2.getAllPeople())
rich_print(mashup.getAllPeople())


title_print('PROCESS DATA QUERY AND MASHUP')

subtitle_print('Activities on Object 150:')
stack_print(process_qh.getById('150'))
print(f"{' —' * (WIDTH // 2)}\n")
stack_print(process_qh2.getById('150'))

subtitle_print('Activities on Object 10:')
stack_print(process_qh.getById('10'))
print(f"{' —' * (WIDTH // 2)}\n")
stack_print(process_qh2.getById('10'))

subtitle_print('Activities on Object 11:')
stack_print(process_qh.getById('11'))
print(f"{' —' * (WIDTH // 2)}\n")
stack_print(process_qh2.getById('10'))

subtitle_print('Activities on Object 13:')
stack_print(process_qh.getById('13'))
print(f"{' —' * (WIDTH // 2)}\n")
stack_print(process_qh2.getById('13'))

subtitle_print('Activities on Object 14:')
stack_print(process_qh.getById('14'))
print(f"{' —' * (WIDTH // 2)}\n")
stack_print(process_qh2.getById('14'))

subtitle_print('Activities on Object 23:')
stack_print(process_qh.getById('23'))
print(f"{' —' * (WIDTH // 2)}\n")
stack_print(process_qh2.getById('23'))

subtitle_print('Activities on Object 29:')
stack_print(process_qh.getById('29'))
print(f"{' —' * (WIDTH // 2)}\n")
stack_print(process_qh2.getById('29'))

subtitle_print('All Activities:')
stack_print(process_qh.getAllActivities())
print(f"{' —' * (WIDTH // 2)}\n")
stack_print(process_qh2.getAllActivities())
rich_print(mashup.getAllActivities())

subtitle_print('Activities by institution Heritage:')
stack_print(process_qh.getActivitiesByResponsibleInstitution('itage'))
print(f"{' —' * (WIDTH // 2)}\n")
stack_print(process_qh2.getActivitiesByResponsibleInstitution('itage'))
rich_print(mashup.getActivitiesByResponsibleInstitution('itage'))

subtitle_print('Activities by person Emily Bronte:')
stack_print(process_qh.getActivitiesByResponsiblePerson('y B'))
print(f"{' —' * (WIDTH // 2)}\n")
stack_print(process_qh2.getActivitiesByResponsiblePerson('y B'))
rich_print(mashup.getActivitiesByResponsiblePerson('y B'))

subtitle_print('Activities with tool "lab-":')
stack_print(process_qh.getActivitiesUsingTool('lab-'))
print(f"{' —' * (WIDTH // 2)}\n")
stack_print(process_qh2.getActivitiesUsingTool('lab-'))
rich_print(mashup.getActivitiesUsingTool('lab-'))

subtitle_print('Activities started after 24th October 2023:')
stack_print(process_qh.getActivitiesStartedAfter('2023-10-24'))
print(f"{' —' * (WIDTH // 2)}\n")
stack_print(process_qh2.getActivitiesStartedAfter('2023-10-24'))
rich_print(mashup.getActivitiesStartedAfter('2023-10-24'))

subtitle_print('Activities ended before 4th March 2023:')
stack_print(process_qh.getActivitiesEndedBefore('2023-03-04'))
print(f"{' —' * (WIDTH // 2)}\n")
stack_print(process_qh2.getActivitiesEndedBefore('2023-03-04'))
rich_print(mashup.getActivitiesEndedBefore('2023-03-04'))

subtitle_print('Acquisitons with technique "3d":')
stack_print(process_qh.getAcquisitionsByTechnique('3d'))
print(f"{' —' * (WIDTH // 2)}\n")
stack_print(process_qh2.getAcquisitionsByTechnique('3d'))
rich_print(mashup.getAcquisitionsByTechnique('3d'))


title_print('ADVANCED MASHUP')

subtitle_print('Activities on Objects by VIAF:263904234 (Tarsizio Riviera):')
rich_print(mashup.getActivitiesOnObjectsAuthoredBy('VIAF:263904234'))

subtitle_print('Objects handled by person Emily Bronte:')
rich_print(mashup.getObjectsHandledByResponsiblePerson('y B'))

subtitle_print('Objects handled by institution Heritage:')
rich_print(mashup.getObjectsHandledByResponsibleInstitution('itage'))

subtitle_print('Authors of Objects acquired in April 2023:')
rich_print(mashup.getAuthorsOfObjectsAcquiredInTimeFrame("2023-04-01", "2023-05-01"))