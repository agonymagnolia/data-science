from streamlod.handlers import MetadataUploadHandler, ProcessDataUploadHandler, MetadataQueryHandler, ProcessDataQueryHandler
from streamlod.mashups import AdvancedMashup

from streamlod.interface import *

"""
Pip install rich for a better experience

"""

metadata_csv_path = 'streamlod/data/meta.csv'
process_json_path = 'streamlod/data/process.json'
metadata_endpoint = 'http://127.0.0.1:9999/blazegraph/sparql'
process_db_path = 'streamlod/databases/relational.db'

metadata_uh = MetadataUploadHandler()
process_uh = ProcessDataUploadHandler()
metadata_upload_path = metadata_uh.setDbPathOrUrl(metadata_endpoint, reset=True)
process_upload_path = process_uh.setDbPathOrUrl(process_db_path, reset=True)

metadata_push1 = metadata_uh.pushDataToDb(metadata_csv_path)
metadata_push2 = metadata_uh.pushDataToDb(metadata_csv_path) # test to manage multiple pushes
process_push1 = process_uh.pushDataToDb(process_json_path)
process_push2 = process_uh.pushDataToDb(process_json_path) # test to manage multiple pushes

metadata_qh = MetadataQueryHandler()
process_qh = ProcessDataQueryHandler()
metadata_query_path = metadata_qh.setDbPathOrUrl(metadata_endpoint)
process_query_path = process_qh.setDbPathOrUrl(process_db_path)

mashup = AdvancedMashup()
mashup.addMetadataHandler(metadata_qh)
mashup.addMetadataHandler(metadata_qh) # test to manage multiple query handlers
mashup.addProcessHandler(process_qh)
mashup.addProcessHandler(process_qh) # test to manage multiple query handlers

title_print('UPLOAD HANDLERS')
print(f"\n{'—' * WIDTH}\n")

print('Metadata Set Path:')
rich_print(metadata_upload_path)

print('\nMetadata First Push:')
rich_print(metadata_push1)

print('\nMetadata Second Push:')
rich_print(metadata_push2)

print('\nProcess Set Path:')
rich_print(process_upload_path)

print('\nProcess First Push:')
rich_print(process_push1)

print('\nProcess Second Push:')
rich_print(process_push2)


title_print('METADATA QUERY AND MASHUP')

subtitle_print('IdentifiableEntity pippo:')
single_print(metadata_qh.getById('pippo'))
rich_print(mashup.getEntityById('pippo'))

subtitle_print('IdentifiableEntity 13:')
single_print(metadata_qh.getById('13'))
rich_print(mashup.getEntityById('13'))

subtitle_print('IdentifiableEntity 15:')
single_print(metadata_qh.getById('15'))
rich_print(mashup.getEntityById('15'))

subtitle_print('IdentifiableEntity 33:')
single_print(metadata_qh.getById('33'))
rich_print(mashup.getEntityById('33'))

subtitle_print('IdentifiableEntity VIAF:100190422:')
single_print(metadata_qh.getById('VIAF:100190422'))
rich_print(mashup.getEntityById('VIAF:100190422'))

subtitle_print('Authors of Object 13:')
single_print(metadata_qh.getAuthorsOfCulturalHeritageObject('13'))
rich_print(mashup.getAuthorsOfCulturalHeritageObject('13'))

subtitle_print('Objects by VIAF:100190422:')
single_print(metadata_qh.getCulturalHeritageObjectsAuthoredBy('VIAF:100190422'))
rich_print(mashup.getCulturalHeritageObjectsAuthoredBy('VIAF:100190422'))

subtitle_print('All CulturalHeritageObjects:')
single_print(metadata_qh.getAllCulturalHeritageObjects())
rich_print(mashup.getAllCulturalHeritageObjects())

subtitle_print('All People:')
single_print(metadata_qh.getAllPeople())
rich_print(mashup.getAllPeople())


title_print('PROCESS DATA QUERY AND MASHUP')

subtitle_print('Activities on Object 150:')
stack_print(process_qh.getById('150'))

subtitle_print('Activities on Object 10:')
stack_print(process_qh.getById('10'))

subtitle_print('Activities on Object 11:')
stack_print(process_qh.getById('11'))

subtitle_print('Activities on Object 13:')
stack_print(process_qh.getById('13'))

subtitle_print('Activities on Object 14:')
stack_print(process_qh.getById('14'))

subtitle_print('Activities on Object 23:')
stack_print(process_qh.getById('23'))

subtitle_print('Activities on Object 29:')
stack_print(process_qh.getById('29'))

subtitle_print('All Activities:')
stack_print(process_qh.getAllActivities())
rich_print(mashup.getAllActivities())

subtitle_print('Activities by institution Heritage:')
stack_print(process_qh.getActivitiesByResponsibleInstitution('itage'))
rich_print(mashup.getActivitiesByResponsibleInstitution('itage'))

subtitle_print('Activities by person Emily Bronte:')
stack_print(process_qh.getActivitiesByResponsiblePerson('y B'))
rich_print(mashup.getActivitiesByResponsiblePerson('y B'))

subtitle_print('Activities with tool "lab-":')
stack_print(process_qh.getActivitiesUsingTool('lab-'))
rich_print(mashup.getActivitiesUsingTool('lab-'))

subtitle_print('Activities started after 24th October 2023:')
stack_print(process_qh.getActivitiesStartedAfter('2023-10-24'))
rich_print(mashup.getActivitiesStartedAfter('2023-10-24'))

subtitle_print('Activities ended before 4th March 2023:')
stack_print(process_qh.getActivitiesEndedBefore('2023-03-04'))
rich_print(mashup.getActivitiesEndedBefore('2023-03-04'))

subtitle_print('Acquisitons with technique "3d":')
stack_print(process_qh.getAcquisitionsByTechnique('3d'))
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