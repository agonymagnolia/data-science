# Importing all the classes for handling the relational and graph databases
from handlers import ProcessDataUploadHandler, MetadataUploadHandler, ProcessDataQueryHandler, MetadataQueryHandler

# Importing the class for dealing with mashup queries
from mashups import AdvancedMashup

print('\n************************************ UPLOAD HANDLERS *************************************')

# Create the relational database
rel_path = 'relational.db'
process = ProcessDataUploadHandler()
print('\nPROCESS PATH:', process.setDbPathOrUrl(rel_path))
print('\nPROCESS PUSH 1:', process.pushDataToDb('data/process.json'))
print('\nPROCESS PUSH 2:', process.pushDataToDb('data/process.json')) # test to handle multiple pushes

# Create the graph database (remember first to run the Blazegraph instance)
grp_endpoint = 'http://127.0.0.1:9999/blazegraph/sparql'
metadata = MetadataUploadHandler()
print('\nMETADATA PATH:', metadata.setDbPathOrUrl(grp_endpoint))
print('\nMETADATA PUSH 1:', metadata.pushDataToDb('data/meta.csv'))
print('\nMETADATA PUSH 2:', metadata.pushDataToDb('data/meta.csv')) # test to handle multiple pushes

# Create the query handlers for both databases
process_qh = ProcessDataQueryHandler()
process_qh.setDbPathOrUrl(rel_path)

metadata_qh = MetadataQueryHandler()
metadata_qh.setDbPathOrUrl(grp_endpoint)

# Create a advanced mashup object for queries
mashup = AdvancedMashup()
#mashup.addProcessHandler(process_qh)
#mashup.addProcessHandler(process_qh)
mashup.addMetadataHandler(metadata_qh)
mashup.addMetadataHandler(metadata_qh)

print('\n********************************* METADATA QUERY HANDLER *********************************')

print('\nID pippo\n', metadata_qh.getById('pippo'))

print('\nID 13\n', metadata_qh.getById('13'))

print('\nID VIAF:100190422\n', metadata_qh.getById('VIAF:100190422'))

print('\nID 15\n', metadata_qh.getById('15'))

print('\nID 33\n', metadata_qh.getById('33'))

print('\nAUTHORS OF 13\n', metadata_qh.getAuthorsOfCulturalHeritageObject('13'))

print('\nCHO BY VIAF:100190422\n', metadata_qh.getCulturalHeritageObjectsAuthoredBy('VIAF:100190422'))

print('\nALL CHO\n', metadata_qh.getAllCulturalHeritageObjects())

print('\nALL PEOPLE\n', metadata_qh.getAllPeople())

print('\n******************************* PROCESS DATA QUERY HANDLER *******************************')

print('\nID 150\n', process_qh.getById('150'))

print('\nID 2\n', process_qh.getById('2'))

print('\nID 5\n', process_qh.getById('5'))

print('\nID 11\n', process_qh.getById('11'))

print('\nID 13\n', process_qh.getById('13'))

print('\nID 23\n', process_qh.getById('23'))

print('\nID 29\n', process_qh.getById('29'))

print('\nALL A\n', process_qh.getAllActivities())

print('\nA BY INSTITUTION Heritage\n', process_qh.getActivitiesByResponsibleInstitution('itage'))

print('\nA BY PERSON Emily Bronte\n', process_qh.getActivitiesByResponsiblePerson('y B'))

print('\nA BY TOOL Meshes\n', process_qh.getActivitiesUsingTool('mesh'))

print('\nA AFTER 23rd October 23\n', process_qh.getActivitiesStartedAfter('2023-10-23'))

print('\nA BEFORE Today\n', process_qh.getActivitiesEndedBefore('2024-07-02'))

print('\nACQ BY TECHNIQUE scanner\n', process_qh.getAcquisitionsByTechnique('light'))

print('\n************************************** BASIC MASHUP **************************************')

print('\nID pippo\n', mashup.getEntityById('pippo'))

print('\nID 13\n', mashup.getEntityById('13'))

print('\nID VIAF:100190422\n', mashup.getEntityById('VIAF:100190422'))

print('\nID 15\n', mashup.getEntityById('15'))

print('\nID 33\n', mashup.getEntityById('33'))

print('\nAUTHORS OF 13\n', mashup.getAuthorsOfCulturalHeritageObject('13'))

print('\nCHO BY Ulisse Aldrovandi\n', mashup.getCulturalHeritageObjectsAuthoredBy('VIAF:100190422'))

print('\nALL CHO\n', mashup.getAllCulturalHeritageObjects())

print('\nALL PEOPLE\n', mashup.getAllPeople())

print('\nALL A\n', mashup.getAllActivities())

print('\nA BY INSTITUTION Heritage\n', mashup.getActivitiesByResponsibleInstitution('itage'))

print('\nA BY PERSON Emily Bronte\n', mashup.getActivitiesByResponsiblePerson('y B'))

print('\nA BY TOOL Meshes\n', mashup.getActivitiesUsingTool('mesh'))

print('\nA AFTER 23rd October 23\n', mashup.getActivitiesStartedAfter('2023-10-23'))

print('\nA BEFORE Today\n', mashup.getActivitiesEndedBefore('2024-07-02'))

print('\nACQ BY TECHNIQUE Scanner\n', mashup.getAcquisitionsByTechnique('light'))

print('\n************************************* ADVANCED MASHUP *************************************')

print('\nA ON CHO BY Ulisse Aldrovandi\n', mashup.getActivitiesOnObjectsAuthoredBy('VIAF:100190422'))

print('\nCHO PROCESSED BY PERSON Emily Bronte\n', mashup.getObjectsHandledByResponsiblePerson('y B'))

print('\nCHO PROCESSED BY INSTITUTION Heritage\n', mashup.getObjectsHandledByResponsibleInstitution('itage'))

print('\nAUTHOR OF CHO PROCESSED IN April 2023\n', mashup.getAuthorsOfObjectsAcquiredInTimeFrame("2023-04-01", "2023-05-01"))