# Importing all the classes for handling the relational and graph databases
from handlers import ProcessDataUploadHandler, MetadataUploadHandler, ProcessDataQueryHandler, MetadataQueryHandler

# Importing the class for dealing with mashup queries
from mashups import AdvancedMashup

# Create the relational database
#rel_path = "relational.db"
#process = ProcessDataUploadHandler()
#process.setDbPathOrUrl(rel_path)
#process.pushDataToDb("data/process.json")
#process.pushDataToDb("data/process.json")

# Create the graph database (remember first to run the Blazegraph instance)
grp_endpoint = "http://127.0.0.1:9999/blazegraph/sparql"
metadata = MetadataUploadHandler()
print(metadata.setDbPathOrUrl(grp_endpoint))
print(metadata.pushDataToDb("data/meta.csv"))
print(metadata.pushDataToDb("data/meta.csv"))

# Create the query handlers for both databases
#process_qh = ProcessDataQueryHandler()
#process_qh.setDbPathOrUrl(rel_path)

metadata_qh = MetadataQueryHandler()
metadata_qh.setDbPathOrUrl(grp_endpoint)

# Create a advanced mashup object for queries
mashup = AdvancedMashup()
#mashup.addProcessHandler(process_qh)
mashup.addMetadataHandler(metadata_qh)

###################################################################################################
print('\nMETADATA QUERY HANDLER')

print('\nID pippo\n', metadata_qh.getById('pippo'))

print('\nID 13\n', metadata_qh.getById('13'))

print('\nID VIAF:100190422\n', metadata_qh.getById('VIAF:100190422'))

print('\nID 15\n', metadata_qh.getById('15'))

print('\nID 33\n', metadata_qh.getById('33'))

print('\nAUTHORS OF 13\n', metadata_qh.getAuthorsOfCulturalHeritageObject('13'))

print('\nCHO BY VIAF:100190422\n', metadata_qh.getCulturalHeritageObjectsAuthoredBy('VIAF:100190422'))

print('\nALL CHO\n', metadata_qh.getAllCulturalHeritageObjects())

print('\nALL PEOPLE\n', metadata_qh.getAllPeople())


print('\nBASIC MASHUP')

print('\nID pippo\n', mashup.getEntityById('pippo'))

print('\nID 13\n', mashup.getEntityById('13'))

print('\nID VIAF:100190422\n', mashup.getEntityById('VIAF:100190422'))

print('\nID 15\n', mashup.getEntityById('15'))

print('\nID 33\n', mashup.getEntityById('33'))

#print('\nAUTHORS OF 13\n', mashup.getAuthorsOfCulturalHeritageObject('13'))

#print('\nCHO BY VIAF:100190422\n', mashup.getCulturalHeritageObjectsAuthoredBy('VIAF:100190422'))

print('\nALL CHO\n', mashup.getAllCulturalHeritageObjects())

print('\nALL PEOPLE\n', mashup.getAllPeople())


#mashup.getAllActivities()
#mashup.getAuthorsOfObjectsAcquiredInTimeFrame("2023-04-01", "2023-05-01")
# etc...