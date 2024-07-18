"""
To run the test navigate to data-science folder and run

    python -m unittest discover -v -s streamlod/tests

after running the Blazegraph database
"""
import unittest
from os import sep
from pandas import DataFrame

from streamlod.handlers import MetadataUploadHandler, ProcessDataUploadHandler, MetadataQueryHandler, ProcessDataQueryHandler
from streamlod.mashups import AdvancedMashup
from streamlod.entities import Person, CulturalHeritageObject, Activity, Acquisition

class TestIncompleteData(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        m1 = 'streamlod' + sep + 'data' + sep + 'incomplete' + sep + 'meta1.csv'
        m1bis = 'streamlod' + sep + 'data' + sep + 'incomplete' + sep + 'meta1bis.csv'
        m2 = 'streamlod' + sep + 'data' + sep + 'incomplete' + sep + 'meta2.csv'
        p1 = 'streamlod' + sep + 'data' + sep + 'incomplete' + sep + 'process1.json'
        p1bis = 'streamlod' + sep + 'data' + sep + 'incomplete' + sep + 'process1bis.json'
        p2 = 'streamlod' + sep + 'data' + sep + 'incomplete' + sep + 'process2.json'
        gdb1 = 'http://127.0.0.1:9999/blazegraph/sparql'
        gdb2 = 'http://127.0.0.1:19999/blazegraph/sparql'
        rdb1 = 'streamlod' + sep + 'databases' + sep + 'relational1.db'
        rdb2 = 'streamlod' + sep + 'databases' + sep + 'relational2.db'

        muh = MetadataUploadHandler()
        muh.setDbPathOrUrl(gdb1, reset=True)
        muh.pushDataToDb(m1)
        muh.pushDataToDb(m1bis)
        muh.setDbPathOrUrl(gdb2, reset=True)
        muh.pushDataToDb(m2)

        puh = ProcessDataUploadHandler()
        puh.setDbPathOrUrl(rdb1, reset=True)
        puh.pushDataToDb(p1)
        puh.pushDataToDb(p1bis)
        puh.setDbPathOrUrl(rdb2, reset=True)
        puh.pushDataToDb(p2)

        mqh1 = MetadataQueryHandler()
        mqh1.setDbPathOrUrl(gdb1)
        mqh2 = MetadataQueryHandler()
        mqh2.setDbPathOrUrl(gdb2)
        pqh1 = ProcessDataQueryHandler()
        pqh1.setDbPathOrUrl(rdb1)
        pqh2 = ProcessDataQueryHandler()
        pqh2.setDbPathOrUrl(rdb2)

        cls.m = AdvancedMashup()
        cls.m.addMetadataHandler(mqh1)
        cls.m.addMetadataHandler(mqh2)
        cls.m.addProcessHandler(pqh1)
        cls.m.addProcessHandler(pqh2)
    
    def test_01_metadata_completeness(self):
        objects = self.m.getAllCulturalHeritageObjects()
        required = ['identifier', 'title', 'owner', 'place']
        for obj in objects:
            for name in required:
                attr = getattr(obj, name)
                self.assertIsInstance(attr, str)
            hasAuthor = obj.hasAuthor
            self.assertIsInstance(hasAuthor, list)
            for author in hasAuthor:
                self.assertIsInstance(author, Person)
            date = obj.date
            self.assertTrue(isinstance(date, str) or date is None)

        people = self.m.getAllPeople()
        for person in people:
            self.assertIsInstance(person.identifier, str)
            self.assertIsInstance(person.name, str)         

    def test_02_metadata_integration(self):
        objs = {}
        for i in range(6):
            objs[i] = self.m.getEntityById(f'{i}')

        # Three authors added in different dbs
        self.assertEqual(objs[1].hasAuthor, [Person('VIAF:30342047', 'Ardhanarishvara'), Person('VIAF:37015518', 'Eugenides, Jeffrey'), Person('VIAF:108159964', 'Plato')])

        # Author name inferred from other records in the db
        self.assertEqual(objs[2].title, 'Desert horned viper and vipera ammodytes')
        self.assertEqual(objs[2].hasAuthor, [Person('VIAF:100190422', 'Aldrovandi, Ulisse')])

        # Required and non required missing attributes pieced together
        self.assertEqual(objs[3].date, '1916')
        self.assertEqual(objs[3].owner, 'Biblioteca Universitaria di Sassari')
        self.assertEqual(objs[3].hasAuthor, [Person('VIAF:7155985', 'Blacc, Aloe'), Person('VIAF:41843502', 'Mansfield, Katherine')])

        # Author name missing and not present in other records in the db
        self.assertIsNone(objs[4])

        # If class or identifier is missing/incorrect the record is not added to the db
        self.assertIsNone(objs[5].date)

    def test_03_get_author_correctness(self):
        authors = self.m.getAllPeople()
        for author in authors:
            authoredby = self.m.getCulturalHeritageObjectsAuthoredBy(author.identifier)
            for obj in authoredby:
                self.assertIn(author, obj.hasAuthor)

    def test_04_equivalence_of_activity_refersTo_and_objects(self):
        objects = self.m.getAllCulturalHeritageObjects()
        for activity in self.m.getAllActivities():
            self.assertIn(activity.refersTo, objects)

if __name__ == '__main__':
    unittest.main()