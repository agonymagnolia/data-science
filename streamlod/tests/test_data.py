"""
To run the tests navigate to data-science folder and run

    python -m unittest discover -v -s streamlod/tests

after running the Blazegraph database
"""
import unittest
from os import sep
from pandas import DataFrame

from streamlod.handlers import MetadataUploadHandler, ProcessDataUploadHandler, MetadataQueryHandler, ProcessDataQueryHandler
from streamlod.mashups import AdvancedMashup
from streamlod.entities import Person, CulturalHeritageObject, Activity, Acquisition, Optimising, Modelling

class Test_01_IncompleteData(unittest.TestCase):

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

    def test_03_author_correctness(self):
        authors = self.m.getAllPeople()
        for author in authors:
            authoredby = self.m.getCulturalHeritageObjectsAuthoredBy(author.identifier)
            for obj in authoredby:
                self.assertIn(author, obj.hasAuthor)

    def test_04_process_completeness(self):
        activities = self.m.getAllActivities()
        for activity in activities:
            self.assertIsInstance(activity.institute, str)
            self.assertIsInstance(activity.object, CulturalHeritageObject)
            self.assertTrue(isinstance(activity.person, str) or activity.person is None)
            self.assertTrue(isinstance(activity.start, str) or activity.start is None)
            self.assertTrue(isinstance(activity.end, str) or activity.end is None)
            tools = activity.tool
            self.assertIsInstance(tools, set)
            for tool in tools:
                self.assertIsInstance(tool, str)
            if isinstance(activity, Acquisition):
                self.assertIsInstance(activity.technique, str)

    def test_05_process_integration(self):
        dfs1 = []
        for pqh in self.m.processQuery:
            dfs1.append(pqh.getById('1'))
        p1 = self.m.toActivity(dfs1)

        dfs2 = []
        for pqh in self.m.processQuery:
            dfs2.append(pqh.getById('2'))
        p2 = self.m.toActivity(dfs2)

        dfs3 = []
        for pqh in self.m.processQuery:
            dfs3.append(pqh.getById('3'))
        p3 = self.m.toActivity(dfs3)

        # Technique from db2 because in db1 activity was discarded
        self.assertEqual(p1[0].technique, 'Photogrammetry')

        # Subsequent pushes on same db
        #self.assertIsNone(p1[1].person)

        # Integration of data from different dbs only for whole activity, not single values
        #self.assertIsNone(p1[1].end)
        #self.assertEqual(p1[1].tool, set())

        # Institute from second push on db1 because in first push the activity was discarded
        for p in p1:
            if isinstance(p, Modelling):
                self.assertEqual(p.institute, 'Philology')

        # Integration of new type of activity on same object from different db
        self.assertTrue(any(isinstance(activity, Optimising) for activity in p2))

        # Not compliant institute datatype
        #self.assertFalse(any(isinstance(activity, Acquisition) for activity in p3))

    def test_06_refersTo_correctness(self):
        objects = self.m.getAllCulturalHeritageObjects()
        for activity in self.m.getAllActivities():
            self.assertIn(activity.object, objects)

    def test_07_process_order(self):
        activities = self.m.getAllActivities()
        sorted_activities = sorted(activities)
        self.assertEqual(activities, sorted_activities)

class Test_02_DuplicateData(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        m1 = 'streamlod' + sep + 'data' + sep + 'duplicate' + sep + 'meta1.csv'
        m1bis = 'streamlod' + sep + 'data' + sep + 'duplicate' + sep + 'meta1bis.csv'
        m2 = 'streamlod' + sep + 'data' + sep + 'duplicate' + sep + 'meta2.csv'
        p1 = 'streamlod' + sep + 'data' + sep + 'duplicate' + sep + 'process1.json'
        p1bis = 'streamlod' + sep + 'data' + sep + 'duplicate' + sep + 'process1bis.json'
        p2 = 'streamlod' + sep + 'data' + sep + 'duplicate' + sep + 'process2.json'
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

    def test_01_duplicatealot(self):
        dfs1 = []
        for pqh in self.m.processQuery:
            dfs1.append(pqh.getById('1'))
        p1 = self.m.toActivity(dfs1)

        for p in p1:
            self.assertEqual(p1[0], p)
        
        self.assertEqual(len(p1), 60)


if __name__ == '__main__':
    unittest.main()