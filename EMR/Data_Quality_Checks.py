import os
import sys
import unittest
import configparser
import psycopg2
import psycopg2.extras

config = configparser.ConfigParser()
config.read('config.ini')

from sql_queries import prod_tables

def run_tests(test_class):
    suite = unittest.TestLoader().loadTestsFromTestCase(test_class)
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)
    
class TestQualityCheck(unittest.TestCase):
    curr = None
    
    @classmethod
    def setUpClass(cls):
        # Set up db connections
        host = config.get('postgres','host')
        username = config.get('postgres','user')
        password = config.get('postgres','password')
        database = config.get('postgres','database')      
        db = psycopg2.connect(host=host, user=username, password=password, database=database)
        cls.curr = db.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
    @classmethod
    def tearDownClass(cls):
        if (cls.curr):
            (cls.curr).close()
        
    def test_connection(self):
        self.assertNotEqual(self.curr, None)
        
    def test_distinct_records(self):
        """
        The following test ensures consistency, 
        making sure duplicate records doesn't exist
        """
        for table in prod_tables:
            self.curr.execute("SELECT COUNT(*) FROM {}".format(table))
            record_count = (self.curr).fetchone()
            
            self.curr.execute("SELECT COUNT(*) FROM (SELECT DISTINCT * FROM {}) AS temp".format(table))
            distinct_record_count = (self.curr).fetchone()
            
            print("{}: Record Count [{}], Distinct Count [{}]".format(table, record_count, distinct_record_count))
            self.assertEqual(record_count, distinct_record_count)
            
    def test_date_formatting(self):
        """
        This following test ensures that the date value
        is of the following format YYYY-MM-DD. 
        Makes parsing out the year consistent.
        """
        
        self.curr.execute("SELECT count(date) FROM prod_police_shootings")
        control_count = (self.curr).fetchone()        
        
        self.curr.execute("SELECT count(date) FROM prod_police_shootings WHERE date ~ '^\d\d\d\d-\d\d-\d\d$'")
        record_count = (self.curr).fetchone()
        
        print("Date in  this format YYYY-MM-DD: Control Count {}, Record Count {}" \
              .format(control_count, record_count))
        self.assertEqual(control_count, record_count)            
    