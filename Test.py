import unittest
import main
from sqlalchemy.engine import URL
from sqlalchemy import MetaData


class TestLoadData(unittest.TestCase):

    def setUp(self):
        print('setUp')
        self.hausarbeit = MetaData()
        self.url_object = URL.create('mysql+pymysql', username='root',
                                     password='Tesnimoez90Python',
                                     host='localhost', database='hausarbeit')

    def test_Load(self):
        print('test_Load')
        result = main.LoadData(self.hausarbeit, self.url_object)
        result.load('Training')
        self.assertIsNotNone(result, 'Table is found in Mysql Database')
        result.load('Ideal')
        self.assertIsNotNone(result, 'Table is found in Mysql Database')


if __name__ == '__main__':
    unittest.main()
