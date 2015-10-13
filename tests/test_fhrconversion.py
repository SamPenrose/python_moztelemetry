import os, unittest

# from moztelemetry import coalesce_by_date, search_extractor
import moztelemetry as MT


class Test_coalesce_by_date(unittest.TestCase):

    def test_all(self):
        data_path = os.path.dirname(__file__) + '/search_mocks.pretty'
        mocks = eval(open(data_path).read())
        data_days = MT.coalesce_by_date(mocks)
        value = data_days['2015-10-05']['org.mozilla.searches.counts'][
            'provider1']
        self.assertEqual(value, 6)


if __name__ == '__main__':
    unittest.main()
