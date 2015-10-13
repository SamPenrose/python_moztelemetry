import os, unittest
import moztelemetry as MT


class Test_coalesce_by_date(unittest.TestCase):

    def test_all(self):
        data_path = os.path.dirname(__file__) + '/search_mocks.pretty'
        mocks = eval(open(data_path).read())
        data_days = MT.coalesce_by_date(mocks)
        counts = data_days['2015-10-05']['org.mozilla.searches.counts']
        self.assertEqual(counts['provider1'], 6)
        self.failIf('provider-idle-daily' in counts)

        # XXX add data values w/ missing keys


if __name__ == '__main__':
    unittest.main()
