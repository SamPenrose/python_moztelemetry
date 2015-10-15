import os, unittest
import moztelemetry as MT


class Test_coalesce_by_date(unittest.TestCase):

    def test_coalesce_by_date(self):
        data_path = os.path.dirname(__file__) + '/search_mocks.pretty'
        mocks = eval(open(data_path).read())
        data_days = MT.coalesce_by_date(mocks)
        counts = data_days['2015-10-05']['org.mozilla.searches.counts']
        self.assertEqual(counts['provider1'], 6)

        self.failIf('ping-idle-daily' in counts)
        self.failIf('ping-missing-creationDate' in counts)
        self.failIf('ping-short-creationDate' in counts)
        self.failIf('provider-no-sum' in counts)

        mocks[1]['clientId'] = 'bad client id'
        self.assertRaises(ValueError, MT.coalesce_by_date, mocks)

    def test_make_ES_filter(self):
        empty = []
        f = MT.make_ES_filter({'a': 1, 'b': [2, 3]}, empty)
        good = {'a': 1, 'b': 3}
        self.assertEqual(f(good), good)
        bad = {'a': 1, 'b': 4}
        self.assertEqual(f(bad), empty)
        bad2 = {'a': 1, 'b': [2, 3]}
        self.assertEqual(f(bad), empty)


        self.assertEqual(f({}), empty)
        good2 = {'a': 1, 'b': 3, 'c': object()}
        self.assertEqual(f(good2), good2)

if __name__ == '__main__':
    unittest.main()
