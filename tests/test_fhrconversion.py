import os, unittest
import moztelemetry as MT


class Test_coalesce_by_date(unittest.TestCase):

    def test_all(self):
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


if __name__ == '__main__':
    unittest.main()
