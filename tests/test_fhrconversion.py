import unittest

# from moztelemetry import coalesce_by_date, search_extractor
import moztelemetry as MT

DATA = [
  {
    'clientId': 'CLIENT_ID',
    'creationDate': '2015-10-05T15:55:41.224Z',
    'payload': {
      'keyedHistograms': {
        'SEARCH_COUNTS': {
          'provider1': {
            'sum': 6
          }
        }
      },
      'info': {
        'reason': 'just because'
      }
    }
   }
]


class Test_coalesce_by_date(unittest.TestCase):

    def test_all(self):
        data_days = MT.coalesce_by_date(DATA)
        value = data_days['2015-10-05']['org.mozilla.searches.counts'][
            'provider1']
        self.assertEqual(value, 6)


if __name__ == '__main__':
    unittest.main()
