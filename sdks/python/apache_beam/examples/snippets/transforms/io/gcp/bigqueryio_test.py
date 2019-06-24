from __future__ import absolute_import
from __future__ import print_function

import logging
import unittest
from unittest import mock

from apache_beam.examples.snippets import common
from apache_beam.examples.snippets.transforms.io.gcp import bigqueryio
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

try:
  from google.cloud import bigquery
except ImportError:
  bigquery = None

try:
  from apache_beam.examples.snippets.transforms.io.gcp import env
except EnvironmentError as error:
  env = error


@unittest.skipIf(bigquery is None, 'GCP dependencies are not installed')
@unittest.skipIf(isinstance(env, Exception), env)
@mock.patch('apache_beam.Pipeline', TestPipeline)
@mock.patch('apache_beam.examples.snippets.transforms.io.gcp.bigqueryio.print', lambda elem: elem)
class BigQueryIOTest(unittest.TestCase):
  @classmethod
  def setUpClass(cls):
    # [START setup_vars]
    project = "" #@param {type:"string"}
    dataset = "beam_samples" #@param {type:"string"}
    table = "nasa_wildfire" #@param {type:"string"}
    # [END setup_vars]

    # Use more unique names for testing resources.
    project = env.project
    dataset = env.bigquery_dataset

    # Create the data file that will be loaded to BigQuery.
    #   https://console.cloud.google.com/bigquery?j=bq:US:bquxjob_692afb04_16a56c7694d&page=queryresults
    #   https://console.cloud.google.com/bigquery?p=bigquery-public-data&d=nasa_wildfire&t=past_week&page=table

    # [START create_data_file]
    import json
    import tempfile

    wildfires = [
      {'latitude': 23.77983, 'longitude': 94.51313, 'confidence': 'low', 'frp': 3.4, 'timestamp': 1555657920},
      {'latitude': 7.17482, 'longitude': -9.54518, 'confidence': 'nominal', 'frp': 32.0, 'timestamp': 1556029080},
      {'latitude': 8.58114, 'longitude': 29.2184, 'confidence': 'high', 'frp': 8.3, 'timestamp': 1555761240},
      {'latitude': 22.75435, 'longitude': 94.18239, 'confidence': 'low', 'frp': 1.1, 'timestamp': 1555743240},
      {'latitude': 19.91492, 'longitude': 101.06348, 'confidence': 'nominal', 'frp': 15.0, 'timestamp': 1555566480},
      {'latitude': 10.78018, 'longitude': -12.55964, 'confidence': 'high', 'frp': 206.4, 'timestamp': 1556029080},
    ]

    f = tempfile.NamedTemporaryFile()
    for fire in wildfires:
      f.write((json.dumps(fire) + '\n').encode('utf-8'))
    f.flush()

    data_file = f.name
    print(data_file)
    # [END create_data_file]

    # Create the testing dataset and table.
    commands = '''
      # [START setup]
      # Create a BigQuery dataset.
      !bq mk --dataset {project}:{dataset}

      # Create a BigQuery table.
      !bq mk --table {project}:{dataset}.{table}

      # Load data from a JSON file.
      !bq load --autodetect --source_format=NEWLINE_DELIMITED_JSON {project}:{dataset}.{table} {data_file}
      # [END setup]
    '''.splitlines()

    common.run_shell_commands(
      commands,
      project=project,
      dataset=dataset,
      table=table,
      data_file=data_file,
    )

    # Close and cleanup the temporary file.
    f.close()

    cls.project = project
    cls.dataset = dataset
    cls.table = table

    return super().setUpClass()

  @classmethod
  def tearDownClass(cls):
    # Delete the dataset and table.
    commands = '''
      # [START cleanup]
      # Remove the BigQuery table.
      !bq rm -f {project}:{dataset}.{table}

      # Remove the BigQuery dataset.
      !bq rm -f {project}:{dataset}
      # [END cleanup]
    '''.splitlines()

    common.run_shell_commands(
      commands,
      project=cls.project,
      dataset=cls.dataset,
      table=cls.table,
    )

    return super().tearDownClass()

  def test_read_table(self):
    # [START read_table_outputs]
    outputs = [
      {'latitude': 23.77983, 'longitude': 94.51313, 'confidence': 'low', 'frp': 3.4, 'timestamp': 1555657920},
      {'latitude': 7.17482, 'longitude': -9.54518, 'confidence': 'nominal', 'frp': 32.0, 'timestamp': 1556029080},
      {'latitude': 8.58114, 'longitude': 29.2184, 'confidence': 'high', 'frp': 8.3, 'timestamp': 1555761240},
      {'latitude': 22.75435, 'longitude': 94.18239, 'confidence': 'low', 'frp': 1.1, 'timestamp': 1555743240},
      {'latitude': 19.91492, 'longitude': 101.06348, 'confidence': 'nominal', 'frp': 15.0, 'timestamp': 1555566480},
      {'latitude': 10.78018, 'longitude': -12.55964, 'confidence': 'high', 'frp': 206.4, 'timestamp': 1556029080},
    ]
    # [END read_table_outputs]

    def test(rows):
      assert_that(rows, equal_to(outputs))

    for table_spec in bigqueryio.table_specs(self.project, self.dataset, self.table):
      bigqueryio.read_table(self.project, table_spec, test)

  def test_read_query(self):
    # [START read_query_outputs]
    outputs = [
      {'phase_emoji': '🌑', 'peak_datetime': '2019-03-06T16:04:00', 'phase': 'New Moon'},
      {'phase_emoji': '🌓', 'peak_datetime': '2019-03-14T10:27:00', 'phase': 'First Quarter'},
      {'phase_emoji': '🌕', 'peak_datetime': '2019-03-21T01:43:00', 'phase': 'Full Moon'},
      {'phase_emoji': '🌗', 'peak_datetime': '2019-03-28T04:10:00', 'phase': 'Last Quarter'},
      {'phase_emoji': '🌑', 'peak_datetime': '2019-04-05T08:50:00', 'phase': 'New Moon'},
    ]
    # [END read_query_outputs]

    def test(rows):
      assert_that(rows, equal_to(outputs))
    bigqueryio.read_query(self.project, test)


if __name__ == '__main__':
  logging.basicConfig(level=logging.INFO)
  unittest.main()
