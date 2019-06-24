from __future__ import absolute_import
from __future__ import print_function

from apache_beam.examples.snippets import common


def table_specs(project, dataset, table):
  # [START table_spec_with_project]
  # project:dataset.table
  table_spec = 'clouddataflow-readonly:samples.weather_stations'
  # [END table_spec_with_project]
  yield '{}:{}.{}'.format(project, dataset, table)

  # [START table_spec]
  # dataset.table
  table_spec = 'samples.weather_stations'
  # [END table_spec]
  yield '{}.{}'.format(dataset, table)


def read_table(project, table_spec, test=None):
  import apache_beam as beam
  from apache_beam.options.pipeline_options import PipelineOptions

  # project = 'your-gcp-project-id'
  # table_spec = 'project:dataset.table'
  # table_spec = 'dataset.table'

  beam_options = PipelineOptions(project=project)
  with beam.Pipeline(options=beam_options) as pipeline:
    rows = (
        pipeline
        | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(table_spec))
        | 'Print' >> beam.Map(print)
    )

    if test:
      test(rows)


def read_query(project, test=None):
  # [START read_query]
  import apache_beam as beam
  from apache_beam.options.pipeline_options import PipelineOptions

  # project = 'your-gcp-project-id'
  query = '''
    SELECT phase_emoji, peak_datetime, phase
    FROM `bigquery-public-data.moon_phases.moon_phases`
    WHERE peak_datetime > '2019-03-01'
    ORDER BY peak_datetime
    LIMIT 5
  '''

  beam_options = PipelineOptions(project=project)
  with beam.Pipeline(options=beam_options) as pipeline:
    rows = (
        pipeline
        | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(
            query=query,
            use_standard_sql=True,
          ))
        | 'Print' >> beam.Map(print)
    )
    # [END read_query]
    if test:
      test(rows)


if __name__ == '__main__':
  eval(common.parse_example())
