from __future__ import absolute_import
from __future__ import print_function


def read_from_pubsub(topic_path, test=None):
  # [START read_from_pubsub]
  import apache_beam as beam
  from apache_beam.options.pipeline_options import PipelineOptions

  # project = 'google-cloud-project-id'
  # topic = 'pubsub-topic'
  # topic_path = 'projects/{}/topics/{}'.format(project, topic)

  beam_options = PipelineOptions(streaming=True)
  with beam.Pipeline(options=beam_options) as pipeline:
    messages = (
        pipeline
        | 'Read from Pub/Sub' >> beam.io.ReadFromPubSub(topic_path)
        | 'Print' >> beam.Map(print)
    )
    # [END read_from_pubsub]
    if test:
      test(messages)


if __name__ == '__main__':
  import argparse

  parser = argparse.ArgumentParser()
  parser.add_argument('example', help='Name of the example to run.')
  parser.add_argument('args', nargs=argparse.REMAINDER, help='Arguments for example.')
  args = parser.parse_args()

  # Call the example as a function.
  eval('{}({})'.format(args.example, ', '.join([repr(arg) for arg in args.args])))
