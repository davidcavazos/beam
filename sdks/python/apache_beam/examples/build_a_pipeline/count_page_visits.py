#!/usr/bin/env python

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import absolute_import

import argparse


def run(input_topic, output_topic, beam_argv=None):
  # [START count_page_visits]
  import apache_beam as beam
  import datetime
  import json

  # input_topic = 'projects/<your-gcp-project>/topics/<your-pubsub-topic>'
  # output_topic = 'projects/<your-gcp-project>/topics/<your-pubsub-topic>'
  # beam_argv = ['--project', '<your-gcp-project>']

  window_size = datetime.timedelta(seconds=5).total_seconds()

  options = beam.options.pipeline_options.PipelineOptions(
      beam_argv,
      save_main_session=True,
      streaming=True,
  )

  print('Listening for messages on ' + input_topic)
  with beam.Pipeline(options=options) as pipeline:
    _ = (
        pipeline
        | 'Read website events' >> beam.io.ReadFromPubSub(input_topic)
        | 'Fixed-sized windows' >> beam.WindowInto(
            beam.window.FixedWindows(window_size))
        | 'Extract URLs' >> beam.Map(lambda message: json.loads(message)['url'])
        | 'Count URLs' >> beam.combiners.Count.PerElement()
        | 'Format to bytestring' >> beam.MapTuple(
            lambda url, count: '{},{}'.format(url, count).encode('utf-8'))
        | 'Print elements' >> beam.Map(lambda elem: print(elem) or elem)
        | 'Write page visits' >> beam.io.WriteToPubSub(output_topic)
    )
  # [END count_page_visits]


if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input-topic',
      required=True,
      help='Input Pub/Sub topic for website events',
  )
  parser.add_argument(
      '--output-topic',
      required=True,
      help='Output Pub/Sub topic for daily page visits',
  )
  args, beam_args = parser.parse_known_args()

  run(args.input_topic, args.output_topic, beam_args)
