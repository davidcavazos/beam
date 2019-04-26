from __future__ import absolute_import

import logging
import unittest
from unittest import mock

from apache_beam.examples.snippets.transforms.io.gcp import pubsubio
from apache_beam.io.gcp.pubsub_test import TestPubSubReadEvaluator
from apache_beam.runners.direct import transform_evaluator
from apache_beam.runners.direct.direct_runner import _DirectReadFromPubSub
from apache_beam.testing import test_utils
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

try:
  from google.cloud import pubsub
except ImportError:
  pubsub = None

# Patch _PubSubReadEvaluator to make it bounded.
transform_evaluator.TransformEvaluatorRegistry._test_evaluators_overrides = {
    _DirectReadFromPubSub: TestPubSubReadEvaluator,
}


@mock.patch('apache_beam.Pipeline', TestPipeline)
@mock.patch(
    'apache_beam.examples.snippets.transforms.io.gcp.pubsubio.print', str)
@mock.patch('apache_beam.io.ReadFromPubSub', lambda _: (
    TestStream()
    .advance_watermark_to(0)
))
class PubSubIOTest(unittest.TestCase):
  def test_read_from_pubsub(self, mock_pubsub):
    # [START read_from_pubsub_outputs]
    outputs = [
        b'Hello',                           # type: bytes
        u'🤷 ¯\\_(ツ)_/¯'.encode('utf-8'),  # type: bytes
    ]
    # [END read_from_pubsub_outputs]
    mock_pubsub.return_value.pull.return_value = test_utils.create_pull_response([
        test_utils.PullResponseMessage(output, ack_id='ack_{}'.format(i))
        for i, output in enumerate(outputs)
    ])

    def test(messages):
      assert_that(messages, equal_to(outputs))
    pubsubio.read_from_pubsub('projects/fakeprj/topics/a_topic', test)


if __name__ == '__main__':
  logging.basicConfig(level=logging.INFO)
  unittest.main()
