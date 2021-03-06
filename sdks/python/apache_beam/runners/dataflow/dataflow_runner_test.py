#
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
#

"""Unit tests for the DataflowRunner class."""

import json
import unittest
from datetime import datetime

import mock

import apache_beam as beam
import apache_beam.transforms as ptransform

from apache_beam.pipeline import Pipeline
from apache_beam.runners import create_runner
from apache_beam.runners import DataflowRunner
from apache_beam.runners import TestDataflowRunner
from apache_beam.runners.dataflow.dataflow_runner import DataflowPipelineResult
from apache_beam.runners.dataflow.dataflow_runner import DataflowRuntimeException
from apache_beam.runners.dataflow.internal.clients import dataflow as dataflow_api
from apache_beam.transforms.display import DisplayDataItem
from apache_beam.utils.pipeline_options import PipelineOptions

# Protect against environments where apitools library is not available.
# pylint: disable=wrong-import-order, wrong-import-position
try:
  from apache_beam.runners.dataflow.internal import apiclient
except ImportError:
  apiclient = None
# pylint: enable=wrong-import-order, wrong-import-position


@unittest.skipIf(apiclient is None, 'GCP dependencies are not installed')
class DataflowRunnerTest(unittest.TestCase):
  default_properties = [
      '--dataflow_endpoint=ignored',
      '--job_name=test-job',
      '--project=test-project',
      '--staging_location=ignored',
      '--temp_location=/dev/null',
      '--no_auth=True']

  @mock.patch('time.sleep', return_value=None)
  def test_wait_until_finish(self, patched_time_sleep):
    values_enum = dataflow_api.Job.CurrentStateValueValuesEnum

    class MockDataflowRunner(object):

      def __init__(self, final_state):
        self.dataflow_client = mock.MagicMock()
        self.job = mock.MagicMock()
        self.job.currentState = values_enum.JOB_STATE_UNKNOWN

        def get_job_side_effect(*args, **kwargs):
          self.job.currentState = final_state
          return mock.DEFAULT

        self.dataflow_client.get_job = mock.MagicMock(
            return_value=self.job, side_effect=get_job_side_effect)
        self.dataflow_client.list_messages = mock.MagicMock(
            return_value=([], None))

    with self.assertRaisesRegexp(
        DataflowRuntimeException, 'Dataflow pipeline failed. State: FAILED'):
      failed_runner = MockDataflowRunner(values_enum.JOB_STATE_FAILED)
      failed_result = DataflowPipelineResult(failed_runner.job, failed_runner)
      failed_result.wait_until_finish()

    succeeded_runner = MockDataflowRunner(values_enum.JOB_STATE_DONE)
    succeeded_result = DataflowPipelineResult(
        succeeded_runner.job, succeeded_runner)
    succeeded_result.wait_until_finish()

  def test_create_runner(self):
    self.assertTrue(
        isinstance(create_runner('DataflowRunner'),
                   DataflowRunner))
    self.assertTrue(
        isinstance(create_runner('TestDataflowRunner'),
                   TestDataflowRunner))

  def test_remote_runner_translation(self):
    remote_runner = DataflowRunner()
    p = Pipeline(remote_runner,
                 options=PipelineOptions(self.default_properties))

    (p | ptransform.Create([1, 2, 3])  # pylint: disable=expression-not-assigned
     | 'Do' >> ptransform.FlatMap(lambda x: [(x, x)])
     | ptransform.GroupByKey())
    remote_runner.job = apiclient.Job(p.options)
    super(DataflowRunner, remote_runner).run(p)

  def test_remote_runner_display_data(self):
    remote_runner = DataflowRunner()
    p = Pipeline(remote_runner,
                 options=PipelineOptions(self.default_properties))

    # TODO: Should not subclass ParDo. Switch to PTransform as soon as
    # composite transforms support display data.
    class SpecialParDo(beam.ParDo):
      def __init__(self, fn, now):
        super(SpecialParDo, self).__init__(fn)
        self.fn = fn
        self.now = now

      # Make this a list to be accessible within closure
      def display_data(self):
        return {'asubcomponent': self.fn,
                'a_class': SpecialParDo,
                'a_time': self.now}

    class SpecialDoFn(beam.DoFn):
      def display_data(self):
        return {'dofn_value': 42}

      def process(self):
        pass

    now = datetime.now()
    # pylint: disable=expression-not-assigned
    (p | ptransform.Create([1, 2, 3, 4, 5])
     | 'Do' >> SpecialParDo(SpecialDoFn(), now))

    remote_runner.job = apiclient.Job(p.options)
    super(DataflowRunner, remote_runner).run(p)
    job_dict = json.loads(str(remote_runner.job))
    steps = [step
             for step in job_dict['steps']
             if len(step['properties'].get('display_data', [])) > 0]
    step = steps[0]
    disp_data = step['properties']['display_data']
    disp_data = sorted(disp_data, key=lambda x: x['namespace']+x['key'])
    nspace = SpecialParDo.__module__+ '.'
    expected_data = [{'type': 'TIMESTAMP', 'namespace': nspace+'SpecialParDo',
                      'value': DisplayDataItem._format_value(now, 'TIMESTAMP'),
                      'key': 'a_time'},
                     {'type': 'STRING', 'namespace': nspace+'SpecialParDo',
                      'value': nspace+'SpecialParDo', 'key': 'a_class',
                      'shortValue': 'SpecialParDo'},
                     {'type': 'INTEGER', 'namespace': nspace+'SpecialDoFn',
                      'value': 42, 'key': 'dofn_value'}]
    expected_data = sorted(expected_data, key=lambda x: x['namespace']+x['key'])
    self.assertEqual(len(disp_data), 3)
    self.assertEqual(disp_data, expected_data)

  def test_no_group_by_key_directly_after_bigquery(self):
    remote_runner = DataflowRunner()
    p = Pipeline(remote_runner,
                 options=PipelineOptions([
                     '--dataflow_endpoint=ignored',
                     '--job_name=test-job',
                     '--project=test-project',
                     '--staging_location=ignored',
                     '--temp_location=/dev/null',
                     '--no_auth=True'
                 ]))
    rows = p | beam.io.Read(beam.io.BigQuerySource('dataset.faketable'))
    with self.assertRaises(ValueError,
                           msg=('Coder for the GroupByKey operation'
                                '"GroupByKey" is not a key-value coder: '
                                'RowAsDictJsonCoder')):
      unused_invalid = rows | beam.GroupByKey()


if __name__ == '__main__':
  unittest.main()
