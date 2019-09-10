# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import pytest
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite


class TestStatestoreRpcErrors(CustomClusterTestSuite):
  """Tests for statestore RPC handling."""

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def setup_class(cls):
    if cls.exploration_strategy() != 'exhaustive':
      pytest.skip('runs only in exhaustive')
    super(TestStatestoreRpcErrors, cls).setup_class()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      " --debug_actions=REGISTER_SUBSCRIBER_FIRST_ATTEMPT:FAIL@1.0")
  def test_register_subscriber_rpc_error(self, vector):
    self.assert_impalad_log_contains("INFO",
        "Injected RPC error.*Debug Action: REGISTER_SUBSCRIBER_FIRST_ATTEMPT")

    # Ensure cluster has started up by running a query.
    result = self.execute_query("select count(*) from functional_parquet.alltypes")
    assert result.success, str(result)
