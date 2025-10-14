/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.flink.maintenance.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import org.apache.curator.test.TestingServer;
import org.apache.flink.shaded.curator5.org.apache.curator.RetryPolicy;
import org.apache.flink.shaded.curator5.org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.flink.shaded.curator5.org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.flink.shaded.curator5.org.apache.curator.retry.RetryNTimes;
import org.apache.flink.shaded.curator5.org.apache.curator.retry.RetryOneTime;
import org.apache.flink.shaded.curator5.org.apache.curator.retry.RetryUntilElapsed;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestZkLockFactory extends TestLockFactoryBase {

  private TestingServer zkTestServer;

  @Override
  TriggerLockFactory lockFactory(String tableName) {
    return new ZkLockFactory(
        zkTestServer.getConnectString(),
        tableName,
        5000,
        3000,
        1000,
        3,
        2000,
        ZKRetryPolicies.EXPONENTIAL_BACKOFF.name());
  }

  @BeforeEach
  @Override
  void before() {
    try {
      zkTestServer = new TestingServer();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    super.before();
  }

  @AfterEach
  public void after() throws IOException {
    super.after();
    if (zkTestServer != null) {
      zkTestServer.close();
    }
  }

  @Test
  void testAllRetryPoliciesCreationAndType() {
    for (ZKRetryPolicies policy : ZKRetryPolicies.values()) {
      ZkLockFactory factory =
          new ZkLockFactory("localhost:2181", "test", 3000, 3000, 1000, 3, 2000, policy.name());

      RetryPolicy retryPolicy = factory.createRetryPolicy();

      assertThat(retryPolicy)
          .as("RetryPolicy should not be null for policy %s", policy)
          .isNotNull();

      switch (policy) {
        case ONE_TIME:
          assertThat(retryPolicy)
              .as("Expected RetryOneTime for policy %s", policy)
              .isInstanceOf(RetryOneTime.class);
          break;

        case N_TIME:
          assertThat(retryPolicy)
              .as("Expected RetryNTimes for policy %s", policy)
              .isInstanceOf(RetryNTimes.class);
          break;

        case BOUNDED_EXPONENTIAL_BACKOFF:
          assertThat(retryPolicy)
              .as("Expected BoundedExponentialBackoffRetry for policy %s", policy)
              .isInstanceOf(BoundedExponentialBackoffRetry.class);
          break;

        case UNTIL_ELAPSED:
          assertThat(retryPolicy)
              .as("Expected RetryUntilElapsed for policy %s", policy)
              .isInstanceOf(RetryUntilElapsed.class);
          break;

        case EXPONENTIAL_BACKOFF:
          assertThat(retryPolicy)
              .as("Expected ExponentialBackoffRetry for policy %s", policy)
              .isInstanceOf(ExponentialBackoffRetry.class);
          break;

        default:
          throw new IllegalStateException("Unhandled policy type: " + policy);
      }
    }
  }
}
