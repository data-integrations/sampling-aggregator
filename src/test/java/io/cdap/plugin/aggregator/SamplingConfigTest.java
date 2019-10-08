/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.aggregator;

import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Tests for {@link SamplingConfig}.
 */
public class SamplingConfigTest {
  private static final String MOCK_STAGE = "mockStage";
  private static final SamplingConfig VALID_CONFIG = new SamplingConfig(
    1,
    null,
    null,
    0.5f,
    SamplingConfig.TYPE.SYSTEMATIC.toString(),
    1
  );

  @Test
  public void testValidConfig() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    VALID_CONFIG.validate(failureCollector);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
  }

  @Test
  public void testInvalidSampleSizeAndSamplePercentage() {
    SamplingConfig config = SamplingConfig.builder(VALID_CONFIG)
      .setSampleSize(null)
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, 2, SamplingConfig.SAMPLE_SIZE);
    assertValidationFailed(failureCollector, 2, SamplingConfig.SAMPLE_PERCENTAGE);
  }

  @Test
  public void testInvalidSamplingTypeAndTotalRecords() {
    SamplingConfig config = SamplingConfig.builder(VALID_CONFIG)
      .setTotalRecords(null)
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, 2, SamplingConfig.SAMPLING_TYPE);
    assertValidationFailed(failureCollector, 2, SamplingConfig.TOTAL_RECORDS);
  }

  @Test
  public void testInvalidSamplePercentage() {
    SamplingConfig config = SamplingConfig.builder(VALID_CONFIG)
      .setSamplePercentage(1000F)
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, SamplingConfig.SAMPLE_PERCENTAGE);
  }

  @Test
  public void testInvalidRandom() {
    SamplingConfig config = SamplingConfig.builder(VALID_CONFIG)
      .setRandom(1000F)
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, SamplingConfig.RANDOM);
  }

  private static void assertValidationFailed(MockFailureCollector failureCollector, String paramName) {
    assertValidationFailed(failureCollector, 1, paramName);
  }

  private static void assertValidationFailed(MockFailureCollector failureCollector, int causeNumber, String paramName) {
    List<ValidationFailure> failureList = failureCollector.getValidationFailures();
    Assert.assertEquals(1, failureList.size());
    ValidationFailure failure = failureList.get(0);
    List<ValidationFailure.Cause> causeList = failure.getCauses()
      .stream()
      .filter(cause -> cause.getAttribute(CauseAttributes.STAGE_CONFIG) != null)
      .collect(Collectors.toList());
    Assert.assertEquals(causeNumber, causeList.size());
    List<String> causeAttributes = causeList.stream()
      .map(cause -> cause.getAttribute(CauseAttributes.STAGE_CONFIG))
      .collect(Collectors.toList());
    Assert.assertTrue(paramName, causeAttributes.contains(paramName));
  }
}
