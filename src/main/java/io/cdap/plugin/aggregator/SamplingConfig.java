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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;

import java.util.Objects;
import java.util.Random;
import javax.annotation.Nullable;

/**
 * Configurations for {@link Sampling}.
 */
public class SamplingConfig extends PluginConfig {
  public static final String SAMPLE_SIZE = "sampleSize";
  public static final String SAMPLE_PERCENTAGE = "samplePercentage";
  public static final String SAMPLING_TYPE = "samplingType";
  public static final String OVER_SAMPLING_PERCENTAGE = "overSamplingPercentage";
  public static final String RANDOM = "random";
  public static final String TOTAL_RECORDS = "totalRecords";

  @Name(SAMPLE_SIZE)
  @Nullable
  @Description("The number of records that needs to be sampled from the input records. Either of " +
    "'samplePercentage' or 'sampleSize' should be specified for this plugin.")
  @Macro
  private Integer sampleSize;

  @Name(SAMPLE_PERCENTAGE)
  @Nullable
  @Description("The percenatage of records that needs to be sampled from the input records. Either of " +
    "'samplePercentage' or 'sampleSize' should be specified for this plugin.")
  @Macro
  private Float samplePercentage;

  @Name(SAMPLING_TYPE)
  @Description("Type of the Sampling algorithm that should to be used to sample the data. This can be either " +
    "Systematic or Reservoir.")
  private String samplingType;

  @Name(OVER_SAMPLING_PERCENTAGE)
  @Nullable
  @Description("The percentage of additional records that should be included in addition to the input " +
    "sample size to account for oversampling. Required for Systematic Sampling.")
  @Macro
  private Float overSamplingPercentage;

  @Name(RANDOM)
  @Nullable
  @Description("Random float value between 0 and 1 to be used in Systematic Sampling. If not provided, " +
    "plugin will internally generate random value.")
  @Macro
  private Float random;

  @Name(TOTAL_RECORDS)
  @Nullable
  @Description("Total number of input records for Systematic Sampling.")
  @Macro
  private Integer totalRecords;

  public SamplingConfig() {
    this.random = new Random().nextFloat();
  }

  public SamplingConfig(@Nullable Integer sampleSize, @Nullable Float samplePercentage,
                        @Nullable Float overSamplingPercentage, @Nullable Float random,
                        String samplingType, @Nullable Integer totalRecords) {
    this.sampleSize = sampleSize;
    this.samplePercentage = samplePercentage;
    this.overSamplingPercentage = overSamplingPercentage;
    this.random = random;
    this.samplingType = samplingType;
    this.totalRecords = totalRecords;
  }

  private SamplingConfig(Builder builder) {
    sampleSize = builder.sampleSize;
    samplePercentage = builder.samplePercentage;
    samplingType = builder.samplingType;
    overSamplingPercentage = builder.overSamplingPercentage;
    random = builder.random;
    totalRecords = builder.totalRecords;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(SamplingConfig copy) {
    Builder builder = new Builder();
    builder.setSampleSize(copy.getSampleSize());
    builder.setSamplePercentage(copy.getSamplePercentage());
    builder.setSamplingType(copy.getSamplingType());
    builder.setOverSamplingPercentage(copy.getOverSamplingPercentage());
    builder.setRandom(copy.getRandom());
    builder.setTotalRecords(copy.getTotalRecords());
    return builder;
  }

  @Nullable
  public Integer getSampleSize() {
    return sampleSize;
  }

  @Nullable
  public Float getSamplePercentage() {
    return samplePercentage;
  }

  public String getSamplingType() {
    return samplingType;
  }

  @Nullable
  public Float getOverSamplingPercentage() {
    return overSamplingPercentage;
  }

  @Nullable
  public Float getRandom() {
    return random;
  }

  @Nullable
  public Integer getTotalRecords() {
    return totalRecords;
  }

  public void validate(FailureCollector collector) {
    if (!containsMacro(SAMPLE_SIZE) && !containsMacro(SAMPLE_PERCENTAGE) && sampleSize == null &&
      samplePercentage == null) {
      collector.addFailure("Sample size or Sample Percentage must be specified.", null)
        .withConfigProperty(SAMPLE_SIZE)
        .withConfigProperty(SAMPLE_PERCENTAGE);
    }

    if (samplingType.equalsIgnoreCase(TYPE.SYSTEMATIC.toString()) && !containsMacro(TOTAL_RECORDS) &&
      totalRecords == null) {
      collector.addFailure("Total Records must be specified when selecting sampling type as 'Systematic'.", null)
        .withConfigProperty(SAMPLING_TYPE)
        .withConfigProperty(TOTAL_RECORDS);
    }

    if (!containsMacro(SAMPLE_PERCENTAGE) && Objects.nonNull(samplePercentage) &&
      (samplePercentage < 1 || samplePercentage > 100)) {
      collector.addFailure("Sample Percentage should be in the range 1 to 100.", null)
        .withConfigProperty(SAMPLE_PERCENTAGE);
    }

    if (random < 0 || random > 1) {
      collector.addFailure("Random should be in the range 0 to 1.", null)
        .withConfigProperty(RANDOM);
    }
  }

  /**
   * Types known to Sampling algorithm.
   */
  public enum TYPE {
    SYSTEMATIC, RESERVOIR
  }

  /**
   * Builder for creating a {@link SamplingConfig}.
   */
  public static final class Builder {
    private Integer sampleSize;
    private Float samplePercentage;
    private String samplingType;
    private Float overSamplingPercentage;
    private Float random;
    private Integer totalRecords;

    private Builder() {
    }

    public Builder setSampleSize(Integer sampleSize) {
      this.sampleSize = sampleSize;
      return this;
    }

    public Builder setSamplePercentage(Float samplePercentage) {
      this.samplePercentage = samplePercentage;
      return this;
    }

    public Builder setSamplingType(String samplingType) {
      this.samplingType = samplingType;
      return this;
    }

    public Builder setOverSamplingPercentage(Float overSamplingPercentage) {
      this.overSamplingPercentage = overSamplingPercentage;
      return this;
    }

    public Builder setRandom(Float random) {
      this.random = random;
      return this;
    }

    public Builder setTotalRecords(Integer totalRecords) {
      this.totalRecords = totalRecords;
      return this;
    }

    public SamplingConfig build() {
      return new SamplingConfig(this);
    }
  }
}
