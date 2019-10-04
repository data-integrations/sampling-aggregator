/*
 * Copyright © 2017-2019 Cask Data, Inc.
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
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.BatchAggregator;
import io.cdap.cdap.etl.api.batch.BatchAggregatorContext;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.collections.buffer.PriorityBuffer;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

/**
 * Sampling plugin to sample random data from large dataset flowing through the plugin.
 */
@Plugin(type = BatchAggregator.PLUGIN_TYPE)
@Name("Sampling")
@Description("Sample a large dataset allowing only a subset of records through to the next stage.")
public class Sampling extends BatchAggregator<String, StructuredRecord, StructuredRecord> {
  private SamplingConfig config;

  public Sampling(SamplingConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    FailureCollector collector = stageConfigurer.getFailureCollector();

    config.validate(collector);
    collector.getOrThrowException();

    stageConfigurer.setOutputSchema(stageConfigurer.getInputSchema());
  }

  @Override
  public void prepareRun(BatchAggregatorContext context) {
    FailureCollector collector = context.getFailureCollector();
    config.validate(collector);
    collector.getOrThrowException();
  }

  @Override
  public void groupBy(StructuredRecord record, Emitter<String> emitter) {
    emitter.emit("sample");
  }

  @Override
  public void aggregate(String groupKey, Iterator<StructuredRecord> iterator,
                        Emitter<StructuredRecord> emitter) {
    int finalSampleSize = 0;
    if (config.getSampleSize() != null) {
      finalSampleSize = config.getSampleSize();
    }
    if (config.getSamplePercentage() != null) {
      finalSampleSize = Math.round((config.getSamplePercentage() / 100) * config.getTotalRecords());
    }

    switch (SamplingConfig.TYPE.valueOf(config.getSamplingType().toUpperCase())) {
      case SYSTEMATIC:
        if (config.getOverSamplingPercentage() != null) {
          finalSampleSize = Math.round(finalSampleSize +
                                         (finalSampleSize * (config.getOverSamplingPercentage() / 100)));
        }

        int sampleIndex = Math.round(config.getTotalRecords() / finalSampleSize);
        Float random;
        if (config.getRandom() != null) {
          random = config.getRandom();
        } else {
          random = new Random().nextFloat();
        }
        int firstSampleIndex = Math.round(sampleIndex * random);
        List<StructuredRecord> records = IteratorUtils.toList(iterator);
        int counter = 0;
        emitter.emit(records.get(firstSampleIndex));
        counter++;

        while (counter < finalSampleSize) {
          int index = firstSampleIndex + (counter * sampleIndex);
          emitter.emit(records.get(index - 1));
          counter++;
        }
        break;

      case RESERVOIR:
        PriorityBuffer sampleData = new PriorityBuffer(true, new Comparator<StructuredRecord>() {
          @Override
          public int compare(StructuredRecord o1, StructuredRecord o2) {
            return Float.compare((float) o2.get("random"), (float) o1.get("random"));
          }
        });

        int count = 0;
        Random randomValue = new Random();
        List<StructuredRecord> recordArray = IteratorUtils.toList(iterator);
        Schema inputSchema = recordArray.get(0).getSchema();
        Schema schemaWithRandomField = createSchemaWithRandomField(inputSchema);
        while (count < finalSampleSize) {
          StructuredRecord record = recordArray.get(0);
          sampleData.add(getSampledRecord(record, randomValue.nextFloat(), schemaWithRandomField));
          count++;
        }

        while (count < recordArray.size()) {
          StructuredRecord structuredRecord = (StructuredRecord) sampleData.get();
          Float randomFloat = randomValue.nextFloat();
          if ((float) structuredRecord.get("random") < randomFloat) {
            sampleData.remove();
            StructuredRecord record = recordArray.get(count);
            sampleData.add(getSampledRecord(record, randomFloat, structuredRecord.getSchema()));
          }
          count++;
        }

        Iterator<StructuredRecord> sampleDataIterator = sampleData.iterator();
        while (sampleDataIterator.hasNext()) {
          StructuredRecord sampledRecord = sampleDataIterator.next();
          StructuredRecord.Builder builder = StructuredRecord.builder(inputSchema);
          for (Schema.Field field : sampledRecord.getSchema().getFields()) {
            if (!field.getName().equalsIgnoreCase("random")) {
              builder.set(field.getName(), sampledRecord.get(field.getName()));
            }
          }
          emitter.emit(builder.build());
        }
        break;
    }
  }

  public StructuredRecord getSampledRecord(StructuredRecord record, Float random, Schema schema) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    for (Schema.Field field : record.getSchema().getFields()) {
      builder.set(field.getName(), record.get(field.getName()));
    }
    builder.set("random", random);
    return builder.build();
  }

  /**
   * Builds the schema for Reservoir sampling algorithm. Adding field for random value.
   */
  private Schema createSchemaWithRandomField(Schema inputSchema) {
    List<Schema.Field> fields = new ArrayList<>();
    fields.add(Schema.Field.of("random", Schema.of(Schema.Type.FLOAT)));
    fields.addAll(inputSchema.getFields());
    return Schema.recordOf("schema", fields);
  }
}
