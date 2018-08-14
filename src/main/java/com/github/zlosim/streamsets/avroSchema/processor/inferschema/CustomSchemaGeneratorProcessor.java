/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.zlosim.streamsets.avroSchema.processor.inferschema;

import com.github.zlosim.streamsets.avroSchema.processor.inferschema.config.SchemaGeneratorConfig;
import com.github.zlosim.streamsets.avroSchema.processor.inferschema.generators.SchemaGenerator;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;

import java.util.List;

public class CustomSchemaGeneratorProcessor extends SingleLaneRecordProcessor {


  private final SchemaGeneratorConfig config;
  private SchemaGenerator generator;

  public CustomSchemaGeneratorProcessor(SchemaGeneratorConfig config) {
    this.config = config;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();

    // Instantiate configured generator
    try {
      this.generator = config.schemaType.getGenerator().newInstance();
      issues.addAll(generator.init(config, getContext()));
    } catch (InstantiationException|IllegalAccessException e) {
      issues.add(getContext().createConfigIssue("SCHEMA", "config.schemaType", ErrorsCustomInfer.SCHEMA_GEN_0001, e.toString()));
    }

    return issues;
  }

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    String schema = null;

    schema = generator.generateSchema(record);

    record.getHeader().setAttribute(config.attributeName, schema);
    batchMaker.addRecord(record);
  }

}
