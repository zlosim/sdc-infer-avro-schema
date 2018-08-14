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
package com.github.zlosim.streamsets.avroSchema.processor.inferschema.config;

import com.github.zlosim.streamsets.avroSchema.processor.inferschema.generators.AvroSchemaGenerator;
import com.github.zlosim.streamsets.avroSchema.processor.inferschema.generators.SchemaGenerator;
import com.streamsets.pipeline.api.Label;

public enum SchemaType implements Label {
  AVRO("Avro", AvroSchemaGenerator.class),
  ;

  private final String label;
  private final Class<? extends SchemaGenerator> generator;

  SchemaType(String label, Class<? extends SchemaGenerator> generator) {
    this.label = label;
    this.generator = generator;
  }

  @Override
  public String getLabel() {
    return label;
  }

  public Class<? extends SchemaGenerator> getGenerator() {
    return generator;
  }
}
