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
package com.github.zlosim.streamsets.avroSchema.processor.inferschema.generators;

import _ss_com.streamsets.pipeline.stage.common.HeaderAttributeConstants;
import com.github.zlosim.streamsets.avroSchema.processor.inferschema.config.AvroDefaultConfig;
import com.github.zlosim.streamsets.avroSchema.processor.inferschema.config.AvroType;
import com.github.zlosim.streamsets.avroSchema.processor.inferschema.config.SchemaGeneratorConfig;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.sdk.RecordCreator;
import org.apache.avro.Schema;
import org.kitesdk.data.spi.JsonUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.Date;

import static org.powermock.api.mockito.PowerMockito.mock;

public class TestAvroSchemaGenerator {

  private SchemaGeneratorConfig config;
  private AvroSchemaGenerator generator;

  @Before
  public void setUp() {
    config = new SchemaGeneratorConfig();
    config.schemaName = "test_schema";
    generator = new AvroSchemaGenerator();
    generator.init(config, mock(Stage.Context.class));
  }

  public void generateAndValidateSchema(Record record, String fieldFragment) throws OnRecordErrorException {
    String schema = generator.generateSchema(record);
    Assert.assertNotNull(schema);
    Schema expectedSchema = Schema.parse("{\"type\":\"record\",\"name\":\"test_schema\",\"fields\":[" + fieldFragment + "]}");
    Assert.assertEquals(expectedSchema, Schema.parse(schema));
  }

  @Test
  public void testGenerateSimpleSchema() throws OnRecordErrorException {
    Record record = RecordCreator.create();
    record.set(Field.create(Field.Type.LIST_MAP, ImmutableMap.of(
        "a", Field.create(Field.Type.STRING, "Arvind"),
        "b", Field.create(Field.Type.BOOLEAN, true),
        "c", Field.create(Field.Type.LONG, 0),
        "d", Field.create(Field.Type.MAP, ImmutableMap.of(
            "e", Field.create(Field.Type.LONG, 5),
            "f", Field.create(Field.Type.LONG, 4)
        ))
    )));

    generateAndValidateSchema(
        record,
        "{ \"name\": \"a\", \"type\": \"string\" }, { \"name\": \"b\", \"type\": \"boolean\" }, { \"name\": \"c\", \"type\": \"long\" }, { \"name\": \"d\", \"type\": { \"type\": \"record\", \"name\": \"d\", \"fields\": [{ \"name\": \"e\", \"type\": \"long\" }, { \"name\": \"f\", \"type\": \"long\" }] } }"
    );
  }

  @Test
  public void testGenerateSimpleNullableSchema() throws OnRecordErrorException {
    Record record = RecordCreator.create();
    record.set(Field.create(Field.Type.LIST_MAP, ImmutableMap.of(
        "name", Field.create(Field.Type.STRING, "Bryan"),
        "salary", Field.create(Field.Type.INTEGER, 10)
    )));

    this.config.avroNullableFields = true;

    generateAndValidateSchema(
        record,
        "{\"name\":\"name\",\"type\":[\"null\",\"string\"]},{\"name\":\"salary\",\"type\":[\"null\",\"int\"]}"
    );
  }

  @Test
  public void testGenerateSimpleNullableSchemaDefaultToNull() throws OnRecordErrorException {
    Record record = RecordCreator.create();
    record.set(Field.create(Field.Type.LIST_MAP, ImmutableMap.of(
        "name", Field.create(Field.Type.STRING, "Bryan"),
        "salary", Field.create(Field.Type.INTEGER, 10)
    )));

    this.config.avroNullableFields = true;
    this.config.avroDefaultNullable = true;

    generateAndValidateSchema(
        record,
        "{\"name\":\"name\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"salary\",\"type\":[\"null\",\"int\"],\"default\":null}"
    );
  }

  @Test
  public void testGenerateSchemaDefaultForString() throws OnRecordErrorException {
    Record record = RecordCreator.create();
    record.set(Field.create(Field.Type.LIST_MAP, ImmutableMap.of(
        "name", Field.create(Field.Type.STRING, "Bryan")
    )));

    this.config.avroDefaultTypes = ImmutableList.of(new AvroDefaultConfig(AvroType.STRING, "defaultValue"));
    this.generator.init(config, mock(Stage.Context.class));

    generateAndValidateSchema(
        record,
        "{\"name\":\"name\",\"type\":\"string\",\"default\":\"defaultValue\"}"
    );
  }

  @Test
  public void testGenerateSchemaDefaultForFloat() throws OnRecordErrorException {
    Record record = RecordCreator.create();
    record.set(Field.create(Field.Type.LIST_MAP, ImmutableMap.of(
        "float", Field.create(Field.Type.FLOAT, 1.0f)
    )));

    this.config.avroDefaultTypes = ImmutableList.of(new AvroDefaultConfig(AvroType.FLOAT, "666.0"));
    this.generator.init(config, mock(Stage.Context.class));

    generateAndValidateSchema(
        record,
        "{\"name\":\"float\",\"type\":\"float\",\"default\":666.0}"
    );
  }

  @Test
  public void testGenerateDecimal() throws OnRecordErrorException {
    Field decimal = Field.create(Field.Type.DECIMAL, new BigDecimal("10.2"));
    decimal.setAttribute(HeaderAttributeConstants.ATTR_PRECISION, "3");
    decimal.setAttribute(HeaderAttributeConstants.ATTR_SCALE, "1");

    Record record = RecordCreator.create();
    record.set(Field.create(Field.Type.LIST_MAP, ImmutableMap.of(
        "decimal", decimal
    )));

    generateAndValidateSchema(
        record,
        "{\"name\":\"decimal\",\"type\":{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":3,\"scale\":1}}"
    );
  }

  @Test
  public void testGenerateDate() throws OnRecordErrorException {
    Record record = RecordCreator.create();
    record.set(Field.create(Field.Type.LIST_MAP, ImmutableMap.of(
        "date", Field.create(Field.Type.DATE, new Date())
    )));

    generateAndValidateSchema(
        record,
        "{\"name\":\"date\",\"type\":{\"type\":\"int\",\"logicalType\":\"date\"}}"
    );
  }

  @Test
  public void testGenerateTime() throws OnRecordErrorException {
    Record record = RecordCreator.create();
    record.set(Field.create(Field.Type.LIST_MAP, ImmutableMap.of(
        "time", Field.create(Field.Type.TIME, new Date())
    )));

    generateAndValidateSchema(
        record,
        "{\"name\":\"time\",\"type\":{\"type\":\"int\",\"logicalType\":\"time-millis\"}}"
    );
  }

  @Test
  public void testGenerateDateTime() throws OnRecordErrorException {
    Record record = RecordCreator.create();
    record.set(Field.create(Field.Type.LIST_MAP, ImmutableMap.of(
        "datetime", Field.create(Field.Type.DATETIME, new Date())
    )));

    generateAndValidateSchema(
        record,
        "{\"name\":\"datetime\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}}"
    );
  }

  @Test
  public void testGenerateList() throws OnRecordErrorException {
    Record record = RecordCreator.create();
    record.set(Field.create(Field.Type.LIST_MAP, ImmutableMap.of(
        "list", Field.create(Field.Type.LIST, ImmutableList.of(
            Field.create(Field.Type.STRING, "Arvind"),
            Field.create(Field.Type.STRING, "Girish")
            )
        ))));

    generateAndValidateSchema(
        record,
        "{\"name\":\"list\",\"type\":{\"type\":\"array\",\"items\":\"string\"}}"
    );
  }

  @Test
  public void testGenerateRecord() throws OnRecordErrorException {
    Record record = RecordCreator.create();
    record.set(Field.create(Field.Type.LIST_MAP, ImmutableMap.builder()
        .put("event_type", Field.create(Field.Type.STRING, "destination"))
        .put("player_id", Field.create(Field.Type.STRING, "36737914"))
        .put("event_data", Field.create(Field.Type.MAP, ImmutableMap.builder()
            .put("train_type", Field.create(Field.Type.STRING, "local"))
            .put("destination", Field.create(Field.Type.STRING, "300"))
            .put("locomotive", Field.create(Field.Type.STRING, "2735"))
            .put("wagon_goods", Field.create(Field.Type.STRING, "2137,2137,2137,2325,2325"))
            .put("level", Field.create(Field.Type.INTEGER, 949))
            .put("tech", Field.create(Field.Type.MAP, ImmutableMap.builder()
                .put("device", Field.create(Field.Type.STRING, "local"))
                .put("device_model", Field.create(Field.Type.STRING, "local"))
                .put("resolution", Field.create(Field.Type.STRING, "local"))
                .put("os", Field.create(Field.Type.STRING, "300"))
                .put("os_version", Field.create(Field.Type.STRING, "2735"))
                .put("browser", Field.create(Field.Type.STRING, "2735"))
                .put("browser_version", Field.create(Field.Type.STRING, "2735"))
                .put("app_version_client", Field.create(Field.Type.STRING, "2735"))
                .put("app_version_server", Field.create(Field.Type.STRING, "2735"))
                .put("flash_version", Field.create(Field.Type.STRING, "2735"))
                .put("appsflyer_id", Field.create(Field.Type.STRING, "2735"))
                .put("platform", Field.create(Field.Type.STRING, "2735"))
                .put("session_token", Field.create(Field.Type.STRING, "2735"))
                .build()))
            .build()))
        .put("meta_data", Field.create(Field.Type.MAP, ImmutableMap.builder()
            .put("hostname", Field.create(Field.Type.STRING, "local"))
            .put("run_id", Field.create(Field.Type.STRING, "local"))
            .put("producer", Field.create(Field.Type.MAP, ImmutableMap.builder()
                .put("hostname", Field.create(Field.Type.STRING, "local"))
                .put("counter", Field.create(Field.Type.INTEGER, 12332))
                .put("run_id", Field.create(Field.Type.STRING, "local"))
                .put("version", Field.create(Field.Type.STRING, "local"))
                .build()))
            .put("counter", Field.create(Field.Type.INTEGER, 308334))
            .put("version", Field.create(Field.Type.STRING, "2735"))
            .build()))
        .put("timestamp", Field.create(Field.Type.STRING, "2735"))
        .put("event_referrer", Field.create(Field.Type.MAP, ImmutableMap.builder()
            .put("scheme", Field.create(Field.Type.STRING, "local"))
            .put("host", Field.create(Field.Type.STRING, "local"))
            .put("path", Field.create(Field.Type.STRING, "dsa"))
            .put("query", Field.create(Field.Type.STRING, "2735"))
            .put("original_url", Field.create(Field.Type.STRING, "2735"))
            .put("params", Field.create(Field.Type.MAP, ImmutableMap.builder()
                .put("version", Field.create(Field.Type.STRING, "local"))
                .build()))
            .build()))
        .put("event_input", Field.create(Field.Type.MAP, ImmutableMap.builder()
            .put("scheme", Field.create(Field.Type.STRING, "local"))
            .put("host", Field.create(Field.Type.STRING, "local"))
            .put("path", Field.create(Field.Type.STRING, "dsa"))
            .put("query", Field.create(Field.Type.STRING, "2735"))
            .put("original_url", Field.create(Field.Type.STRING, "2735"))
            .put("params", Field.create(Field.Type.MAP, ImmutableMap.builder()
                .put("a", Field.create(Field.Type.STRING, "local"))
                .build()))
            .build())).build()
    ));


    String schema = generator.generateSchema(record);
    Assert.assertNotNull(schema);
    Schema expectedSchema = JsonUtil.inferSchema(JsonUtil.parse("{\"event_type\":\"destination\",\"player_id\":\"36737914\",\"event_data\":{\"train_type\":\"local\",\"destination\":\"300\",\"locomotive\":\"2735\",\"wagon_goods\":\"2137,2137,2137,2325,2325\",\"level\":949,\"tech\":{\"device\":\"desktop\",\"device_model\":\"\",\"resolution\":\"\",\"os\":\"Windows\",\"os_version\":\"unknown\",\"browser\":\"Chrome\",\"browser_version\":\"64.0.3282.140\",\"app_version_client\":\"v0.9.2291\",\"app_version_server\":\"v158.2.0\",\"flash_version\":\"\",\"appsflyer_id\":\"\",\"platform\":\"portal\",\"session_token\":\"a0017ef8-87bd-5b6f-9f30-16586cad4d69\"}},\"meta_data\":{\"hostname\":\"tsm-prod-php-08\",\"run_id\":\"1534161013.59065b7170759033c7.09381248\",\"producer\":{\"hostname\":\"dwh-java-16\",\"counter\":712787656,\"run_id\":\"802f87ed-2911-409d-99a1-2c621719b364\",\"version\":\"1.2.7\"},\"counter\":308334,\"version\":\"1.3\"},\"timestamp\":\"2018-08-13T13:44:53+0000\",\"event_referrer\":{\"scheme\":\"https\",\"host\":\"static2.trainstationgame.com\",\"path\":\"/client_static/client/Trainz_v0.9.2291.swf\",\"query\":\"version=v0.9.473\",\"original_url\":\"https://static2.trainstationgame.com/client_static/client/Trainz_v0.9.2291.swf?version=v0.9.473\",\"params\":{\"version\":\"v0.9.473\"}},\"event_input\":{\"scheme\":\"https\",\"host\":\"tsm.trainstationgame.com\",\"path\":\"/\",\"query\":\"a=game/synchronize\",\"original_url\":\"https://tsm.trainstationgame.com/?a=game/synchronize\",\"params\":{\"a\":\"game/synchronize\"}}}"), "test_schema");
    Assert.assertEquals(expectedSchema, Schema.parse(schema));
  }

  @Test
  public void testGenerateMapWithDefaultValues() throws OnRecordErrorException {
    Record record = RecordCreator.create();
    record.set(Field.create(Field.Type.LIST_MAP, ImmutableMap.of(
        "map", Field.create(Field.Type.MAP, ImmutableMap.of(
            "Doer", Field.create(Field.Type.STRING, "Arvind"),
            "Talker", Field.create(Field.Type.STRING, "Girish")
            )
        ))));

    this.config.avroDefaultTypes = ImmutableList.of(new AvroDefaultConfig(AvroType.STRING, "defaultValue"));
    this.generator.init(config, mock(Stage.Context.class));

    String schema = generator.generateSchema(record);
    Assert.assertNotNull(schema);
    Schema expectedSchema = new Schema.Parser().parse("{ \"type\": \"record\", \"name\": \"test_schema\", \"fields\": [{ \"name\": \"map\", \"type\": { \"type\": \"record\", \"name\": \"map\", \"fields\": [{ \"name\": \"Doer\", \"type\": \"string\", \"default\": \"defaultValue\" }, { \"name\": \"Talker\", \"type\": \"string\", \"default\": \"defaultValue\" }] } }] }");
    Assert.assertEquals(expectedSchema, new Schema.Parser().parse(schema));

  }

  @Test(expected = OnRecordErrorException.class)
  public void testGenerateMapNull() throws OnRecordErrorException {
    Record record = RecordCreator.create();
    record.set(Field.create(Field.Type.LIST_MAP, ImmutableMap.of(
        "map", Field.create(Field.Type.MAP, null
        ))));

    this.config.avroDefaultTypes = ImmutableList.of(new AvroDefaultConfig(AvroType.STRING, "defaultValue"));
    this.generator.init(config, mock(Stage.Context.class));

    generateAndValidateSchema(record, null);
  }

  @Test(expected = OnRecordErrorException.class)
  public void testGenerateMapEmpty() throws OnRecordErrorException {
    Record record = RecordCreator.create();
    record.set(Field.create(Field.Type.LIST_MAP, ImmutableMap.of(
        "map", Field.create(Field.Type.MAP, Collections.emptyMap()
        ))));

    this.config.avroDefaultTypes = ImmutableList.of(new AvroDefaultConfig(AvroType.STRING, "defaultValue"));
    this.generator.init(config, mock(Stage.Context.class));

    generateAndValidateSchema(record, null);
  }

  @Test
  public void testGenerateExpandedTypes() throws OnRecordErrorException {
    Record record = RecordCreator.create();
    record.set(Field.create(Field.Type.LIST_MAP, ImmutableMap.of(
        "short", Field.create(Field.Type.SHORT, 10),
        "char", Field.create(Field.Type.CHAR, 'A')
    )));

    this.config.avroExpandTypes = true;

    generateAndValidateSchema(
        record,
        "{\"name\":\"short\",\"type\":\"int\"},{\"name\":\"char\",\"type\":\"string\"}"
    );
  }
}
