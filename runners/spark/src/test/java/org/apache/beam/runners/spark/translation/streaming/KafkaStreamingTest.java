/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.spark.translation.streaming;

import org.apache.beam.runners.spark.EvaluationResult;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.runners.spark.io.ConsoleIO;
import org.apache.beam.runners.spark.translation.streaming.utils.EmbeddedKafkaCluster;
import org.apache.beam.runners.spark.translation.streaming.utils.PAssertStreaming;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import com.google.common.collect.ImmutableMap;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.streaming.Durations;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
/**
 * Test Kafka as input.
 */
public class KafkaStreamingTest {
  private static final EmbeddedKafkaCluster.EmbeddedZookeeper EMBEDDED_ZOOKEEPER =
          new EmbeddedKafkaCluster.EmbeddedZookeeper(1701);
  private static final EmbeddedKafkaCluster EMBEDDED_KAFKA_CLUSTER =
          new EmbeddedKafkaCluster(EMBEDDED_ZOOKEEPER.getConnection(), new Properties(),
              Collections.singletonList(6667));
  private static final String TOPIC = "kafka_beam_test_topic";
  private static final Map<String, String> KAFKA_MESSAGES = ImmutableMap.of(
      "k1", "v1", "k2", "v2", "k3", "v3", "k4", "v4"
  );
  private static final String[] EXPECTED = {"k1,v1", "k2,v2", "k3,v3", "k4,v4"};
  private static final long TEST_TIMEOUT_MSEC = 10000000L;

  @BeforeClass
  public static void init() throws IOException {
    EMBEDDED_ZOOKEEPER.startup();
    EMBEDDED_KAFKA_CLUSTER.startup();

    // write to Kafka
    Properties producerProps = new Properties();
    producerProps.putAll(EMBEDDED_KAFKA_CLUSTER.getProps());
    producerProps.put("acks", "1");
    producerProps.put("bootstrap.servers", EMBEDDED_KAFKA_CLUSTER.getBrokerList());
    producerProps.put("request.timeout.ms", "500");
    Serializer<String> stringSerializer = new StringSerializer();
    try (@SuppressWarnings("unchecked") KafkaProducer<String, String> kafkaProducer =
        new KafkaProducer(producerProps, stringSerializer, stringSerializer)) {
      for (Map.Entry<String, String> en : KAFKA_MESSAGES.entrySet()) {
        kafkaProducer.send(new ProducerRecord<>(TOPIC, en.getKey(), en.getValue()));
      }
    }
  }

  @Test
  public void testRun() throws Exception {
    // test read from Kafka
    SparkPipelineOptions options =
        PipelineOptionsFactory.as(SparkPipelineOptions.class);
    options.setRunner(SparkRunner.class);
    options.setStreaming(true);
    options.setSparkMaster("local[*]");
    options.setBatchIntervalMillis(Durations.seconds(1).milliseconds());
    options.setTimeout(TEST_TIMEOUT_MSEC); // run for one interval
    Pipeline p = Pipeline.create(options);

    Map<String, Object> consumerProperties = ImmutableMap.<String, Object>of(
        "bootstrap.servers", EMBEDDED_KAFKA_CLUSTER.getBrokerList(),
        "group.id", "spark-beam-test-group",
        "auto.offset.reset", "earliest"
    );

    KafkaIO.TypedRead<String, String> reader = KafkaIO.read()
        .withKeyCoder(StringUtf8Coder.of())
        .withValueCoder(StringUtf8Coder.of())
        .withBootstrapServers(EMBEDDED_KAFKA_CLUSTER.getBrokerList())
        .updateConsumerProperties(consumerProperties)
        .withTopics(Collections.singletonList(TOPIC));


    PCollection<KafkaRecord<String, String>> input = p.apply(reader);
    PCollection<String> formattedKV = input.apply(ParDo.of(new FormatKVFn()));

//    PAssertStreaming.assertContents(formattedKV, EXPECTED);
    formattedKV.apply(ConsoleIO.Write.<String>from());

    EvaluationResult res = (EvaluationResult) p.run();
    res.close();
  }

  @AfterClass
  public static void tearDown() {
    EMBEDDED_KAFKA_CLUSTER.shutdown();
    EMBEDDED_ZOOKEEPER.shutdown();
  }

  private static class FormatKVFn extends DoFn<KafkaRecord<String, String>, String> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      KafkaRecord<String, String> kafkaRecord = c.element();
      String key = kafkaRecord.getKV().getKey();
      String value = kafkaRecord.getKV().getValue();
      String formatted = String.format("Kafka Record with key: %s value: %s read from topic: %s "
          + "partition: %d offset: %d", key, value, kafkaRecord.getTopic(),
              kafkaRecord.getPartition(), kafkaRecord.getOffset());
      c.output(formatted);
    }
  }

}
