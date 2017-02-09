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

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Uninterruptibles;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.SparkPipelineResult;
import org.apache.beam.runners.spark.aggregators.ClearAggregatorsRule;
import org.apache.beam.runners.spark.coders.CoderHelpers;
import org.apache.beam.runners.spark.translation.streaming.utils.EmbeddedKafkaCluster;
import org.apache.beam.runners.spark.translation.streaming.utils.ReuseSparkContext;
import org.apache.beam.runners.spark.translation.streaming.utils.SparkTestPipelineOptionsForStreaming;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;


/**
 * Tests DStream recovery from checkpoint - recreate the job and continue (from checkpoint).
 *
 * <p>Tests Aggregators, which rely on Accumulators - Aggregators should be available, though
 * state is not preserved (Spark issue), so they start from initial value.
 * //TODO: after the runner supports recovering the state of Aggregators, update this test's
 * expected values for the recovered (second) run.
 */
public class ResumeFromCheckpointStreamingTest {
  private static final EmbeddedKafkaCluster.EmbeddedZookeeper EMBEDDED_ZOOKEEPER =
      new EmbeddedKafkaCluster.EmbeddedZookeeper();
  private static final EmbeddedKafkaCluster EMBEDDED_KAFKA_CLUSTER =
      new EmbeddedKafkaCluster(EMBEDDED_ZOOKEEPER.getConnection(), new Properties());
  private static final String TOPIC = "kafka_beam_test_topic";

  @Rule
  public TemporaryFolder checkpointParentDir = new TemporaryFolder();

  @Rule
  public SparkTestPipelineOptionsForStreaming commonOptions =
      new SparkTestPipelineOptionsForStreaming();

  @Rule
  public ReuseSparkContext noContextResue = ReuseSparkContext.no();

  @BeforeClass
  public static void init() throws IOException {
    EMBEDDED_ZOOKEEPER.startup();
    EMBEDDED_KAFKA_CLUSTER.startup();
  }

  private static void produce(Map<String, Instant> messages) {
    Properties producerProps = new Properties();
    producerProps.putAll(EMBEDDED_KAFKA_CLUSTER.getProps());
    producerProps.put("request.required.acks", 1);
    producerProps.put("bootstrap.servers", EMBEDDED_KAFKA_CLUSTER.getBrokerList());
    Serializer<String> stringSerializer = new StringSerializer();
    Serializer<Instant> instantSerializer = new Serializer<Instant>() {
      @Override
      public void configure(Map<String, ?> configs, boolean isKey) { }

      @Override
      public byte[] serialize(String topic, Instant data) {
        return CoderHelpers.toByteArray(data, InstantCoder.of());
      }

      @Override
      public void close() { }
    };
    try (@SuppressWarnings("unchecked") KafkaProducer<String, Instant> kafkaProducer =
        new KafkaProducer(producerProps, stringSerializer, instantSerializer)) {
          for (Map.Entry<String, Instant> en : messages.entrySet()) {
            kafkaProducer.send(new ProducerRecord<>(TOPIC, en.getKey(), en.getValue()));
          }
          kafkaProducer.close();
        }
  }

  @Test
  public void testWithResume() throws Exception {
    SparkPipelineOptions options = commonOptions.withTmpCheckpointDir(checkpointParentDir);

    options.setCheckpointDurationMillis(500L);

    // write to Kafka
    produce(ImmutableMap.of(
        "k1", new Instant(100),
        "k2", new Instant(200),
        "k3", new Instant(300),
        "k4", new Instant(400)
    ));

    // first run will read from Kafka backlog - "auto.offset.reset=smallest"
    SparkPipelineResult res = run(options);
    res.waitUntilFinish(Duration.standardSeconds(2));
    long processedMessages1 = res.getAggregatorValue("processedMessages", Long.class);
    assertThat(String.format("Expected %d processed messages count but "
        + "found %d", 4, processedMessages1), processedMessages1, equalTo(4L));

    // write a bit more.
    produce(ImmutableMap.of(
        "k5", new Instant(499)
    ));

    // recovery should resume from last read offset, and read the second batch of input.
    res = runAgain(options);
    res.waitUntilFinish(Duration.standardSeconds(2));
    long processedMessages2 = res.getAggregatorValue("processedMessages", Long.class);
    assertThat(String.format("Expected %d processed messages count but "
        + "found %d", 1, processedMessages2), processedMessages2, equalTo(1L));
  }

  private SparkPipelineResult runAgain(SparkPipelineOptions options) {
    // sleep before next run.
    Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
    return run(options);
  }

  private static SparkPipelineResult run(SparkPipelineOptions options) {
    KafkaIO.TypedRead<String, Instant> read = KafkaIO.read()
        .withBootstrapServers(EMBEDDED_KAFKA_CLUSTER.getBrokerList())
        .withTopics(Collections.singletonList(TOPIC))
        .withKeyCoder(StringUtf8Coder.of())
        .withValueCoder(InstantCoder.of())
        .updateConsumerProperties(ImmutableMap.<String, Object>of("auto.offset.reset", "earliest"))
        .withTimestampFn(new SerializableFunction<KV<String, Instant>, Instant>() {
          @Override
          public Instant apply(KV<String, Instant> kv) {
            return kv.getValue();
          }
        }).withWatermarkFn(new SerializableFunction<KV<String, Instant>, Instant>() {
          @Override
          public Instant apply(KV<String, Instant> kv) {
            // we add 1 milli to reach end-of-window.
            return kv.getValue().plus(Duration.millis(1L));
          }
        });

    Pipeline p = Pipeline.create(options);

    PCollection<String> windowed = p
        .apply(read.withoutMetadata())
        .apply(Keys.<String>create())
        .apply(ParDo.of(new PassThroughFn<String>()))
        .apply(Window.<String>into(FixedWindows.of(Duration.millis(500)))
            .triggering(AfterWatermark.pastEndOfWindow()
                .withEarlyFirings(AfterPane.elementCountAtLeast(4)))
                .accumulatingFiredPanes()
                .withAllowedLateness(Duration.ZERO));

    PCollection<Long> count = windowed.apply(Count.<String>globally().withoutDefaults());

    count.apply(new PAssertWithoutFlatten());

    return (SparkPipelineResult) p.run();
  }

  @AfterClass
  public static void tearDown() {
    EMBEDDED_KAFKA_CLUSTER.shutdown();
    EMBEDDED_ZOOKEEPER.shutdown();
  }

  private static class PassThroughFn<T> extends DoFn<T, T> {

    private final Aggregator<Long, Long> aggregator =
        createAggregator("processedMessages", Sum.ofLongs());

    @ProcessElement
    public void process(ProcessContext c) {
      aggregator.addValue(1L);
      c.output(c.element());
    }
  }

}
