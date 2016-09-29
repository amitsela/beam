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


import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Uninterruptibles;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.beam.runners.spark.EvaluationResult;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.aggregators.AccumulatorSingleton;
import org.apache.beam.runners.spark.translation.streaming.utils.EmbeddedKafkaCluster;
import org.apache.beam.runners.spark.translation.streaming.utils.PAssertStreaming;
import org.apache.beam.runners.spark.translation.streaming.utils.TestOptionsForStreaming;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;



/**
 * Test DStream recovery from checkpoint.
 * <p>Test the following:
 * <ul>
 * <li>Offset recovery: consumer set to "earliest", but a proper restart from checkpoint should
 * not re-read consumed records.</li>
 * <li>Accumulator recovery: Beam {@link Aggregator}s are implemented
 * using {@link org.apache.spark.Accumulator}s, make sure they are not stale after recovery.</li>
 * //TODO: after the runner supports recovering the state of Aggregators, update this test's
 * </ul>
 * </p>
 */
public class RecoverFromCheckpointStreamingTest {
  private static final EmbeddedKafkaCluster.EmbeddedZookeeper EMBEDDED_ZOOKEEPER =
      new EmbeddedKafkaCluster.EmbeddedZookeeper();
  private static final EmbeddedKafkaCluster EMBEDDED_KAFKA_CLUSTER =
      new EmbeddedKafkaCluster(EMBEDDED_ZOOKEEPER.getConnection(), new Properties());

  private static final String TOPIC = "topic";

  @Rule
  public TemporaryFolder checkpointParentDir = new TemporaryFolder();

  @Rule
  public TestOptionsForStreaming commonOptions = new TestOptionsForStreaming();

  @BeforeClass
  public static void init() throws IOException {
    EMBEDDED_ZOOKEEPER.startup();
    EMBEDDED_KAFKA_CLUSTER.startup();
    //--- this is a recovery test, we actually want the context to close, and restart,
    // disable the reuse set by surefire plugin.
    System.setProperty("beam.spark.test.reuseSparkContext", "false");
  }

  @Test
  public void testRun() throws Exception {
    SparkPipelineOptions options = commonOptions.withTmpCheckpointDir(
        checkpointParentDir.newFolder(getClass().getSimpleName()));

    //--- first run.
    String[] expected1 = new String[]{"k1,v1", "k2,v2", "k3,v3", "k4,v4"};
    produce(ImmutableMap.of("k1", "v1", "k2", "v2", "k3", "v3", "k4", "v4"));
    EvaluationResult res = run(options, expected1);
    res.close();
    long processedMessages1 = res.getAggregatorValue("processedMessages", Long.class);
    Assert.assertEquals(String.format("Expected %d processed messages count but "
        + "found %d", expected1.length, processedMessages1), expected1.length, processedMessages1);

    //-- second run, from checkpoint.
    String[] expected2 = new String[]{"k5,v5"};
    produce(ImmutableMap.of("k5", "v5"));
    res = runAgain(options, expected2);
    res.close();
    long processedMessages2 = res.getAggregatorValue("processedMessages", Long.class);
    //TODO: once Accumulators can recover value -> assert = expected1 + expected2.
    Assert.assertEquals(String.format("Expected %d processed messages count but "
        + "found %d", expected2.length, processedMessages2), expected2.length, processedMessages2);
  }

  private static EvaluationResult runAgain(SparkPipelineOptions options, String[] expected) {
    // sleep before next run.
    Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
    AccumulatorSingleton.clear();
    return run(options, expected);
  }

  private static EvaluationResult run(SparkPipelineOptions options, String[] expected) {
    Map<String, Object> consumerProps = ImmutableMap.<String, Object>of(
        "auto.offset.reset", "earliest"
    );

    KafkaIO.Read<String, String> read = KafkaIO.read()
        .withBootstrapServers(EMBEDDED_KAFKA_CLUSTER.getBrokerList())
        .withTopics(Collections.singletonList(TOPIC))
        .withKeyCoder(StringUtf8Coder.of())
        .withValueCoder(StringUtf8Coder.of())
        .updateConsumerProperties(consumerProps);

    Duration windowDuration = new Duration(options.getBatchIntervalMillis());

    Pipeline p = Pipeline.create(options);
    PCollection<String> formattedKV =
        p.apply(read.withoutMetadata())
        .apply(Window.<KV<String, String>>into(FixedWindows.of(windowDuration)))
        .apply(ParDo.of(new FormatAsText()));

    PAssertStreaming.assertContents(formattedKV, expected);

    return  (EvaluationResult) p.run();
  }

  private static void produce(Map<String, String> messages) {
    Properties producerProps = new Properties();
    producerProps.putAll(EMBEDDED_KAFKA_CLUSTER.getProps());
    producerProps.put("acks", "1");
    producerProps.put("bootstrap.servers", EMBEDDED_KAFKA_CLUSTER.getBrokerList());
    Serializer<String> stringSerializer = new StringSerializer();
    try (@SuppressWarnings("unchecked") KafkaProducer<String, String> kafkaProducer =
        new KafkaProducer(producerProps, stringSerializer, stringSerializer)) {
          for (Map.Entry<String, String> en : messages.entrySet()) {
            kafkaProducer.send(new ProducerRecord<>(TOPIC, en.getKey(), en.getValue()));
          }
          // await send completion.
          kafkaProducer.flush();
        }
  }

  @AfterClass
  public static void tearDown() {
    EMBEDDED_KAFKA_CLUSTER.shutdown();
    EMBEDDED_ZOOKEEPER.shutdown();
  }

  private static class FormatAsText extends DoFn<KV<String, String>, String> {

    private final Aggregator<Long, Long> aggregator =
        createAggregator("processedMessages", new Sum.SumLongFn());

    @ProcessElement
    public void process(ProcessContext c) {
      aggregator.addValue(1L);
      String formatted = c.element().getKey() + "," + c.element().getValue();
      c.output(formatted);
    }
  }

}
