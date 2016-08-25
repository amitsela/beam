package org.apache.beam.runners.spark.translation.streaming;

import org.apache.beam.runners.spark.EvaluationResult;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.runners.spark.io.ConsoleIO;
import org.apache.beam.runners.spark.translation.streaming.utils.UnboundedSocketSource;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.apache.spark.streaming.Durations;
import org.junit.Test;

/**
 * Created by ansela on 8/16/16.
 */
public class TestFromSocket {

  @Test
  public void test() throws Exception {
    // test read from Kafka
    SparkPipelineOptions options =
            PipelineOptionsFactory.as(SparkPipelineOptions.class);
    options.setRunner(SparkRunner.class);
    options.setStreaming(true);
    options.setBatchIntervalMillis(Durations.seconds(3).milliseconds());

    Pipeline p = Pipeline.create(options);

    PCollection<String> input = p.apply(Read.from(
        new UnboundedSocketSource<>("localhost", 9999, '\n', 3)));
    input.apply(ConsoleIO.Write.<String>from());

    EvaluationResult res = (EvaluationResult) p.run();
    res.close();
  }
}
