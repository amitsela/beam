package org.apache.beam.runners.spark.translation.streaming;

import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.runners.spark.translation.SparkContextFactory;
import org.apache.beam.runners.spark.translation.SparkPipelineTranslator;
import org.apache.beam.runners.spark.translation.TransformTranslator;
import org.apache.beam.sdk.Pipeline;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link JavaStreamingContext} factory for resilience.
 * For more details see: https://spark.apache.org/docs/1.6.2/streaming-programming-guide.html#how-to-configure-checkpointing
 */
public class SparkRunnerStreamingContextFactory implements JavaStreamingContextFactory {
  private static final Logger LOG =
      LoggerFactory.getLogger(SparkRunnerStreamingContextFactory.class);

  private final Pipeline pipeline;
  private final SparkPipelineOptions options;

  public SparkRunnerStreamingContextFactory(Pipeline pipeline, SparkPipelineOptions options) {
    this.pipeline = pipeline;
    this.options = options;
  }

  private StreamingEvaluationContext ctxt;

  @Override
  public JavaStreamingContext create() {
    LOG.info("Creating a new Spark Streaming Context");

    SparkPipelineTranslator translator = new StreamingTransformTranslator.Translator(
        new TransformTranslator.Translator());
    Duration batchDuration = new Duration(options.getBatchIntervalMillis());
    LOG.info("Setting Spark streaming batchDuration to {} msec", batchDuration.milliseconds());

    JavaSparkContext jsc = SparkContextFactory.getSparkContext(options);
    JavaStreamingContext jssc = new JavaStreamingContext(jsc, batchDuration);
    ctxt = new StreamingEvaluationContext(jsc, pipeline, jssc,
        options.getTimeout());
    pipeline.traverseTopologically(new SparkRunner.Evaluator(translator, ctxt));
    ctxt.computeOutputs();

    jssc.checkpoint(options.getCheckpointDir());
    return jssc;
  }

  public StreamingEvaluationContext getCtxt() {
    return ctxt;
  }
}
