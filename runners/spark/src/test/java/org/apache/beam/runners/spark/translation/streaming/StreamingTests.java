package org.apache.beam.runners.spark.translation.streaming;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.io.Serializable;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.io.CreateStream;
import org.apache.beam.runners.spark.translation.streaming.utils.ReuseSparkContext;
import org.apache.beam.runners.spark.translation.streaming.utils.SparkTestPipelineOptionsForStreaming;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Never;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;


/**
 * A test suite to test Spark runner implementation of triggers and panes.
 *
 * <p>Since Spark is a micro-batch engine, and will process any test-sized input
 * within the same (first) batch, it is important to make sure inputs are ingested across
 * micro-batches using {@link org.apache.spark.streaming.dstream.QueueInputDStream}.
 */
public class StreamingTests implements Serializable {

  @Rule
  public transient TemporaryFolder checkpointParentDir = new TemporaryFolder();
  @Rule
  public transient SparkTestPipelineOptionsForStreaming commonOptions =
      new SparkTestPipelineOptionsForStreaming();
  @Rule
  public transient ReuseSparkContext noContextResue = ReuseSparkContext.no();
  @Rule
  public transient TestName testName = new TestName();
  @Rule
  public transient ExpectedException thrown = ExpectedException.none();

  @Test
  public void testLateDataAccumulating() throws IOException {
    SparkPipelineOptions options = commonOptions.withTmpCheckpointDir(checkpointParentDir);
    Pipeline p = Pipeline.create(options);
    options.setJobName(testName.getMethodName());
    Duration batchDuration = Duration.millis(options.getBatchIntervalMillis());

    Instant instant = new Instant(0);
    CreateStream<TimestampedValue<Integer>> source =
        CreateStream.<TimestampedValue<Integer>>withBatchInterval(batchDuration)
            .nextBatch()
            .advanceWatermarkForNextBatch(instant.plus(Duration.standardMinutes(6)))
            .nextBatch(
                TimestampedValue.of(1, instant),
                TimestampedValue.of(2, instant),
                TimestampedValue.of(3, instant))
            .advanceWatermarkForNextBatch(instant.plus(Duration.standardMinutes(20)))
            // These elements are late but within the allowed lateness
            .nextBatch(
                TimestampedValue.of(4, instant),
                TimestampedValue.of(5, instant))
            // These elements are droppably late
            .advanceNextBatchWatermarkToInfinity()
            .nextBatch(
                TimestampedValue.of(-1, instant),
                TimestampedValue.of(-2, instant),
                TimestampedValue.of(-3, instant));

    PCollection<Integer> windowed = p
        .apply(source).setCoder(TimestampedValue.TimestampedValueCoder.of(VarIntCoder.of()))
        .apply(ParDo.of(new OnlyValue<Integer>()))
        .apply(Window.<Integer>into(FixedWindows.of(Duration.standardMinutes(5))).triggering(
            AfterWatermark.pastEndOfWindow()
                .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane()
                    .plusDelayOf(Duration.standardMinutes(2)))
                .withLateFirings(AfterPane.elementCountAtLeast(1)))
            .accumulatingFiredPanes()
            .withAllowedLateness(Duration.standardMinutes(5), Window.ClosingBehavior.FIRE_ALWAYS));
    PCollection<Integer> triggered = windowed.apply(WithKeys.<Integer, Integer>of(1))
        .apply(GroupByKey.<Integer, Integer>create())
        .apply(Values.<Iterable<Integer>>create())
        .apply(Flatten.<Integer>iterables());
    PCollection<Long> count = windowed.apply(Count.<Integer>globally().withoutDefaults());
    PCollection<Integer> sum = windowed.apply(Sum.integersGlobally().withoutDefaults());

    IntervalWindow window = new IntervalWindow(instant, instant.plus(Duration.standardMinutes(5L)));
    PAssert.that(triggered)
        .inFinalPane(window)
        .containsInAnyOrder(1, 2, 3, 4, 5);
    PAssert.that(triggered)
        .inOnTimePane(window)
        .containsInAnyOrder(1, 2, 3);
    PAssert.that(count)
        .inWindow(window)
        .satisfies(new SerializableFunction<Iterable<Long>, Void>() {
          @Override
          public Void apply(Iterable<Long> input) {
            for (Long count : input) {
              assertThat(count, allOf(greaterThanOrEqualTo(3L), lessThanOrEqualTo(5L)));
            }
            return null;
          }
        });
    PAssert.that(sum)
        .inWindow(window)
        .satisfies(new SerializableFunction<Iterable<Integer>, Void>() {
          @Override
          public Void apply(Iterable<Integer> input) {
            for (Integer sum : input) {
              assertThat(sum, allOf(greaterThanOrEqualTo(6), lessThanOrEqualTo(15)));
            }
            return null;
          }
        });

    p.run();
  }

//  @Test
//  @Category({NeedsRunner.class, UsesTestStream.class})
//  public void testProcessingTimeTrigger() {
//    TestStream<Long> source = TestStream.create(VarLongCoder.of())
//            .addElements(TimestampedValue.of(1L, new Instant(1000L)),
//                    TimestampedValue.of(2L, new Instant(2000L)))
//            .advanceProcessingTime(Duration.standardMinutes(12))
//            .addElements(TimestampedValue.of(3L, new Instant(3000L)))
//            .advanceProcessingTime(Duration.standardMinutes(6))
//            .advanceWatermarkToInfinity();
//
//    PCollection<Long> sum = p.apply(source)
//            .apply(Window.<Long>triggering(AfterWatermark.pastEndOfWindow()
//                    .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane()
//                            .plusDelayOf(Duration.standardMinutes(5)))).accumulatingFiredPanes()
//                    .withAllowedLateness(Duration.ZERO))
//            .apply(Sum.longsGlobally());
//
//    PAssert.that(sum).inEarlyGlobalWindowPanes().containsInAnyOrder(3L, 6L);
//
//    p.run();
//  }

  @Test
  public void testDiscardingMode() throws IOException {
    SparkPipelineOptions options = commonOptions.withTmpCheckpointDir(checkpointParentDir);
    Pipeline p = Pipeline.create(options);
    options.setJobName(testName.getMethodName());
    Duration batchDuration = Duration.millis(options.getBatchIntervalMillis());

    CreateStream<TimestampedValue<String>> source =
        CreateStream.<TimestampedValue<String>>withBatchInterval(batchDuration)
            .nextBatch(
                TimestampedValue.of("firstPane", new Instant(100)),
                TimestampedValue.of("alsoFirstPane", new Instant(200)))
            .advanceWatermarkForNextBatch(new Instant(1001L))
            .nextBatch(
                TimestampedValue.of("onTimePane", new Instant(500)))
            .advanceNextBatchWatermarkToInfinity()
            .nextBatch(
                TimestampedValue.of("finalLatePane", new Instant(750)),
                TimestampedValue.of("alsoFinalLatePane", new Instant(250)));

    FixedWindows windowFn = FixedWindows.of(Duration.millis(1000L));
    Duration allowedLateness = Duration.millis(5000L);
    PCollection<String> values =
        p.apply(source).setCoder(TimestampedValue.TimestampedValueCoder.of(StringUtf8Coder.of()))
            .apply(ParDo.of(new OnlyValue<String>()))
            .apply(
                Window.<String>into(windowFn)
                    .triggering(
                        AfterWatermark.pastEndOfWindow()
                            .withEarlyFirings(AfterPane.elementCountAtLeast(2))
                            .withLateFirings(Never.ever()))
                    .discardingFiredPanes()
                    .withAllowedLateness(allowedLateness))
            .apply(WithKeys.<Integer, String>of(1))
            .apply(GroupByKey.<Integer, String>create())
            .apply(Values.<Iterable<String>>create())
            .apply(Flatten.<String>iterables());

    IntervalWindow window = windowFn.assignWindow(new Instant(100));
    PAssert.that(values)
        .inWindow(window)
        .containsInAnyOrder(
            "firstPane", "alsoFirstPane", "onTimePane", "finalLatePane", "alsoFinalLatePane");
    PAssert.that(values)
        .inCombinedNonLatePanes(window)
        .containsInAnyOrder("firstPane", "alsoFirstPane", "onTimePane");
    PAssert.that(values).inOnTimePane(window).containsInAnyOrder("onTimePane");
    PAssert.that(values)
        .inFinalPane(window)
        .containsInAnyOrder("finalLatePane", "alsoFinalLatePane");

    p.run();
  }

  @Test
  public void testFirstElementLate() throws IOException {
    SparkPipelineOptions options = commonOptions.withTmpCheckpointDir(checkpointParentDir);
    Pipeline p = Pipeline.create(options);
    options.setJobName(testName.getMethodName());
    Duration batchDuration = Duration.millis(options.getBatchIntervalMillis());

    Instant lateElementTimestamp = new Instant(-1_000_000);
    CreateStream<TimestampedValue<String>> source =
        CreateStream.<TimestampedValue<String>>withBatchInterval(batchDuration)
            .nextBatch()
            .advanceWatermarkForNextBatch(new Instant(-1_000_000))
            .nextBatch(
                TimestampedValue.of("late", lateElementTimestamp),
                TimestampedValue.of("onTime", new Instant(100)))
            .advanceNextBatchWatermarkToInfinity();

    FixedWindows windowFn = FixedWindows.of(Duration.millis(1000L));
    Duration allowedLateness = Duration.millis(5000L);
    PCollection<String> values = p.apply(source)
            .setCoder(TimestampedValue.TimestampedValueCoder.of(StringUtf8Coder.of()))
        .apply(ParDo.of(new OnlyValue<String>()))
        .apply(Window.<String>into(windowFn).triggering(DefaultTrigger.of())
            .discardingFiredPanes()
            .withAllowedLateness(allowedLateness))
        .apply(WithKeys.<Integer, String>of(1))
        .apply(GroupByKey.<Integer, String>create())
        .apply(Values.<Iterable<String>>create())
        .apply(Flatten.<String>iterables());

    //TODO: empty panes do not emmit anything so Spark won't evaluate an "empty" assertion.
//    PAssert.that(values).inWindow(windowFn.assignWindow(lateElementTimestamp)).empty();
    PAssert.that(values)
        .inWindow(windowFn.assignWindow(new Instant(100)))
        .containsInAnyOrder("onTime");

    p.run();
  }

  @Test
  public void testElementsAtAlmostPositiveInfinity() throws IOException {
    SparkPipelineOptions options = commonOptions.withTmpCheckpointDir(checkpointParentDir);
    Pipeline p = Pipeline.create(options);
    options.setJobName(testName.getMethodName());
    Duration batchDuration = Duration.millis(options.getBatchIntervalMillis());

    Instant endOfGlobalWindow = GlobalWindow.INSTANCE.maxTimestamp();
    CreateStream<TimestampedValue<String>> source =
        CreateStream.<TimestampedValue<String>>withBatchInterval(batchDuration)
            .nextBatch(
                TimestampedValue.of("foo", endOfGlobalWindow),
                TimestampedValue.of("bar", endOfGlobalWindow))
            .advanceNextBatchWatermarkToInfinity();

    FixedWindows windows = FixedWindows.of(Duration.standardHours(6));
    PCollection<String> windowedValues = p.apply(source)
            .setCoder(TimestampedValue.TimestampedValueCoder.of(StringUtf8Coder.of()))
        .apply(ParDo.of(new OnlyValue<String>()))
        .apply(Window.<String>into(windows))
        .apply(WithKeys.<Integer, String>of(1))
        .apply(GroupByKey.<Integer, String>create())
        .apply(Values.<Iterable<String>>create())
        .apply(Flatten.<String>iterables());

    PAssert.that(windowedValues)
        .inWindow(windows.assignWindow(GlobalWindow.INSTANCE.maxTimestamp()))
        .containsInAnyOrder("foo", "bar");
    p.run();
  }

//  @Test
//  public void testMultipleStreams() throws IOException {
//    SparkPipelineOptions options = commonOptions.withTmpCheckpointDir(checkpointParentDir);
//    Pipeline p = Pipeline.create(options);
//    options.setJobName(testName.getMethodName());
//    Duration batchDuration = Duration.millis(options.getBatchIntervalMillis());
//
//    CreateStream<String> source =
//        CreateStream.<String>withBatchInterval(batchDuration)
//            .nextBatch("foo", "bar").advanceWatermarkForNextBatch(new Instant(100))
//            .nextBatch().advanceNextBatchWatermarkToInfinity();
//
////    CreateStream<Integer> other =
////        CreateStream.<Integer>withBatchInterval(batchDuration)
////            .nextBatch(1, 2, 3, 4)
////            .advanceNextBatchWatermarkToInfinity();
//
//    PCollection<String> createStrings =
//        p.apply("CreateStrings", source).setCoder(StringUtf8Coder.of())
//            .apply("WindowStrings",
//                Window.<String>triggering(AfterPane.elementCountAtLeast(2))
//                    .withAllowedLateness(Duration.ZERO)
//                    .accumulatingFiredPanes());
//    PAssert.that(createStrings).containsInAnyOrder("foo", "bar");
////    PCollection<Integer> createInts =
////        p.apply("CreateInts", other).setCoder(VarIntCoder.of())
////            .apply("WindowInts",
////                Window.<Integer>triggering(AfterPane.elementCountAtLeast(4))
////                    .withAllowedLateness(Duration.ZERO)
////                    .accumulatingFiredPanes());
////    PAssert.that(createInts).containsInAnyOrder(1, 2, 3, 4);
//
//    p.run();
//  }

  @Test
  public void testElementAtPositiveInfinityThrows() {
    Duration batchDuration = Duration.millis(commonOptions.getOptions().getBatchIntervalMillis());
    CreateStream<TimestampedValue<Integer>> source =
        CreateStream.<TimestampedValue<Integer>>withBatchInterval(batchDuration)
            .nextBatch(TimestampedValue.of(-1, BoundedWindow.TIMESTAMP_MAX_VALUE.minus(1L)));
    thrown.expect(IllegalArgumentException.class);
    source.nextBatch(TimestampedValue.of(1, BoundedWindow.TIMESTAMP_MAX_VALUE));
  }

  @Test
  public void testAdvanceWatermarkNonMonotonicThrows() {
    Duration batchDuration = Duration.millis(commonOptions.getOptions().getBatchIntervalMillis());
    CreateStream<Integer> source =
        CreateStream.<Integer>withBatchInterval(batchDuration)
            .advanceWatermarkForNextBatch(new Instant(0L));
    thrown.expect(IllegalArgumentException.class);
    source.advanceWatermarkForNextBatch(new Instant(-1L));
  }

  @Test
  public void testAdvanceWatermarkEqualToPositiveInfinityThrows() {
    Duration batchDuration = Duration.millis(commonOptions.getOptions().getBatchIntervalMillis());
    CreateStream<Integer> source =
        CreateStream.<Integer>withBatchInterval(batchDuration)
            .advanceWatermarkForNextBatch(BoundedWindow.TIMESTAMP_MAX_VALUE.minus(1L));
    thrown.expect(IllegalArgumentException.class);
    source.advanceWatermarkForNextBatch(BoundedWindow.TIMESTAMP_MAX_VALUE);
  }

  private static class OnlyValue<T> extends DoFn<TimestampedValue<T>, T> {

    OnlyValue() { }

    @ProcessElement
    public void onlyValue(ProcessContext c) {
      c.outputWithTimestamp(c.element().getValue(), c.element().getTimestamp());
    }
  }
}
