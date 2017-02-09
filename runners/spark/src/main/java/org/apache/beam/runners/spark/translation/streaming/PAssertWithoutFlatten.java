package org.apache.beam.runners.spark.translation.streaming;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.isIn;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;


/**
 * A custom PAssert that avoids using {@link org.apache.beam.sdk.transforms.Flatten}
 * until BEAM-1444 is resolved.
 */
public class PAssertWithoutFlatten extends PTransform<PCollection<Long>, PDone> {

  @Override
  public PDone expand(PCollection<Long> input) {
    input.apply(ParDo.of(new AssertDoFn(4L, 5L)));
    return PDone.in(input.getPipeline());
  }

  private static class AssertDoFn extends DoFn<Long, Void> {
    private final Aggregator<Integer, Integer> success =
        createAggregator(PAssert.SUCCESS_COUNTER, Sum.ofIntegers());
    private final Aggregator<Integer, Integer> failure =
        createAggregator(PAssert.FAILURE_COUNTER, Sum.ofIntegers());
    private final Long[] possibleResults;

    AssertDoFn(Long... possibleResults) {
      this.possibleResults = possibleResults;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      try {
        assertThat(c.element(), isIn(possibleResults));
        success.addValue(1);
      } catch (Throwable t) {
        failure.addValue(1);
        throw t;
      }
    }
  }
}
