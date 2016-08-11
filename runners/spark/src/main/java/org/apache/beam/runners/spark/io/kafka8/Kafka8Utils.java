package org.apache.beam.runners.spark.io.kafka8;

import com.google.api.client.util.Maps;

import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.joda.time.Instant;

import java.util.Map;

/**
 * Created by ansela on 8/11/16.
 */
public class Kafka8Utils {

  /**
   * Clear out all configurations that do not match a value of type String as they will not match
   * the Spark streaming Kafka API. See {@link KafkaUtils#createDirectStream}.
   */
  public static Map<String, String> toKafkaParams(Map<String, Object> consumerConfig09) {
    Map<String, String> params = Maps.newHashMap();
    for (Map.Entry<String, Object> en: consumerConfig09.entrySet()) {
      if (en.getValue() instanceof String) {
        params.put(en.getKey(), en.getValue().toString());
      }
    }
    return params;
  }

  /**
   * Wrap the input with a dummy {@link KafkaRecord} so that the supplied timestampFn
   * from {@link KafkaIO} could be used.
   */
  public static <K, V> SerializableFunction<KV<K, V>, Instant>
  wrapKafkaRecordAndThen(final SerializableFunction<KafkaRecord<K, V>, Instant> fn) {
    return new SerializableFunction<KV<K, V>, Instant>() {
      @Override
      public Instant apply(KV<K, V> input) {
        // wrap the KV with KafkaRecord just as an adapter.
        // Use values that will fail if used - they shouldn't be used because the KafkaIO is
        // the one that adds the Kafka coordinates (topic, partition, offset) to the user defined
        // time extractor.
        return fn == null ? Instant.now() : fn.apply(new KafkaRecord<K, V>(null, -1, -1, input));
      }
    };
  }

}
