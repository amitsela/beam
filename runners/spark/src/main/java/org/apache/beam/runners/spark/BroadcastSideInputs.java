package org.apache.beam.runners.spark;

import com.google.common.collect.Maps;

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.runners.spark.util.BroadcastHelper;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.spark.api.java.JavaSparkContext;


/**
 * For resilience, {@link org.apache.spark.broadcast.Broadcast}s are required to be wrapped
 * in a Singleton.
 * For more details see: https://spark.apache.org/docs/1.6.2/streaming-programming-guide.html#accumulators-and-broadcast-variables
 */
public class BroadcastSideInputs {

  //TODO: tags are per transform, this holder holds all.
  private static volatile Map<TupleTag<?>, BroadcastHelper<?>> sideInputs = null;

  public static synchronized Map<TupleTag<?>, BroadcastHelper<?>>
  getOrCreateBroadcast(JavaSparkContext jsc, Map<TupleTag<?>, BroadcastHelper<?>> providedInputs) {
    if (sideInputs == null) {
      sideInputs = new HashMap<>(providedInputs);
    } else {
      for (Map.Entry<TupleTag<?>, BroadcastHelper<?>> en: providedInputs.entrySet()) {
        if (!sideInputs.containsKey(en.getKey())) {
          sideInputs.put(en.getKey(), en.getValue());
        }
      }
    }
    Map<TupleTag<?>, BroadcastHelper<?>> res =
        Maps.newHashMapWithExpectedSize(providedInputs.size());
    for (Map.Entry<TupleTag<?>, BroadcastHelper<?>> en: providedInputs.entrySet()) {
      BroadcastHelper<?> broadcastHelper = sideInputs.get(en.getKey());
      if (!broadcastHelper.isBroadcasted()) {
        broadcastHelper.broadcast(jsc);
      }
      res.put(en.getKey(), broadcastHelper);
    }
    return res;
  }

  /** For testing only. */
  public static void clear() {
    synchronized (BroadcastSideInputs.class) {
      for (BroadcastHelper<?> helper: sideInputs.values()) {
        helper.unregister();
      }
    }
  }
}
