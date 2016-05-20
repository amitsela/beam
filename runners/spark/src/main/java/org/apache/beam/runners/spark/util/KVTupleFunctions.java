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

package org.apache.beam.runners.spark.util;

import org.apache.beam.sdk.values.KV;
import org.apache.spark.api.java.function.MapFunction;

import scala.Tuple2;

/**
 * KV-Tuple2 utility class.
 */
public class KVTupleFunctions {

  public static <K, V> MapFunction<KV<K, V>, Tuple2<K, V>> kVToTuple2() {
    return new MapFunction<KV<K, V>, Tuple2<K, V>>() {
      @Override
      public Tuple2<K, V> call(KV<K, V> kv) throws Exception {
        return new Tuple2<>(kv.getKey(), kv.getValue());
      }
    };
  }

  public static <K, V> MapFunction<Tuple2<K, V>, KV<K, V>> tuple2ToKV() {
    return new MapFunction<Tuple2<K, V>, KV<K, V>>() {
      @Override
      public KV<K, V> call(Tuple2<K, V> tuple2) throws Exception {
        return KV.of(tuple2._1(), tuple2._2());
      }
    };
  }

  public static <K, V> MapFunction<Tuple2<K, V>, K> tuple2GetFirst() {
    return new MapFunction<Tuple2<K, V>, K>() {
      @Override
      public K call(Tuple2<K, V> tuple2) throws Exception {
        return tuple2._1();
      }
    };
  }
}
