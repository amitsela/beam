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
package org.apache.beam.runners.spark.translation;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.beam.runners.spark.coders.CoderHelpers;
import org.apache.beam.runners.spark.util.SideInputBroadcast;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.spark.api.java.JavaSparkContext;


/**
 * SparkPCollectionView is used to pass serialized views to lambdas.
 */
public class SparkPCollectionView {

    private static volatile Map<PCollectionView<?>,
        SideInputBroadcast<?>> broadcastHelperMap = new LinkedHashMap<>();

    public static <ElemT, ViewT> void putView(
        PCollectionView<ViewT> view,
        Iterable<WindowedValue<ElemT>> value,
        Coder<Iterable<WindowedValue<ElemT>>> coder,
        JavaSparkContext sparkContext) {
        synchronized (SparkPCollectionView.class) {
            SideInputBroadcast<?> helper = broadcastHelperMap.get(view);
            if (helper != null) {
                // clear the current view if exists.
                helper.unpersist();
                broadcastHelperMap.remove(view);
            }
            // broadcast the updated view.
            broadcastHelperMap.put(
                view,
                createBroadcastHelper(CoderHelpers.toByteArray(value, coder), coder, sparkContext));
        }
    }

    public static SideInputBroadcast get(PCollectionView<?> view) {
        return checkNotNull(broadcastHelperMap.get(view));
    }

    private static <T> SideInputBroadcast createBroadcastHelper(
        byte[] bytes,
        Coder<Iterable<WindowedValue<T>>> coder,
        JavaSparkContext context) {
        SideInputBroadcast helper = SideInputBroadcast.create(bytes, coder);
        helper.broadcast(context);
        return helper;
    }
}
