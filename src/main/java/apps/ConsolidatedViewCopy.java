/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package apps;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsolidatedViewCopy {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-consolidated-view");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder builder = new StreamsBuilder();

        // Create a stream from the input text file
        KStream<String, String> sales = builder.<String, String>stream("streams-sales-input", Consumed.with(Serdes.String(), Serdes.String()));

        // Split the text line into store name, product name, and quantity
        KStream<String, Double> products = sales.map((key, value) -> {
            String[] tokens = value.split("\\|");
            String store = tokens[0];
            String product = tokens[1];
            double quantity = Double.parseDouble(tokens[2]);
            return KeyValue.pair(store + "|" + product, quantity);
        });

        products.foreach((key, value) -> System.out.println("\n\n\nKey: " + key + ", Value: " + value));

        // Group by the key and reduce by summing the quantities
        KTable<String, Double> productTotals = products
                .groupByKey()
                .reduce((a, b) -> a + b, Materialized.with(Serdes.String(), Serdes.Double()));

        // Change the key to the product name only
        KTable<String, Double> productTotalsByProduct = productTotals.toStream()
                .map((key, value) -> {
                    String[] tokens = key.split("\\|");
                    String product = tokens[1];
                    return KeyValue.pair(product, value);
                })
                .groupByKey()
                .reduce((a, b) -> a + b, Materialized.with(Serdes.String(), Serdes.Double()));

        productTotalsByProduct.toStream().foreach((key, value) -> System.out.println("\n\n\nKey: " + key + ", Value: " + value));


        // Convert to string
        // KTable<String, String> consolidatedOutput =  productTotalsByProduct.mapValues(String::valueOf);

        // Write the result to the output topic
        productTotalsByProduct.toStream().to("streams-consolidate-sales-output", Produced.with(Serdes.String(), Serdes.Double()));

        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}


