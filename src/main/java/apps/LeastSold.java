package apps;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LeastSold {

    private static String getPayload(String schemaValue)
    {
        Pattern payloadPattern = Pattern.compile("\"payload\":\"([^\"]+)\"");
        Matcher matcher = payloadPattern.matcher(schemaValue);

        // Get payload
        return matcher.find() ? matcher.group(1) : "Store 2|Banana|3";
    }

    public static void main(final String[] args) {

        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-least-sold-view");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        final StreamsBuilder builder = new StreamsBuilder();

        // Create a stream from the source (input text file)
        final KStream<String, String> sales = builder.<String, String>stream("streams-sales-input", Consumed.with(Serdes.String(), Serdes.String()));

        // Parse the JSON strings and extract the payload
        final KStream<String, String> payload = sales.mapValues((value) -> {
            // Read payload from schema
            if(value.contains("schema")) return LeastSold.getPayload(value);

            // Return value only
            return value;
        });

        // Split the text line into store name, product name, and quantity
        KStream<String, String> products = payload.map((key, value) -> {
            String[] tokens = value.split("\\|");
            String store = tokens[0];
            String product = tokens[1];
            String quantity = tokens[2];
            return KeyValue.pair(store, product + "|" + quantity);
        });

        // Group by the key and reduce by getting the least quantity
        KTable<String, String> leastSold = products
                .groupByKey()
                .reduce(
                    (v1, v2) -> {
                        try {
                            String product1 = v1.split("\\|")[0];
                            double quantity1 = Double.parseDouble(v1.split("\\|")[1]);

                            String product2 = v2.split("\\|")[0];
                            double quantity2 = Double.parseDouble(v2.split("\\|")[1]);

                            return (quantity1 < quantity2) ? v1: v2;
                        } catch(Exception e) {
                            return v1;
                        }
                    }
                ).mapValues((key, value) -> key + "|" + value);

        // Write the result to the output topic
        leastSold.toStream().to("streams-least-sales-output", Produced.with(Serdes.String(), Serdes.String()));

        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
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
        } catch (final Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
