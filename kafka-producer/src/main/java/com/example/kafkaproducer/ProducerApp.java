package com.example.kafkaproducer;

import com.example.kafkaproducer.avro.Instrument;
import com.example.kafkaproducer.avro.Trade;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public final class ProducerApp {
    private ProducerApp() {
    }

    public static void main(String[] args) {
        String bootstrapServers = envOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        String topic = envOrDefault("KAFKA_TOPIC", "trades");
        String tradeId = envOrDefault("KAFKA_TRADE_ID", "trade-1");
        String instrumentId = envOrDefault("KAFKA_INSTRUMENT_ID", "inst-1");
        String instrumentName = envOrDefault("KAFKA_INSTRUMENT_NAME", "Example Corp");
        String instrumentSymbol = envOrDefault("KAFKA_INSTRUMENT_SYMBOL", "EXM");
        long quantity = envOrDefaultLong("KAFKA_TRADE_QUANTITY", 100L);
        double price = envOrDefaultDouble("KAFKA_TRADE_PRICE", 125.75);
        Trade.Side side = Trade.Side.valueOf(envOrDefault("KAFKA_TRADE_SIDE", "BUY"));
        long timestamp = envOrDefaultLong("KAFKA_TRADE_TIMESTAMP", Instant.now().toEpochMilli());

        Instrument instrument = Instrument.newBuilder()
                .setId(instrumentId)
                .setName(instrumentName)
                .setSymbol(instrumentSymbol)
                .build();

        Trade trade = Trade.newBuilder()
                .setId(tradeId)
                .setInstrument(instrument)
                .setQuantity(quantity)
                .setPrice(price)
                .setSide(side)
                .setTimestamp(timestamp)
                .build();

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "event-sourcing-producer");

        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(properties)) {
            byte[] payload = serializeTrade(trade);
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, tradeId, payload);
            RecordMetadata metadata = producer.send(record).get();
            System.out.printf("Trade sent to %s partition %d with offset %d%n",
                    metadata.topic(),
                    metadata.partition(),
                    metadata.offset());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Producer interrupted: " + e.getMessage());
        } catch (ExecutionException | IOException e) {
            System.err.println("Failed to send trade: " + e.getMessage());
        }
    }

    private static byte[] serializeTrade(Trade trade) throws IOException {
        SpecificDatumWriter<Trade> writer = new SpecificDatumWriter<>(Trade.class);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
        writer.write(trade, encoder);
        encoder.flush();
        return outputStream.toByteArray();
    }

    private static String envOrDefault(String key, String defaultValue) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? defaultValue : value;
    }

    private static long envOrDefaultLong(String key, long defaultValue) {
        String value = System.getenv(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private static double envOrDefaultDouble(String key, double defaultValue) {
        String value = System.getenv(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }
}
