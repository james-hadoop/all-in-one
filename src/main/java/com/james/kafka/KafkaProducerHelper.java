package com.james.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaProducerHelper {

    private static Logger logger = LoggerFactory.getLogger(KafkaProducerHelper.class);
    private Producer<String, String> producer;

    private String bootstrapServers;
    private String acks;
    private int retries;
    private int batchSize;

    private int lingerMs;
    private int bufferMemory;
    private String keySerializer;
    private String valueSerializer;

    // use default configs
    public KafkaProducerHelper(String brokers) {
        logger.info("kafka broker list : " + brokers);
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("acks", "0");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<String, String>(props);
    }

    public KafkaProducerHelper() {
    }

    public void initProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", this.getBootstrapServers());
        props.put("acks", this.getAcks());
        props.put("retries", this.getRetries());
        props.put("batch.size", this.getBatchSize());
        props.put("linger.ms", this.getLingerMs());
        props.put("buffer.memory", this.getBufferMemory());
        props.put("key.serializer", this.getKeySerializer());
        props.put("value.serializer", this.getValueSerializer());

        logger.info("kafka broker list : " + this.getBootstrapServers());
        producer = new KafkaProducer<String, String>(props);
    }

    public void produce(String topic, String message) throws Exception {
        producer.send(new ProducerRecord<String, String>(topic, message));
    }

    public void close() {
        producer.close();
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public String getAcks() {
        return acks;
    }

    public void setAcks(String acks) {
        this.acks = acks;
    }

    public int getRetries() {
        return retries;
    }

    public void setRetries(int retries) {
        this.retries = retries;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getLingerMs() {
        return lingerMs;
    }

    public void setLingerMs(int lingerMs) {
        this.lingerMs = lingerMs;
    }

    public int getBufferMemory() {
        return bufferMemory;
    }

    public void setBufferMemory(int bufferMemory) {
        this.bufferMemory = bufferMemory;
    }

    public String getKeySerializer() {
        return keySerializer;
    }

    public void setKeySerializer(String keySerializer) {
        this.keySerializer = keySerializer;
    }

    public String getValueSerializer() {
        return valueSerializer;
    }

    public void setValueSerializer(String valueSerializer) {
        this.valueSerializer = valueSerializer;
    }
}