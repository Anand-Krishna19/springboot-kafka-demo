package com.kafka.springboot;

import com.launchdarkly.eventsource.MessageEvent;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;



public class WikiChangesHandler implements BackgroundEventHandler {

    public static final Logger LOGGER = LoggerFactory.getLogger(WikiChangesHandler.class);

    private KafkaTemplate<String, String> kafkaTemplate;
    private String topic;

    public WikiChangesHandler(KafkaTemplate<String, String> kafkaTemplate, String topic){
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    @Override
    public void onOpen() throws Exception {
        LOGGER.info("Connected to Wikimedia recent changes stream");
    }

    @Override
    public void onClosed() throws Exception {
        LOGGER.info("Wikimedia recent changes stream closed");
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) throws Exception {
        LOGGER.info(String.format("event data -> %s", messageEvent.getData()));

        kafkaTemplate.send(topic, messageEvent.getData())
                .whenComplete((result, exception) -> {
                    if (exception != null) {
                        LOGGER.error("Failed to publish Wikimedia event to Kafka", exception);
                    }
                });
    }

    @Override
    public void onComment(String comment) throws Exception {

    }

    @Override
    public void onError(Throwable t) {
        LOGGER.error("Error reading Wikimedia recent changes stream", t);
    }
}
