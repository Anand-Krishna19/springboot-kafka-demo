package com.kafka.springboot;

import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.ConnectStrategy;
import com.launchdarkly.eventsource.StreamException;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.util.concurrent.TimeUnit;


@Service
public class WikiChangesProducer {

    public static final Logger LOGGER = LoggerFactory.getLogger(WikiChangesProducer.class);

    private KafkaTemplate<String, String> kafkaTemplate;

    public WikiChangesProducer(KafkaTemplate<String, String> kafkaTemplate){
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage() throws InterruptedException, StreamException {
         String topic = "wikimedia_topic";

        BackgroundEventHandler eventHandler = new WikiChangesHandler(kafkaTemplate, topic);
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";

        BackgroundEventSource.Builder builder = new BackgroundEventSource.Builder(
                eventHandler,
                new EventSource.Builder(
                        ConnectStrategy.http(URI.create(url))
                                .header("User-Agent", "springboot-kafka-demo/1.0")
                )
        );
        BackgroundEventSource eventSource = builder.build();
        eventSource.start();

        TimeUnit.MINUTES.sleep(10);
    }
}
