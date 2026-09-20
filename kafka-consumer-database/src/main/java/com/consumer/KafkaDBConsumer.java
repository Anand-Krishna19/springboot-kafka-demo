package com.consumer;

import com.consumer.entity.WikimediaData;
import com.consumer.repo.WikimediaDataRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaDBConsumer {

    public static final Logger LOGGER = LoggerFactory.getLogger(KafkaDBConsumer.class);

    private WikimediaDataRepository wikimediaDataRepository;

    public KafkaDBConsumer(WikimediaDataRepository wikimediaDataRepository){
        this.wikimediaDataRepository = wikimediaDataRepository;
    }

    @KafkaListener(topics = "wikimedia_topic", groupId = "my-consumer-group-id")
    public void consume(String eventMessage){
        LOGGER.info(String.format("Event Message received -> %s",eventMessage));

        WikimediaData data = new WikimediaData();
        data.setWikiEventData(eventMessage);

        wikimediaDataRepository.save(data);
    }
}
