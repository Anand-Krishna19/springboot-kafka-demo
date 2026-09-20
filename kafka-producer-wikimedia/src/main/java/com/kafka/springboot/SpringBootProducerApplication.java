package com.kafka.springboot;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SpringBootProducerApplication implements CommandLineRunner {

    private final WikiChangesProducer wikiChangesProducer;

    public SpringBootProducerApplication(WikiChangesProducer wikiChangesProducer) {
        this.wikiChangesProducer = wikiChangesProducer;
    }

    public static void main(String[] args) {
        SpringApplication.run(SpringBootProducerApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception{
        wikiChangesProducer.sendMessage();
    }
}
