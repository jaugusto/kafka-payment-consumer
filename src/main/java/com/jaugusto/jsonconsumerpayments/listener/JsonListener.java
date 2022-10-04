package com.jaugusto.jsonconsumerpayments.listener;

import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;


@Log4j2
@Component
public class JsonListener {

    @SneakyThrows
    @KafkaListener(topics = "payment-topic", groupId = "create-group", containerFactory = "jsonContainerFactory")
    public void fraudDetector(){
        log.info("Payment received");
        Thread.sleep(1000);
        log.info("checking frauds....");
        Thread.sleep(1000);
        log.info("Congrats Approved!");
        Thread.sleep(1000);
    }

    @SneakyThrows
    @KafkaListener(topics = "payment-topic", groupId = "pdf-group", containerFactory = "jsonContainerFactory")
    public void pdfGenerator(){
        log.info("creating pdf...");
        Thread.sleep(1000);
    }

    @SneakyThrows
    @KafkaListener(topics = "payment-topic", groupId = "email-group", containerFactory = "jsonContainerFactory")
    public void sendingEmail(){
        log.info("sending email...");
    }

}
