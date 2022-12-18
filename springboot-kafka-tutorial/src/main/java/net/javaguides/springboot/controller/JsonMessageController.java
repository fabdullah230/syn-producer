package net.javaguides.springboot.controller;

import net.javaguides.springboot.kafka.JsonKafkaProducer;
import net.javaguides.springboot.payload.Transaction;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/json")
public class JsonMessageController {

    private JsonKafkaProducer kafkaProducer;

    public JsonMessageController(JsonKafkaProducer kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    @PostMapping("/publish")
    public ResponseEntity<String> publish(@RequestBody Transaction transaction){
        kafkaProducer.sendMessage(transaction);
        return ResponseEntity.ok("Json transaction sent to kafka topic");
    }
}
