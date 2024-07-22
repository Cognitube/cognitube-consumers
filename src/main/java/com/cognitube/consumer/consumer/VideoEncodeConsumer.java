package com.cognitube.consumer.consumer;

import com.cognitube.consumer.consumer.dao.VideoEncodeMessage;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.io.File;

@Slf4j
@Component
@AllArgsConstructor
public class VideoEncodeConsumer {

    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "${kafka.video.reencode.topic}", groupId = "${kafka.video.process.group.id}")
    public void consumeReencodeMessage(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        process(record, acknowledgment);
    }

    private void process(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        acknowledgment.acknowledge();

        try {
            VideoEncodeMessage message = objectMapper.readValue(record.value(), VideoEncodeMessage.class);
        } catch (JsonProcessingException e) {
            log.error("Failed to deserialize video reencode message", e);
            return;
        }
    }

    private File reencodeVideo(File videoFile) {
        // reencode video
        return null;
    }
}
