package com.cognitube.consumer.producer;

import com.cognitube.consumer.consumer.dao.VideoUploadMessage;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

/**
 * @author Haozhe Zhang
 * @version 1.0
 * @project cognitube-consumers
 * @description Add new message for video process in case of processing failure
 * @date 2024/7/17 08:55:00
 */
@Slf4j
@Component
public class VideoProcessProducer {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    @Autowired
    private ObjectMapper objectMapper;

    public void sendMessage(String message, String topicName) {
        try {
            kafkaTemplate.send(topicName, message).get();
            log.info("Sent message: {}", message);
        } catch (Exception ex) {
            log.error("Error sending message: {}", message, ex);
        }
    }

    private void sendMessageAsync(String message, String topicName, Callback callback) {
        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topicName, message);
        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Error sending message asynchronously: {}", message, ex);
                if (callback != null) {
                    callback.onCompletion(null, ex instanceof Exception ? (Exception) ex : new RuntimeException(ex));
                }
            } else {
                log.info("Sent message asynchronously: {}", message);
                if (callback != null) {
                    callback.onCompletion(result.getRecordMetadata(), null);
                }
            }
        });
    }

    public void sendVideoUploadMessageAsync(VideoUploadMessage message, String topicName, Callback callback) {
        try {
            String jsonMessage = objectMapper.writeValueAsString(message);
            sendMessageAsync(jsonMessage, topicName, callback);
        } catch (JsonProcessingException ex) {
            log.error("Error serializing message: {}", message, ex);
        }
    }
}
