package com.cognitube.consumer.consumer;

import com.cognitube.consumer.consumer.dao.VideoKeywordFetchMessage;
import com.cognitube.consumer.model.response.AzureTranscriptionResponse;
import com.cognitube.consumer.producer.VideoProcessProducer;
import com.cognitube.consumer.service.VideoProcessingService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Instant;

/**
 * @author Haozhe Zhang
 * @version 1.0
 * @project cognitube-consumers
 * @description
 * @date 2024/9/16 18:49:35
 */
@AllArgsConstructor
@Slf4j
@Component
public class VideoKeywordFetchConsumer {

    private final ObjectMapper objectMapper;
    private final VideoProcessProducer videoProcessProducer;
    private final String KAFKA_VIDEO_KEYWORD_FETCH_TOPIC;
    private final VideoProcessingService videoProcessingService;

    @KafkaListener(topics = "${kafka.video.keyword.fetch.topic}", groupId = "${kafka.video.process.group.id}")
    public void consumeProcessVideoMessage(ConsumerRecord<String, String> record) {
        process(record);
    }

    private void process(ConsumerRecord<String, String> record) {
        VideoKeywordFetchMessage message = null;

        try {
            message = objectMapper.readValue(record.value(), VideoKeywordFetchMessage.class);
        } catch (JsonProcessingException e) {
            log.error("Failed to deserialize video keyword fetch message", e);
            return;
        }

        try {
            final long currentTimestamp = Instant.now(Clock.systemUTC()).getEpochSecond();
            if (currentTimestamp < message.getRetryTime()) {
                final VideoKeywordFetchMessage finalMessage = message;
                videoProcessProducer.sendKafkaMessageAsync(message, KAFKA_VIDEO_KEYWORD_FETCH_TOPIC,  (metadata, exception) -> {
                    if (exception != null) {
                        videoProcessingService.markVideoProcessingStatusAsFailed(finalMessage.getVideoId());
                        throw new RuntimeException("Failed to send video upload message", exception);
                    }
                });

                return;
            }
            log.info("Start fetching keywords for video: {}", message.getVideoId());

            // Fetch keywords for video
            AzureTranscriptionResponse azureTranscriptionResponse = videoProcessingService.getTranscriptionResponse(message.getId());
            final String videoId = message.getVideoId();
            switch (azureTranscriptionResponse.getStatus()) {
                case "Succeeded":
                    videoProcessingService.processTranscriptionResult(message.getId());
                    break;
                case "Running":
                    videoProcessProducer.sendKafkaMessageAsync(message, KAFKA_VIDEO_KEYWORD_FETCH_TOPIC, (metadata, exception) -> {
                        if (exception != null) {
                            videoProcessingService.markVideoProcessingStatusAsFailed(videoId);
                            throw new RuntimeException("Failed to send video upload message", exception);
                        }
                    });
                    break;
                default:
                    log.error("Transcription job failed for video: {}", videoId);
                    break;
            }

            log.info("Finish fetching keywords for video: {}", message.getVideoId());
        } catch (Exception e) {
            log.error("Failed to fetch keywords for video: {}", message.getVideoId(), e);
        }
    }
}
