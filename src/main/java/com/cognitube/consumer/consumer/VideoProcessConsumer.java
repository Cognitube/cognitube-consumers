package com.cognitube.consumer.consumer;

import com.cognitube.consumer.consumer.dao.VideoUploadMessage;
import com.cognitube.consumer.producer.VideoProcessProducer;
import com.cognitube.consumer.service.VideoProcessingService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.io.File;

/**
 * @author Yijing Yang
 * @version 1.0
 * @project cognitube-backend
 * @description Consumer for video processing
 * @date 2024/7/8 20:20:20
 */
@Slf4j
@Component
@AllArgsConstructor
public class VideoProcessConsumer {

    private final ObjectMapper objectMapper;
    private final VideoProcessProducer videoProcessProducer;
    private final String KAFKA_VIDEO_PROCESS_TOPIC;
    private final VideoProcessingService videoProcessingService;

    @KafkaListener(topics = "${kafka.video.process.topic}", groupId = "${kafka.video.process.group.id}")
    public void consumeProcessVideoMessage(ConsumerRecord<String, String> record) {
        process(record);
    }

    private void process(ConsumerRecord<String, String> record) {
        VideoUploadMessage message = null;

        try {
            message = objectMapper.readValue(record.value(), VideoUploadMessage.class);
        } catch (JsonProcessingException e) {
            log.error("Failed to deserialize video process message", e);
            return;
        }

        MDC.put("videoId", message.getVideoId());
        MDC.put("userId", message.getUserId().toString());
        MDC.put("processingTime", System.currentTimeMillis() + "");

        try {
            if (videoProcessingService.isVideoProcessedOrProcessing(message.getVideoId(), message.getRetryCount())) {
                return;
            }

            log.info("Start processing video: {}", message.getVideoId());

            videoProcessingService.recordVideoProcessingStatus(
                    message.getVideoId(),
                    message.getUserId(),
                    message.getVideoName(),
                    message.getRetryCount(),
                    message.getVideoUrl()
            );

            videoProcessingService.createVideoTranscodingJob(message.getVideoUrl(), message.getVideoId(), 0);
            videoProcessingService.createAudioExtractionJob(message.getVideoUrl(), message.getVideoId(), 0);
        } catch (Exception e) {
            //TODO: specify more exception types
            log.error("Failed to process video", e);
            if (message.getRetryCount() > 3) {
                log.error("Failed to process video after 3 retries. Terminating processing for video.");
                videoProcessingService.markVideoProcessingStatusAsFailed(message.getVideoId());
            } else {
                message.setRetryCount(message.getRetryCount() + 1);
                final VideoUploadMessage finalMessage = message;
                videoProcessProducer.sendKafkaMessageAsync(message, KAFKA_VIDEO_PROCESS_TOPIC, (metadata, exception) -> {
                    if (exception != null) {
                        videoProcessingService.markVideoProcessingStatusAsFailed(finalMessage.getVideoId());
                        throw new RuntimeException("Failed to send video upload message", exception);
                    } else {
                        log.info("Video upload message sent successfully. Retry count: {}", finalMessage.getRetryCount());
                    }
                });
            }
        }
    }
}

