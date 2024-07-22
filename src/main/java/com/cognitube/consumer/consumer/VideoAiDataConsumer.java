package com.cognitube.consumer.consumer;

import com.cognitube.consumer.consumer.dao.VideoAiDataMessage;
import com.cognitube.consumer.enums.VideoStatus;
import com.cognitube.consumer.mapper.VideoMapper;
import com.cognitube.consumer.model.Video;
import com.cognitube.consumer.producer.VideoProcessProducer;
import com.cognitube.consumer.service.VideoProcessingService;
import com.cognitube.consumer.util.RedisKeys;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@AllArgsConstructor
public class VideoAiDataConsumer {

    private final ObjectMapper objectMapper;
    private final VideoMapper videoMapper;
    private final RedisTemplate<String, String> redisTemplate;
    private final String KAFKA_VIDEO_AI_TOPIC;
    private final VideoProcessProducer videoProcessProducer;
    private final VideoProcessingService videoProcessingService;

    @KafkaListener(topics = "${kafka.video.ai.topic}", groupId = "${kafka.video.process.group.id}")
    public void consumeAiDataMessage(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        process(record, acknowledgment);
    }

    private void process(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        acknowledgment.acknowledge();
        VideoAiDataMessage message;

        try {
            message = objectMapper.readValue(record.value(), VideoAiDataMessage.class);
        } catch (JsonProcessingException e) {
            log.error("Failed to deserialize video ai data message", e);
            return;
        }

        try {
            if (videoProcessingService.isVideoProcessingStepDone(message.getVideoId(), KAFKA_VIDEO_AI_TOPIC)) {
                return;
            }

            final String videoId = message.getVideoId();
            final String keywordsUrl = message.getKeywordsUrl();
            final String transcriptUrl = message.getTranscriptUrl();
            final Video video = Video.builder()
                    .setId(videoId).
                    setTranscriptLink(transcriptUrl).
                    setKeywordsLink(keywordsUrl)
                    .build();

            videoMapper.updateVideo(video);
            log.info("Updated video ai data for video.");

            videoProcessingService.recordVideoProcessingStatus(videoId, KAFKA_VIDEO_AI_TOPIC);
            videoProcessingService.updateVideoStatusIfDone(videoId);
        } catch (Exception e) {
            log.error("Failed to process video ai data", e);
            if (message.getRetryCount() > 3) {
                log.error("Failed to process video ai data after 3 retries. Terminating processing for video ai data.");
            } else {
                message.setRetryCount(message.getRetryCount() + 1);
                final VideoAiDataMessage finalMessage = message;
                videoProcessProducer.sendKafkaMessageAsync(message, KAFKA_VIDEO_AI_TOPIC, (metadata, exception) -> {
                    if (exception != null) {
                        throw new RuntimeException("Failed to send video ai data message", exception);
                    } else {
                        log.info("Video upload message sent successfully. Retry count: {}", finalMessage.getRetryCount());
                    }
                });
            }
        }
    }



}
