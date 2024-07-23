package com.cognitube.consumer.consumer;

import com.cognitube.consumer.consumer.dao.VideoAiDataMessage;
import com.cognitube.consumer.mapper.VideoMapper;
import com.cognitube.consumer.model.Video;
import com.cognitube.consumer.producer.VideoProcessProducer;
import com.cognitube.consumer.service.VideoProcessingService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@AllArgsConstructor
public class VideoAiDataConsumer {

    private final ObjectMapper objectMapper;
    private final VideoMapper videoMapper;
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
            if (videoProcessingService.isVideoProcessingStepDoneOrFailed(message.getVideoId(), KAFKA_VIDEO_AI_TOPIC)) {
                return;
            }

            final boolean isAiProcessSuccessful = message.isSuccess();
            if (!isAiProcessSuccessful) {
                final String error = message.getError();
                throw new Exception(error);
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
            videoProcessingService.markVideoProcessingStatusAsFailed(message.getVideoId());
        }
    }



}
