package com.cognitube.consumer.consumer;

import com.cognitube.consumer.consumer.dao.VideoEncodeMessage;
import com.cognitube.consumer.mapper.VideoMapper;
import com.cognitube.consumer.model.Video;
import com.cognitube.consumer.service.VideoEncodingService;
import com.cognitube.consumer.service.VideoProcessingService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.UUID;

@Slf4j
@Component
@AllArgsConstructor
public class VideoEncodeConsumer {

    private final ObjectMapper objectMapper;
    private final VideoMapper videoMapper;
    private final String KAFKA_VIDEO_REENCODE_TOPIC;
    private final VideoProcessingService videoProcessingService;

    @KafkaListener(topics = "${kafka.video.reencode.topic}", groupId = "${kafka.video.process.group.id}")
    public void consumeReencodeMessage(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        process(record, acknowledgment);
    }

    private void process(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        acknowledgment.acknowledge();
        VideoEncodeMessage message;
        try {
            message = objectMapper.readValue(record.value(), VideoEncodeMessage.class);
        } catch (JsonProcessingException e) {
            log.error("Failed to deserialize video reencode message", e);
            return;
        }

        try {
            final boolean isTranscodingSuccessful = message.isSuccess();
            if (!isTranscodingSuccessful) {
                final String error = message.getError();
                throw new Exception(error);
            }

            final String videoId = message.getVideoId();
            final String processedVideoUrl = message.getVideoUrl();
            log.info("Reencoded video {} stored at {}", videoId, processedVideoUrl);

            final Video video = Video.builder()
                    .setId(videoId)
                    .setFileLink(processedVideoUrl)
                    .build();
            videoMapper.updateVideo(video);

            videoProcessingService.recordVideoProcessingStatus(message.getVideoId(), KAFKA_VIDEO_REENCODE_TOPIC);
            videoProcessingService.updateVideoStatusIfDone(message.getVideoId());
        } catch (Exception e) {
            //TODO: specify more exception types
            log.error("Failed to reencode video", e);
            videoProcessingService.markVideoProcessingStatusAsFailed(message.getVideoId());
        }
    }
}
