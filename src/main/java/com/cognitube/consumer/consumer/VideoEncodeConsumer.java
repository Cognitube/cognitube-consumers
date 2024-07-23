package com.cognitube.consumer.consumer;

import com.cognitube.consumer.consumer.dao.VideoEncodeMessage;
import com.cognitube.consumer.mapper.VideoMapper;
import com.cognitube.consumer.model.Video;
import com.cognitube.consumer.producer.VideoProcessProducer;
import com.cognitube.consumer.service.BlobService;
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
    private final VideoEncodingService videoEncodingService;
    private final BlobService blobService;
    private final VideoMapper videoMapper;
    private final String KAFKA_VIDEO_REENCODE_TOPIC;
    private final VideoProcessProducer videoProcessProducer;
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

        File tempVideoFile = null;
        File processedVideo = null;
        try {
            tempVideoFile = blobService.downloadFile(message.getVideoUrl());
            processedVideo = reencodeVideo(tempVideoFile);
            final double duration = videoProcessingService.getVideoDuration(processedVideo);
            final String processedVideoUrl = blobService.uploadTempVideoGetFullUrl(processedVideo);
            log.info("Reencoded video {} with duration {}", processedVideoUrl, duration);

            Video video = Video.builder()
                    .setId(message.getVideoId())
                    .setFileLink(processedVideoUrl)
                    .setLength(duration)
                    .build();
            videoMapper.updateVideo(video);

            videoProcessingService.recordVideoProcessingStatus(message.getVideoId(), KAFKA_VIDEO_REENCODE_TOPIC);
            videoProcessingService.updateVideoStatusIfDone(message.getVideoId());
        } catch (Exception e) {
            //TODO: specify more exception types
            log.error("Failed to reencode video", e);
            if (message.getRetryCount() > 3) {
                log.error("Failed to reencode video after 3 retries. Terminating reencoding for video.");
                videoProcessingService.markVideoProcessingStatusAsFailed(message.getVideoId());
            } else {
                message.setRetryCount(message.getRetryCount() + 1);
                final VideoEncodeMessage finalMessage = message;
                videoProcessProducer.sendKafkaMessageAsync(message, KAFKA_VIDEO_REENCODE_TOPIC, (metadata, exception) -> {
                    if (exception != null) {
                        videoProcessingService.markVideoProcessingStatusAsFailed(finalMessage.getVideoId());
                        throw new RuntimeException("Failed to send video reencode message", exception);
                    } else {
                        log.info("Video reencode message sent successfully. Retry count: {}", finalMessage.getRetryCount());
                    }
                });
            }
        } finally {
            videoProcessingService.removeFile(tempVideoFile);
            videoProcessingService.removeFile(processedVideo);
        }
    }

    private File reencodeVideo(File videoFile) {
        String newFileName = "processed_" + UUID.randomUUID() + ".mp4";
        File processedVideo = new File(videoFile.getParent(), newFileName);
        videoEncodingService.reencodeVideo(videoFile, processedVideo);
        return processedVideo;
    }
}
