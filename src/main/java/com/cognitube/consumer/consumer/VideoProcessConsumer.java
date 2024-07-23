package com.cognitube.consumer.consumer;


import com.cognitube.consumer.consumer.dao.VideoEncodeMessage;
import com.cognitube.consumer.consumer.dao.VideoUploadMessage;
import com.cognitube.consumer.producer.VideoProcessProducer;
import com.cognitube.consumer.service.BlobService;
import com.cognitube.consumer.service.VideoEncodingService;
import com.cognitube.consumer.service.VideoProcessingService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.json.JSONObject;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
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
    private final BlobService blobService;
    private final VideoProcessProducer videoProcessProducer;
    private final VideoEncodingService videoEncodingService;
    private final String KAFKA_VIDEO_PROCESS_TOPIC;
    private final String KAFKA_VIDEO_REENCODE_TOPIC;
    private final String KAFKA_VIDEO_AI_TOPIC;
    private final String keywordServiceUrl;
    private final String blobEndpoint;
    private final VideoProcessingService videoProcessingService;

    @KafkaListener(topics = "${kafka.video.process.topic}", groupId = "${kafka.video.process.group.id}")
    public void consumeProcessVideoMessage(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        process(record, acknowledgment);
    }

    private void process(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        VideoUploadMessage message = null;
        acknowledgment.acknowledge();

        try {
            message = objectMapper.readValue(record.value(), VideoUploadMessage.class);
        } catch (JsonProcessingException e) {
            log.error("Failed to deserialize video process message", e);
            return;
        }

        MDC.put("videoId", message.getVideoId());
        MDC.put("userId", message.getUserId().toString());
        MDC.put("processingTime", System.currentTimeMillis() + "");

        File videoFile = null;
        File audioFile = null;

        try {
            if (videoProcessingService.isVideoProcessedOrProcessing(message.getVideoId())) {
                return;
            }

            videoProcessingService.recordVideoProcessingStatus(message.getVideoId(), message.getUserId(), message.getVideoName());

            log.info("Start processing video: {}", message.getVideoId());
            videoFile = getOriginalVideo(message.getVideoUrl());

            final double videoDuration = videoProcessingService.getVideoDuration(videoFile);
            videoProcessingService.recordVideoDuration(message.getVideoId(), videoDuration);

            log.info("Start converting video to audio: {}", message.getVideoId());
            audioFile = videoEncodingService.convertVideoToAudio(videoFile);
            if (audioFile == null) {
                log.warn("Video does not have an audio track.");
                videoProcessingService.recordVideoProcessingStatus(message.getVideoId(), KAFKA_VIDEO_AI_TOPIC);
            } else {
                final String audioUrl = blobService.uploadAudio(audioFile);
                log.info("Audio extracted. Stored at: {}", audioUrl);

                createKeywordExtractionJob(audioUrl, message.getVideoId());
                log.info("Keyword extraction job created. Waiting for keywords to be extracted.");
            }

            sendMessageToEncodeKafkaGroup(
                VideoEncodeMessage.builder()
                    .setVideoId(message.getVideoId())
                    .setVideoUrl(message.getVideoUrl())
                    .setRetryCount(0)
                    .build()
            );

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
                        throw new RuntimeException("Failed to send video upload message", exception);
                    } else {
                        log.info("Video upload message sent successfully. Retry count: {}", finalMessage.getRetryCount());
                    }
                });
            }
        } finally {
            videoProcessingService.removeFile(videoFile);
            videoProcessingService.removeFile(audioFile);
        }
    }

    // TODO
    private void sendMessageToEncodeKafkaGroup(VideoEncodeMessage subTaskMessage) {
        videoProcessProducer.sendKafkaMessageAsync(subTaskMessage, KAFKA_VIDEO_REENCODE_TOPIC, (metadata, exception) -> {
            if (exception != null) {
                throw new RuntimeException("Failed to send video upload message", exception);
            }
        });
    }


    private void createKeywordExtractionJob(String audioFileurl, String videoId) {
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            final HttpPost uploadFile = new HttpPost(keywordServiceUrl + "/v1/get-keywords");

            final JSONObject json = new JSONObject();
            json.put("url", blobEndpoint + "/" + audioFileurl);

            final StringEntity entity = new StringEntity(json.toString(), ContentType.APPLICATION_JSON);
            uploadFile.setEntity(entity);

            httpClient.execute(uploadFile);
        } catch (Exception e) {
            throw new RuntimeException("Failed to extract keywords", e);
        }
    }

    private File getOriginalVideo(String videoUrl) {
        return blobService.downloadFile(videoUrl);
    }
}

