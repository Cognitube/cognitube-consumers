package com.cognitube.consumer.consumer;

import com.cognitube.consumer.consumer.dao.VideoAudioExtractionMessage;
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
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * @author Haozhe Zhang
 * @version 1.0
 * @project cognitube-consumers
 * @description Consumer for handling audio extraction messages
 * @date 2024/8/11 13:53:12
 */
@Slf4j
@AllArgsConstructor
@Component
public class VideoAudioExtractionConsumer {

    private final ObjectMapper objectMapper;
    private final VideoProcessingService videoProcessingService;
    private final String keywordServiceUrl;

    @KafkaListener(topics = "${kafka.video.audio.extraction.topic}", groupId = "${kafka.video.process.group.id}")
    public void consumeAudioExtractionMessage(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        process(record, acknowledgment);
    }

    private void process(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        acknowledgment.acknowledge();
        log.info("Received audio extraction message: {}", record.value());

        VideoAudioExtractionMessage message;

        try {
            message = objectMapper.readValue(record.value(), VideoAudioExtractionMessage.class);
        } catch (JsonProcessingException e) {
            log.error("Failed to deserialize audio extraction message", e);
            return;
        }

        try {
            if (videoProcessingService.isAudioExtractedOrExtracting(message.getVideoId(), message.getRetryCount())) {
                return;
            }

            final boolean isSuccess = message.isSuccess();
            if (!isSuccess) {
                final String error = message.getError();
                if (!error.equals("Video does not contain an audio track")) {
                    throw new Exception(error);
                }

                log.info("Video does not contain an audio track");
                videoProcessingService.recordAudioExtractionStatus(message.getVideoId(), message.getRetryCount());
                return;
            }

            final String videoId = message.getVideoId();
            final String audioUrl = message.getAudioUrl();
            log.info("Extracted audio for video {} stored at {}", videoId, audioUrl);

            createKeywordExtractionJob(audioUrl, videoId);
            videoProcessingService.recordAudioExtractionStatus(message.getVideoId(), message.getRetryCount());
        } catch (Exception e) {
            log.error("Failed to process audio extraction message", e);
            if (message.getRetryCount() > 3) {
                log.error("Failed to process video after 3 retries. Terminating processing for video.");
                videoProcessingService.markVideoProcessingStatusAsFailed(message.getVideoId());
            } else {
                try {
                    videoProcessingService.createAudioExtractionJob(message.getAudioUrl(), message.getVideoId(), message.getRetryCount() + 1);
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            }
        }
    }


    private void createKeywordExtractionJob(String audioFileurl, String videoId) {
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            final HttpPost uploadFile = new HttpPost(keywordServiceUrl + "/v1/transcription/create");

            final JSONObject json = new JSONObject();
            json.put("videoId", videoId);
            json.put("audioUrl", audioFileurl);

            final StringEntity entity = new StringEntity(json.toString(), ContentType.APPLICATION_JSON);
            uploadFile.setEntity(entity);

            httpClient.execute(uploadFile);
        } catch (Exception e) {
            throw new RuntimeException("Failed to extract keywords", e);
        }
    }
}
