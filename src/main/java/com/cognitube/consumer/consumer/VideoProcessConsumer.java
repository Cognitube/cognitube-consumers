package com.cognitube.consumer.consumer;


import com.cognitube.consumer.consumer.dao.VideoEncodeMessage;
import com.cognitube.consumer.consumer.dao.VideoUploadMessage;
import com.cognitube.consumer.producer.VideoProcessProducer;
import com.cognitube.consumer.service.BlobService;
import com.cognitube.consumer.service.VideoEncodingService;
import com.cognitube.consumer.util.RedisKeys;
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
import org.jcodec.api.FrameGrab;
import org.jcodec.common.io.FileChannelWrapper;
import org.jcodec.common.io.NIOUtils;
import org.json.JSONObject;
import org.slf4j.MDC;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.stream.Stream;


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
    private static final Double SEGMENT_DURATION_IN_MINUTE = 30.0;

    private final ObjectMapper objectMapper;
    private final BlobService blobService;
    private final VideoProcessProducer videoProcessProducer;
    private final RedisTemplate<String, String> redisTemplate;
    private final VideoEncodingService videoEncodingService;
    private final String KAFKA_VIDEO_PROCESS_TOPIC;
    private final String keywordServiceUrl;
    private final String blobEndpoint;

    @KafkaListener(topics = "${kafka.video.process.topic}", groupId = "${kafka.video.process.group.id}")
    public void consumeProcessVideoMessage(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        process(record, acknowledgment);
    }

    public void process(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
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

        try {
            if(isVideoProcessedOrProcessing(message.getVideoId())) {
                return;
            }

            recordVideoProcessingStatus(message.getVideoId());

            log.info("Start processing video: {}", message.getVideoId());
            final File videoFile = getOriginalVideo(message.getVideoUrl());

            log.info("Start converting video to audio: {}", message.getVideoId());
            final File audioFile = videoEncodingService.convertVideoToAudio(videoFile);
            String audioUrl = blobService.uploadAudio(audioFile);

            log.info("Audio extracted. Stored at: {}", audioUrl);
            createKeywordExtractionJob(audioUrl, message.getVideoId());

            log.info("Keyword extraction job created. Waiting for keywords to be extracted.");

        } catch (Exception e) {
            //TODO: specify more exception types
            log.error("Failed to process video", e);
            if (message != null && message.getRetryCount() > 3) {
                log.error("Failed to process video after 3 retries. Terminating processing for video.");
            } else {
                message.setRetryCount(message.getRetryCount() + 1);
                final VideoUploadMessage finalMessage = message;
                videoProcessProducer.sendVideoUploadMessageAsync(message, KAFKA_VIDEO_PROCESS_TOPIC, (metadata, exception) -> {
                    if (exception != null) {
                        throw new RuntimeException("Failed to send video upload message", exception);
                    } else {
                        log.info("Video upload message sent successfully. Retry count: {}", finalMessage.getRetryCount());
                    }
                });
            }
        }
    }

    private void publishSubTasksToKafka(VideoUploadMessage message, Double duration, String keywordsUrl) {
        Stream.iterate(0.0, n -> n < duration, n -> n + SEGMENT_DURATION_IN_MINUTE)
                .forEach(startindex -> {
                    Double endindex = Math.min(startindex + SEGMENT_DURATION_IN_MINUTE, duration);
                    VideoEncodeMessage subTaskMessage = new VideoEncodeMessage(startindex, endindex, keywordsUrl, message);
                    sendMessageToEncodeKafkaGroup(subTaskMessage);
                    updateTempTable(subTaskMessage);
                });
    }

    // TODO
    private void updateTempTable(VideoEncodeMessage subTaskMessage) {
    }

    // TODO
    private void sendMessageToEncodeKafkaGroup(VideoEncodeMessage subTaskMessage) {
    }

    private void recordVideoProcessingStatus(String videoId) {
        final String videoProcessStatusKey = RedisKeys.getVideoProcessingStatusKey(videoId);
        try {
            redisTemplate.opsForValue().set(videoProcessStatusKey, "processing");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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

    private boolean isVideoProcessedOrProcessing(String videoId) {
        final String videoProcessStatusKey = RedisKeys.getVideoProcessingStatusKey(videoId);
        String status = null;
        try {
            status = redisTemplate.opsForValue().get(videoProcessStatusKey);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return status != null && (status.equals("processed") || status.equals("processing"));
    }

    // TODO
    private String getKeywordsFromAIService(File videoFile) {
        return "keywords URL";
    }


    private File getOriginalVideo(String videoUrl) {
        return blobService.downloadFile(videoUrl);
    }

    public double getVideoDuration(File videoFile) {
        try (FileChannelWrapper ch = NIOUtils.readableChannel(videoFile)) {
            FrameGrab grab = FrameGrab.createFrameGrab(ch);
            return grab.getVideoTrack().getMeta().getTotalDuration();
        } catch (Exception e) {
            throw new RuntimeException("Failed to get video duration", e);
        }
    }

}

