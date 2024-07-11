package com.cognitube.consumer.consumer;


import com.cognitube.consumer.consumer.dao.VideoEncodeMessage;
import com.cognitube.consumer.consumer.dao.VideoUploadMessage;
import com.cognitube.consumer.service.BlobService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jcodec.api.FrameGrab;
import org.jcodec.common.io.FileChannelWrapper;
import org.jcodec.common.io.NIOUtils;
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

    // TODO: Add partition to parallel consume
    @KafkaListener(topics = "${kafka.video.process.topic}", groupId = "${kafka.video.process.group.id}")
    public void consumeProcessVideoMessage(ConsumerRecord<String, String> record, Acknowledgment acknowledgment){
        boolean success = process(record);
        if (success) {
            acknowledgment.acknowledge();
        }
    }

    public boolean process(ConsumerRecord<String, String> record) {
        try {
            // TODO: logs

            VideoUploadMessage message = objectMapper.readValue(record.value(), VideoUploadMessage.class);

            if(checkProcessedInResultTable(message.getVideoId())) return true;

            File videoFile = getOriginalVideo(message.getVideoUrl());
            Double duration = getVideoDuration(videoFile);

            if(checkProcessingTempTable(message.getVideoId(), duration)) return true;

            String keywordsUrl = getKeywordsFromAIService(videoFile);
            publishSubTasksToKafka(message, duration, keywordsUrl);

            return true;

        } catch (JsonProcessingException e) {
            log.error("Failed to deserialize video process message", e);
            return false;
        } catch (Exception e) {
            log.error("Failed to process video", e);
            return false;
        }
    }

    private void publishSubTasksToKafka(VideoUploadMessage message, Double duration, String keywordsUrl) {
        createTempTableByVid(message.getVideoId());
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

    // TODO
    private void createTempTableByVid(Long videoId) {
    }

    // TODO: if temp table exist && tem table record num = duration / SEGMENT_DURATION_IN_MINUTE => return true
    private boolean checkProcessingTempTable(Long videoId, Double duration) {
        return false;
    }

    // TODO: if result status == processed  => return true
    private boolean checkProcessedInResultTable(Long videoId) {
        return false;
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

