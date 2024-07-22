package com.cognitube.consumer.service;

import com.cognitube.consumer.enums.VideoStatus;
import com.cognitube.consumer.mapper.VideoMapper;
import com.cognitube.consumer.model.Video;
import com.cognitube.consumer.util.RedisKeys;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.io.File;
import java.util.concurrent.TimeUnit;

/**
 * @author Haozhe Zhang
 * @version 1.0
 * @project cognitube-consumers
 * @description Methods shared by video processing consumers.
 * @date 2024/7/21 22:40:24
 */
@Service
@AllArgsConstructor
@Slf4j
public class VideoProcessingService {

    private final VideoMapper videoMapper;
    private final RedisTemplate<String, String> redisTemplate;
    private final String KAFKA_VIDEO_AI_TOPIC;
    private final String KAFKA_VIDEO_REENCODE_TOPIC;

    public boolean isVideoProcessedOrProcessing(String videoId) {
        final String videoProcessStatusKey = RedisKeys.getVideoProcessingStatusKey(videoId);
        boolean status;
        try {
            status = Boolean.TRUE.equals(redisTemplate.hasKey(videoProcessStatusKey));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return status;
    }

    public void recordVideoProcessingStatus(String videoId) {
        final String videoProcessStatusKey = RedisKeys.getVideoProcessingStatusKey(videoId);
        try {
            HashOperations<String, Object, Object> hashOps = redisTemplate.opsForHash();
            hashOps.put(videoProcessStatusKey, KAFKA_VIDEO_REENCODE_TOPIC, "pending");
            hashOps.put(videoProcessStatusKey, KAFKA_VIDEO_AI_TOPIC, "pending");
            hashOps.put(videoProcessStatusKey, "status", "processing");
            redisTemplate.expire(videoProcessStatusKey, 12, TimeUnit.HOURS);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public boolean isVideoProcessingStepDoneOrFailed(String videoId, String topic) {
        final String videoProcessStatusKey = RedisKeys.getVideoProcessingStatusKey(videoId);
        String status;
        try {
            boolean keyExists = Boolean.TRUE.equals(redisTemplate.hasKey(videoProcessStatusKey));
            if (!keyExists) {
                // this happens when the temp table has expired in the middle of the process
                throw new RuntimeException("Video process status key does not exist");
            }

            HashOperations<String, Object, Object> hashOps = redisTemplate.opsForHash();
            status = (String) hashOps.get(videoProcessStatusKey, topic);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return "done".equals(status) || "failed".equals(status);
    }

    public void recordVideoProcessingStatus(String videoId, String topic) {
        final String videoProcessStatusKey = RedisKeys.getVideoProcessingStatusKey(videoId);
        try {
            HashOperations<String, Object, Object> hashOps = redisTemplate.opsForHash();
            hashOps.put(videoProcessStatusKey, topic, "done");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void markVideoProcessingStatusAsFailed(String videoId) {
        final String videoProcessStatusKey = RedisKeys.getVideoProcessingStatusKey(videoId);
        try {
            HashOperations<String, Object, Object> hashOps = redisTemplate.opsForHash();
            hashOps.put(videoProcessStatusKey, "status", "failed");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void updateVideoStatusIfDone(String videoId) {
        final String videoProcessStatusKey = RedisKeys.getVideoProcessingStatusKey(videoId);
        try {
            boolean keyExists = Boolean.TRUE.equals(redisTemplate.hasKey(videoProcessStatusKey));
            if (!keyExists) {
                // this happens when the temp table has expired in the middle of the process
                throw new RuntimeException("Video process status key does not exist");
            }

            HashOperations<String, Object, Object> hashOps = redisTemplate.opsForHash();
            String aiStatus = (String) hashOps.get(videoProcessStatusKey, KAFKA_VIDEO_AI_TOPIC);
            String reencodeStatus = (String) hashOps.get(videoProcessStatusKey, KAFKA_VIDEO_REENCODE_TOPIC);
            if (!"done".equals(aiStatus) || !"done".equals(reencodeStatus)) {
                return;
            }

            Video video = Video.builder()
                    .setId(videoId)
                    .setStatus(VideoStatus.OK)
                    .build();
            videoMapper.updateVideo(video);
            log.info("Video has been processed.");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void removeFile(File file) {
        if (file != null && file.exists()) {
            file.delete();
        }
    }
}
