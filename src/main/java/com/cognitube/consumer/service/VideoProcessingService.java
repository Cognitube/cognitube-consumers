package com.cognitube.consumer.service;

import com.cognitube.consumer.enums.VideoStatus;
import com.cognitube.consumer.mapper.VideoMapper;
import com.cognitube.consumer.model.Video;
import com.cognitube.consumer.util.DateUtil;
import com.cognitube.consumer.util.RedisKeys;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jcodec.api.FrameGrab;
import org.jcodec.common.io.FileChannelWrapper;
import org.jcodec.common.io.NIOUtils;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.io.File;
import java.time.LocalDate;
import java.util.Objects;
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
    private final NotificationService notificationService;

    public double getVideoDuration(File videoFile) {
        try (FileChannelWrapper ch = NIOUtils.readableChannel(videoFile)) {
            FrameGrab grab = FrameGrab.createFrameGrab(ch);
            return grab.getVideoTrack().getMeta().getTotalDuration();
        } catch (Exception e) {
            throw new RuntimeException("Failed to get video duration", e);
        }
    }

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

    public void recordVideoProcessingStatus(String videoId, Long userId, String videoName) {
        final String videoProcessStatusKey = RedisKeys.getVideoProcessingStatusKey(videoId);
        try {
            final HashOperations<String, Object, Object> hashOps = redisTemplate.opsForHash();
            hashOps.put(videoProcessStatusKey, KAFKA_VIDEO_REENCODE_TOPIC, "pending");
            hashOps.put(videoProcessStatusKey, KAFKA_VIDEO_AI_TOPIC, "pending");
            hashOps.put(videoProcessStatusKey, "status", "processing");
            hashOps.put(videoProcessStatusKey, "videoName", videoName);
            hashOps.put(videoProcessStatusKey, "userId", userId.toString());
            redisTemplate.expire(videoProcessStatusKey, 12, TimeUnit.HOURS);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void recordVideoDuration(String videoId, double videoDuration) {
        final String videoProcessStatusKey = RedisKeys.getVideoProcessingStatusKey(videoId);
        try {
            final HashOperations<String, Object, Object> hashOps = redisTemplate.opsForHash();
            hashOps.put(videoProcessStatusKey, "videoDuration", String.valueOf(videoDuration));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public boolean isVideoProcessingStepDoneOrFailed(String videoId, String topic) {
        final String videoProcessStatusKey = RedisKeys.getVideoProcessingStatusKey(videoId);
        String status;
        try {
            final boolean keyExists = Boolean.TRUE.equals(redisTemplate.hasKey(videoProcessStatusKey));
            if (!keyExists) {
                // this happens when the temp table has expired in the middle of the process
                throw new RuntimeException("Video process status key does not exist");
            }

            final HashOperations<String, Object, Object> hashOps = redisTemplate.opsForHash();
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
            final boolean keyExists = Boolean.TRUE.equals(redisTemplate.hasKey(videoProcessStatusKey));
            if (!keyExists) {
                // this happens when the temp table has expired in the middle of the process
                throw new RuntimeException("Video process status key does not exist");
            }

            final HashOperations<String, Object, Object> hashOps = redisTemplate.opsForHash();
            final String aiStatus = (String) hashOps.get(videoProcessStatusKey, KAFKA_VIDEO_AI_TOPIC);
            final String reencodeStatus = (String) hashOps.get(videoProcessStatusKey, KAFKA_VIDEO_REENCODE_TOPIC);
            final String overallStatus = (String) hashOps.get(videoProcessStatusKey, "status");
            final String videoName = (String) Objects.requireNonNull(hashOps.get(videoProcessStatusKey, "videoName"));
            final Long userId = Long.parseLong((String) Objects.requireNonNull(hashOps.get(videoProcessStatusKey, "userId")));
            final double videoDuration = Double.parseDouble((String) Objects.requireNonNull(hashOps.get(videoProcessStatusKey, "videoDuration")));

            if ("failed".equals(overallStatus)) {
                handleFailedProcessing(userId, videoName);
                return;
            }

            if ("pending".equals(aiStatus) || "pending".equals(reencodeStatus)) {
                return;
            }

            hashOps.put(videoProcessStatusKey, "status", "done");

            final Video video = Video.builder()
                    .setId(videoId)
                    .setStatus(VideoStatus.OK)
                    .build();
            videoMapper.updateVideo(video);

            final String userWeeklyUploadLimitKey = RedisKeys.getUserWeeklyUploadLimitKey(userId, String.valueOf(DateUtil.getWeekOfYear(LocalDate.now())));
            redisTemplate.opsForValue().increment(userWeeklyUploadLimitKey, videoDuration);
            redisTemplate.expire(userWeeklyUploadLimitKey, 7, TimeUnit.DAYS);

            final String notificationMessage = String.format("Your video %s has been successfully uploaded and processed!", videoName);
            notificationService.addSystemNotification(userId, notificationMessage);

            log.info("Video has been processed.");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // TODO: add more handling for database record and temp files on blob
    private void handleFailedProcessing(Long userId, String videoName) {
        final String notificationMessage = String.format("Your video %s has failed to process. Please try again later!", videoName);
        notificationService.addSystemNotification(userId, notificationMessage);
    }

    public void removeFile(File file) {
        if (file != null && file.exists()) {
            file.delete();
        }
    }
}
