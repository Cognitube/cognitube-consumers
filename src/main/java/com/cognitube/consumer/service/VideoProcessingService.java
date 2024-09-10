package com.cognitube.consumer.service;

import com.cognitube.consumer.enums.VideoStatus;
import com.cognitube.consumer.mapper.VideoMapper;
import com.cognitube.consumer.model.Video;
import com.cognitube.consumer.util.DateUtil;
import com.cognitube.consumer.util.RedisKeys;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.jcodec.api.FrameGrab;
import org.jcodec.common.io.FileChannelWrapper;
import org.jcodec.common.io.NIOUtils;
import org.json.JSONObject;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.*;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import java.io.File;
import java.time.LocalDate;
import java.util.Collections;
import java.util.Objects;
import java.util.UUID;
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
    private final String transcodingServiceUrl;
    private final Integer VIDEO_PROCESSING_OVERTIME_THRESHOLD_IN_HOUR;

    public boolean isVideoProcessedOrProcessing(String videoId, int retryCount) {
        final String videoProcessStatusKey = RedisKeys.getVideoProcessingStatusKey(videoId);
        try {
            final boolean status = Boolean.TRUE.equals(redisTemplate.hasKey(videoProcessStatusKey));
            if (!status) {
                return false;
            }

            final int count = redisTemplate.opsForHash().get(videoProcessStatusKey, "retryCount") == null
                    ? 0 : Integer.parseInt((String) Objects.requireNonNull(
                    redisTemplate.opsForHash().get(videoProcessStatusKey, "retryCount")
            ));
            if (count >= retryCount) {
                return true;
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return false;
    }

    public boolean isAudioExtractedOrExtracting(String videoId, int retryCount) {
        final String videoProcessStatusKey = RedisKeys.getVideoProcessingStatusKey(videoId);
        try {
            final boolean status = Boolean.TRUE.equals(redisTemplate.hasKey(videoProcessStatusKey));
            if (!status) {
                throw new RuntimeException("Video process status key does not exist");
            }

            final int count = redisTemplate.opsForHash().get(videoProcessStatusKey, "audioExtractionRetryCount") == null
                    ? -1 : Integer.parseInt((String) Objects.requireNonNull(
                    redisTemplate.opsForHash().get(videoProcessStatusKey, "audioExtractionRetryCount")
            ));
            if (count >= retryCount) {
                return true;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return false;
    }

    public void recordAudioExtractionStatus(String videoId, int retryCount) {
        final String videoProcessStatusKey = RedisKeys.getVideoProcessingStatusKey(videoId);
        try {
            final HashOperations<String, Object, Object> hashOps = redisTemplate.opsForHash();
            hashOps.put(videoProcessStatusKey, "audioExtractionRetryCount", String.valueOf(retryCount));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void recordVideoProcessingStatus(String videoId, Long userId, String videoName, int retryCount) {
        final String videoProcessStatusKey = RedisKeys.getVideoProcessingStatusKey(videoId);
        final String videoProcessOvertimeKey = RedisKeys.getVideoProcessingOvertimeKey(videoId);
        final String videoProcessOvertimeAllCharsKey = RedisKeys.getVideoProcessingOvertimeAllCharsKey();
        try {
            redisTemplate.execute(new SessionCallback<Object>() {
                @Override
                public <K, V> Object execute(RedisOperations<K, V> operations) throws DataAccessException {
                    operations.multi(); // 开始事务

                    final HashOperations<K, Object, Object> hashOps = (HashOperations) operations.opsForHash();
                    hashOps.put((K) videoProcessStatusKey, "KAFKA_VIDEO_REENCODE_TOPIC", "pending");
                    hashOps.put((K) videoProcessStatusKey, "KAFKA_VIDEO_AI_TOPIC", "pending");
                    hashOps.put((K) videoProcessStatusKey, "status", "processing");
                    hashOps.put((K) videoProcessStatusKey, "videoName", videoName);
                    hashOps.put((K) videoProcessStatusKey, "userId", userId.toString());
                    hashOps.put((K) videoProcessStatusKey, "retryCount", String.valueOf(retryCount));
                    operations.expire((K) videoProcessStatusKey, 12, TimeUnit.HOURS);

                    final ZSetOperations<K, Object> zSetOps = (ZSetOperations) operations.opsForZSet();
                    zSetOps.add((K) videoProcessOvertimeKey, videoId, System.currentTimeMillis() + VIDEO_PROCESSING_OVERTIME_THRESHOLD_IN_HOUR * 60 * 60 * 1000);
                    operations.expire((K) videoProcessOvertimeKey, VIDEO_PROCESSING_OVERTIME_THRESHOLD_IN_HOUR, TimeUnit.HOURS);

                    SetOperations<K, Object> setOps = (SetOperations) operations.opsForSet();
                    setOps.add((K) videoProcessOvertimeAllCharsKey, videoId.substring(videoId.length() - 1));
                    operations.expire((K) videoProcessOvertimeAllCharsKey, VIDEO_PROCESSING_OVERTIME_THRESHOLD_IN_HOUR, TimeUnit.HOURS);

                    return operations.exec(); // 提交事务
                }
            });

            log.info("Video processing started for video ID: {}", videoId);
            log.info("Video processing overtime key: {}", videoProcessOvertimeKey);
            log.info("Video processing overtime all chars key: {}", videoProcessOvertimeAllCharsKey);

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
        final String lockKey = RedisKeys.getVideoProcessingLockKey(videoId); // 定义一个锁键
        final String lockValue = UUID.randomUUID().toString(); // 锁的唯一值
        final int lockExpiration = 30; // 锁的过期时间，单位为秒

        try {
            Boolean acquiredLock = redisTemplate.opsForValue().setIfAbsent(lockKey, lockValue, lockExpiration, TimeUnit.SECONDS);
            if (Boolean.FALSE.equals(acquiredLock)) {
                log.info("Failed to acquire lock for video processing status update for video {}", videoId);
                return;
            }

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

            if ("failed".equals(overallStatus)) {
                handleFailedProcessing(userId, videoName);
                return;
            }

            if ("pending".equals(aiStatus) || "pending".equals(reencodeStatus)) {
                return;
            }

            if ("done".equals(overallStatus)) {
                return;
            }

            hashOps.put(videoProcessStatusKey, "status", "done");

            final Video video = Video.builder()
                    .setId(videoId)
                    .setStatus(VideoStatus.OK)
                    .build();
            videoMapper.updateVideo(video);

            final String notificationMessage = String.format("Your video %s has been successfully uploaded and processed!", videoName);
            notificationService.addSystemNotification(userId, notificationMessage);

            log.info("Video has been processed.");
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            releaseLock(lockKey, lockValue);
        }
    }

    public void invalidateVideoCache(String videoId) {
        final String videoCacheKey = RedisKeys.getVideoCacheKey(videoId);

        try {
            redisTemplate.delete(videoCacheKey);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void releaseLock(String lockKey, String lockValue) {
        // 使用 Lua 脚本原子性地释放锁
        String script = "if redis.call('get', KEYS[1]) == ARGV[1] then " +
                "return redis.call('del', KEYS[1]) " +
                "else return 0 end";
        redisTemplate.execute(new DefaultRedisScript<>(script, Long.class), Collections.singletonList(lockKey), lockValue);
    }

    // TODO: add more handling for database record and temp files on blob
    private void handleFailedProcessing(Long userId, String videoName) {
        final String notificationMessage = String.format("Your video %s has failed to process. Please try again later!", videoName);
        notificationService.addSystemNotification(userId, notificationMessage);
    }

    public void createAudioExtractionJob(String videoUrl, String videoId, int retryCount) {
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            final HttpPost request = new HttpPost(transcodingServiceUrl + "/v1/extract-audio");

            final JSONObject json = new JSONObject();
            json.put("videoId", videoId);
            json.put("videoUrl", videoUrl);
            json.put("retryCount", retryCount);

            final StringEntity entity = new StringEntity(json.toString(), ContentType.APPLICATION_JSON);
            request.setEntity(entity);

            HttpResponse response = httpClient.execute(request);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != 200) {
                String responseBody = EntityUtils.toString(response.getEntity());
                log.error("Failed to send audio extraction request {}: {}", statusCode, responseBody);
                throw new RuntimeException("Failed to send audio extraction request");
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to send audio extraction request", e);
        }
    }

    public void createVideoTranscodingJob(String videoUrl, String videoId, int retryCount) {
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            final HttpPost request = new HttpPost(transcodingServiceUrl + "/v1/transcode");

            final JSONObject json = new JSONObject();
            json.put("videoId", videoId);
            json.put("videoUrl", videoUrl);
            json.put("retryCount", retryCount);

            final StringEntity entity = new StringEntity(json.toString(), ContentType.APPLICATION_JSON);
            request.setEntity(entity);

            HttpResponse response = httpClient.execute(request);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != 200) {
                String responseBody = EntityUtils.toString(response.getEntity());
                log.error("Failed to send transcoding request {}: {}", statusCode, responseBody);
                throw new RuntimeException("Failed to send transcoding request");
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to send transcoding request", e);
        }
    }
}
