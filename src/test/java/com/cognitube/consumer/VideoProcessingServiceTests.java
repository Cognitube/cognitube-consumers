// VideoProcessingServiceTests.java
package com.cognitube.consumer;

import com.cognitube.consumer.mapper.VideoMapper;
import com.cognitube.consumer.model.Video;
import com.cognitube.consumer.service.BlobService;
import com.cognitube.consumer.service.NotificationService;
import com.cognitube.consumer.service.VideoProcessingService;
import com.cognitube.consumer.util.RedisKeys;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.RedisTemplate;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class VideoProcessingServiceTests {

    @Mock
    private VideoMapper videoMapper;

    @Mock
    private RedisTemplate<String, String> redisTemplate;

    @Mock
    private NotificationService notificationService;

    @Mock
    private BlobService blobService;

    @Mock
    private HashOperations<String, Object, Object> hashOperations;

    @InjectMocks
    private VideoProcessingService videoProcessingService;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        when(redisTemplate.opsForHash()).thenReturn(hashOperations);
    }

    @Test
    void testHandleFailedProcessing() {
        String testVideoId = "testVideoId";
        String videoProcessStatusKey = RedisKeys.getVideoProcessingStatusKey(testVideoId);

        // Mock Redis operations
        when(redisTemplate.hasKey(videoProcessStatusKey)).thenReturn(true);
        when(hashOperations.get(videoProcessStatusKey, "videoUrl")).thenReturn("testVideoUrl");

        // Mock VideoMapper operations
        Video testVideo = Video.builder()
                .setId(testVideoId)
                .setAuthorId(1L)
                .setTitle("Test Video")
                .setFileLink("testFileLink")
                .setCoverImageLink("testCoverImageLink")
                .setKeywordsLink("testKeywordsLink")
                .setTranscriptLink("testTranscriptLink")
                .build();
        when(videoMapper.selectVideoById(testVideoId)).thenReturn(testVideo);

        // Call the method
        videoProcessingService.handleFailedProcessing(testVideoId);

        // Verify interactions
        verify(notificationService).addSystemNotification(eq(1L), anyString());
        verify(blobService).safeDeleteFile("testVideoUrl");
        verify(blobService).safeDeleteFile("testFileLink");
        verify(blobService).safeDeleteFile("testCoverImageLink");
        verify(blobService).safeDeleteFile("testKeywordsLink");
        verify(blobService).safeDeleteFile("testTranscriptLink");
        verify(videoMapper).deleteVideoById(testVideoId);
        verify(redisTemplate).delete(videoProcessStatusKey);
    }
}