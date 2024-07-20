package com.cognitube.consumer.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

/**
 * @author Haozhe Zhang
 * @version 1.0
 * @project cognitube-consumers
 * @description Used for importing values via the Value annotation
 * @date 2024/7/20 17:02:31
 */
@Configuration
public class ConstantValueConfig {
    public final String keywordServiceUrl;
    public final String blobEndpoint;
    public final String tempVideoContainerName;
    public final String videoContainerName;
    public final String imageContainerName;
    public final String keywordsContainerName;
    public final String azureFrontdoorUrl;
    public final String profileImageContainerName;
    public final String audioContainerName;
    public final String namespace;
    public final String username;
    public final String password;
    public final long MAX_FILE_SIZE_BYTES;
    public final int DEFAULT_AUDIO_BIT_RATE_KBPS;
    public final int DEFAULT_SAMPLING_RATE;
    public final String KAFKA_VIDEO_PROCESS_TOPIC;

    public ConstantValueConfig(
            @Value("${application.keyword.service.url}") String keywordServiceUrl,
            @Value("${azure.storage.blob.endpoint}") String blobEndpoint,
            @Value("${azure.storage.container-name-temp-video-container}") String tempVideoContainerName,
            @Value("${azure.storage.container-name-video-container}") String videoContainerName,
            @Value("${azure.storage.container-name-image-container}") String imageContainerName,
            @Value("${azure.storage.container-name-keywords-container}") String keywordsContainerName,
            @Value("${azure.frontdoor.url}") String azureFrontdoorUrl,
            @Value("${azure.storage.container-name-profile-image-container}") String profileImageContainerName,
            @Value("${azure.storage.container-name-audio-container}") String audioContainerName,
            @Value("${kafka.eventhub.namespace:}") String namespace,
            @Value("${kafka.username}") String username,
            @Value("${kafka.password}") String password,
            @Value("${application.audio.max.size}") long MAX_FILE_SIZE_BYTES,
            @Value("${application.encoding.default.bitrate}") int DEFAULT_AUDIO_BIT_RATE_KBPS,
            @Value("${application.encoding.default.sampling-rate}") int DEFAULT_SAMPLING_RATE,
            @Value("${kafka.topic.video-process}") String KAFKA_VIDEO_PROCESS_TOPIC
    ) {
        this.keywordServiceUrl = keywordServiceUrl;
        this.blobEndpoint = blobEndpoint;
        this.tempVideoContainerName = tempVideoContainerName;
        this.videoContainerName = videoContainerName;
        this.imageContainerName = imageContainerName;
        this.keywordsContainerName = keywordsContainerName;
        this.azureFrontdoorUrl = azureFrontdoorUrl;
        this.profileImageContainerName = profileImageContainerName;
        this.audioContainerName = audioContainerName;
        this.namespace = namespace;
        this.username = username;
        this.password = password;
        this.MAX_FILE_SIZE_BYTES = MAX_FILE_SIZE_BYTES;
        this.DEFAULT_AUDIO_BIT_RATE_KBPS = DEFAULT_AUDIO_BIT_RATE_KBPS;
        this.DEFAULT_SAMPLING_RATE = DEFAULT_SAMPLING_RATE;
        this.KAFKA_VIDEO_PROCESS_TOPIC = KAFKA_VIDEO_PROCESS_TOPIC;
    }
}

