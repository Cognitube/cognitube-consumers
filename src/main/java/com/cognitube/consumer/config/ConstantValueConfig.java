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

    @Value("${application.keyword.service.url}")
    public String keywordServiceUrl;

    @Value("${azure.storage.blob.endpoint}")
    public String blobEndpoint;
    
    @Value("${azure.storage.container-name-temp-video-container}")
    public String tempVideoContainerName;

    @Value("${azure.storage.container-name-video-container}")
    public String videoContainerName;

    @Value("${azure.storage.container-name-image-container}")
    public String imageContainerName;

    @Value("${azure.storage.container-name-keywords-container}")
    public String keywordsContainerName;

    @Value("${azure.frontdoor.url}")
    public String azureFrontdoorUrl;

    @Value("${azure.storage.container-name-profile-image-container}")
    public String profileImageContainerName;

    @Value("${azure.storage.container-name-audio-container}")
    public String audioContainerName;

    @Value("${kafka.eventhub.namespace:}")
    public String namespace;

    @Value("${kafka.username}")
    public String username;

    @Value("${kafka.password}")
    public String password;

    @Value("${application.audio.max.size}")
    public long MAX_FILE_SIZE_BYTES;
}
