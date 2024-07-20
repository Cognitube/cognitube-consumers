package com.cognitube.consumer.config;

import com.azure.storage.blob.BlobServiceClient;
import com.cognitube.consumer.consumer.VideoProcessConsumer;
import com.cognitube.consumer.producer.VideoProcessProducer;
import com.cognitube.consumer.service.BlobService;
import com.cognitube.consumer.service.VideoEncodingService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.RedisTemplate;

/**
 * @author Haozhe Zhang
 * @version 1.0
 * @project cognitube-consumers
 * @description Used for importing values via the Value annotation
 * @date 2024/7/20 17:02:31
 */
@Configuration
public class ConsumerConfig {

    @Bean
    public final BlobService blobService(
            BlobServiceClient blobServiceClient,
            @Value("${azure.storage.container-name-video-container}") String videoContainerName,
            @Value("${azure.storage.container-name-image-container}") String imageContainerName,
            @Value("${azure.storage.container-name-keywords-container}") String keywordsContainerName,
            @Value("${azure.storage.container-name-profile-image-container}") String profileImageContainerName,
            @Value("${azure.storage.container-name-audio-container}") String audioContainerName,
            @Value("${azure.storage.container-name-temp-video-container}") String tempVideoContainerName,
            @Value("${azure.frontdoor.url}") String azureFrontdoorUrl) {
        return new BlobService(
                blobServiceClient,
                videoContainerName,
                imageContainerName,
                keywordsContainerName,
                profileImageContainerName,
                audioContainerName,
                tempVideoContainerName,
                azureFrontdoorUrl
        );
    }

    @Bean
    public final VideoProcessConsumer videoProcessConsumer(
            ObjectMapper objectMapper,
            BlobService blobService,
            VideoProcessProducer videoProcessProducer,
            RedisTemplate<String, String> redisTemplate,
            VideoEncodingService videoEncodingService,
            @Value("${kafka.video.process.topic}") String KAFKA_VIDEO_PROCESS_TOPIC,
            @Value("${application.keyword.service.url}") String keywordServiceUrl,
            @Value("${azure.storage.blob.endpoint}") String blobEndpoint
    ) {
        return new VideoProcessConsumer(
                objectMapper,
                blobService,
                videoProcessProducer,
                redisTemplate,
                videoEncodingService,
                KAFKA_VIDEO_PROCESS_TOPIC,
                keywordServiceUrl,
                blobEndpoint
        );
    }

    @Bean
    public final KafkaConsumerConfig kafkaConsumerConfig(
            @Value("${kafka.eventhub.namespace}") String namespace,
            @Value("${kafka.username}") String username,
            @Value("${kafka.password}") String password
    ) {
        return new KafkaConsumerConfig(
                namespace,
                username,
                password);
    }

    @Bean
    public final VideoEncodingService videoEncodingService(
            @Value("${application.encoding.default.bitrate}") int DEFAULT_AUDIO_BIT_RATE_KBPS,
            @Value("${application.encoding.default.sampling-rate}") int DEFAULT_SAMPLING_RATE
    ) {
        return new VideoEncodingService(
                DEFAULT_AUDIO_BIT_RATE_KBPS,
                DEFAULT_SAMPLING_RATE
        );
    }
}

