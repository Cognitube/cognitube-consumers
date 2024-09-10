package com.cognitube.consumer.config;

import com.azure.storage.blob.BlobServiceClient;
import com.cognitube.consumer.consumer.VideoAiDataConsumer;
import com.cognitube.consumer.consumer.VideoAudioExtractionConsumer;
import com.cognitube.consumer.consumer.VideoEncodeConsumer;
import com.cognitube.consumer.consumer.VideoProcessConsumer;
import com.cognitube.consumer.mapper.NotificationMapper;
import com.cognitube.consumer.mapper.VideoMapper;
import com.cognitube.consumer.producer.VideoProcessProducer;
import com.cognitube.consumer.service.BlobService;
import com.cognitube.consumer.service.NotificationService;
import com.cognitube.consumer.service.VideoProcessingService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;

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
    public NotificationService notificationService(
            NotificationMapper notificationMapper
    ) {
        return new NotificationService(
                notificationMapper
        );
    }

    @Bean
    public VideoAiDataConsumer videoAiDataConsumer(
            ObjectMapper objectMapper,
            VideoMapper videoMapper,
            @Value("${kafka.video.ai.topic}") String KAFKA_VIDEO_AI_TOPIC,
            VideoProcessingService videoProcessingService
    ) {
        return new VideoAiDataConsumer(
                objectMapper,
                videoMapper,
                KAFKA_VIDEO_AI_TOPIC,
                videoProcessingService
        );
    }

    @Bean
    public VideoEncodeConsumer videoEncodeConsumer(
            ObjectMapper objectMapper,
            VideoMapper videoMapper,
            @Value("${kafka.video.reencode.topic}") String KAFKA_VIDEO_REENCODE_TOPIC,
            VideoProcessingService videoProcessingService,
            RedisTemplate<String, Double> redisTemplate
        ) {
        return new VideoEncodeConsumer(
                objectMapper,
                videoMapper,
                KAFKA_VIDEO_REENCODE_TOPIC,
                videoProcessingService,
                redisTemplate
        );
    }

    @Bean
    public VideoAudioExtractionConsumer videoAudioExtractionConsumer(
            ObjectMapper objectMapper,
            VideoProcessingService videoProcessingService,
            @Value("${application.keyword.service.url}") String keywordServiceUrl,
            @Value("${kafka.video.ai.topic}") String KAFKA_VIDEO_AI_TOPIC
    ) {
        return new VideoAudioExtractionConsumer(
                objectMapper,
                videoProcessingService,
                keywordServiceUrl,
                KAFKA_VIDEO_AI_TOPIC
        );
    }

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }

    @Bean
    public AzureConfig azureConfig(@Value("${azure.storage.connection-string}") String connectionString) {
        return new AzureConfig(connectionString);
    }

    @Bean
    public BlobService blobService(
            BlobServiceClient blobServiceClient,
            @Value("${azure.storage.container-name-video-container}") String videoContainerName,
            @Value("${azure.storage.container-name-image-container}") String imageContainerName,
            @Value("${azure.storage.container-name-keywords-container}") String keywordsContainerName,
            @Value("${azure.storage.container-name-profile-image-container}") String profileImageContainerName,
            @Value("${azure.storage.container-name-audio-container}") String audioContainerName,
            @Value("${azure.storage.container-name-temp-video-container}") String tempVideoContainerName,
            @Value("${azure.frontdoor.url}") String azureFrontdoorUrl,
            @Value("${CUSTOM_TEMP_DIR:}") String CUSTOM_TEMP_DIR
    ) {
        return new BlobService(
                blobServiceClient,
                videoContainerName,
                imageContainerName,
                keywordsContainerName,
                profileImageContainerName,
                audioContainerName,
                tempVideoContainerName,
                azureFrontdoorUrl,
                CUSTOM_TEMP_DIR
        );
    }

    @Bean
    public VideoProcessConsumer videoProcessConsumer(
            ObjectMapper objectMapper,
            VideoProcessProducer videoProcessProducer,
            @Value("${kafka.video.process.topic}") String KAFKA_VIDEO_PROCESS_TOPIC,
            VideoProcessingService videoProcessingService
    ) {
        return new VideoProcessConsumer(
                objectMapper,
                videoProcessProducer,
                KAFKA_VIDEO_PROCESS_TOPIC,
                videoProcessingService
        );
    }

    @Bean
    public KafkaConsumerConfig kafkaConsumerConfig(
            @Value("${kafka.eventhub.namespace}") String namespace,
            @Value("${kafka.username}") String username,
            @Value("${kafka.password}") String password,
            @Value("${kafka.bootstrap-servers}") String bootstrapServers
    ) {
        return new KafkaConsumerConfig(
                namespace,
                username,
                password,
                bootstrapServers
        );
    }

    @Bean
    public KafkaProducerConfig kafkaProducerConfig(
            @Value("${kafka.eventhub.namespace}") String namespace,
            @Value("${kafka.username}") String username,
            @Value("${kafka.password}") String password,
            @Value("${kafka.bootstrap-servers}") String bootstrapServers
    ) {
        return new KafkaProducerConfig(
                namespace,
                username,
                password,
                bootstrapServers
        );
    }

    @Bean
    public VideoProcessProducer videoProcessProducer(
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper
    ) {
        return new VideoProcessProducer(
                kafkaTemplate,
                objectMapper
        );
    }

    @Bean
    public VideoProcessingService videoProcessingService(
            VideoMapper videoMapper,
            RedisTemplate<String, String> redisTemplate,
            @Value("${kafka.video.ai.topic}") String KAFKA_VIDEO_AI_TOPIC,
            @Value("${kafka.video.reencode.topic}") String KAFKA_VIDEO_REENCODE_TOPIC,
            NotificationService notificationService,
            @Value("${application.transcoding.service.url}") String transcodingServiceUrl,
            @Value("${application.video.processing.overtime.threshold.in.hour}") Integer VIDEO_PROCESSING_OVERTIME_THRESHOLD_IN_HOUR
    ) {
        return new VideoProcessingService(
                videoMapper,
                redisTemplate,
                KAFKA_VIDEO_AI_TOPIC,
                KAFKA_VIDEO_REENCODE_TOPIC,
                notificationService,
                transcodingServiceUrl,
                VIDEO_PROCESSING_OVERTIME_THRESHOLD_IN_HOUR
        );
    }
}

