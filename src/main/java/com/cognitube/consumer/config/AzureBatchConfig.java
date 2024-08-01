package com.cognitube.consumer.config;

import com.microsoft.azure.batch.BatchClient;
import com.microsoft.azure.batch.auth.BatchSharedKeyCredentials;
import lombok.AllArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author Haozhe Zhang
 * @version 1.0
 * @project cognitube-consumers
 * @description Azure batch client
 * @date 2024/8/1 14:42:22
 */
@Configuration
@AllArgsConstructor
public class AzureBatchConfig {
    private String batchAccountUrl;
    private String batchAccountName;
    private String batchAccountKey;

    @Bean
    public BatchClient batchClient() {
        BatchSharedKeyCredentials credentials = new BatchSharedKeyCredentials(
                batchAccountUrl, batchAccountName, batchAccountKey);
        return BatchClient.open(credentials);
    }
}