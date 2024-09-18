package com.cognitube.consumer.model.response;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

/**
 * @author Haozhe Zhang
 * @version 1.0
 * @project cognitube-consumers
 * @description
 * @date 2024/9/16 23:42:17
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@Data
public class AzureTranscriptionResponse {
    @JsonProperty("links")
    private Links links;

    @JsonProperty("status")
    private String status;

    @JsonProperty("displayName")
    private String videoId;

    @Data
    public static class Links {
        @JsonProperty("files")
        private String files;
    }
}
