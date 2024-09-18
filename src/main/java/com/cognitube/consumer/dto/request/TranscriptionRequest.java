package com.cognitube.consumer.dto.request;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

/**
 * @author Haozhe Zhang
 * @version 1.0
 * @project cognitube-consumers
 * @description
 * @date 2024/9/17 21:21:15
 */
@Data
@AllArgsConstructor
@Builder(builderClassName = "Builder", setterPrefix = "set")
public class TranscriptionRequest {
    private String id;
}
