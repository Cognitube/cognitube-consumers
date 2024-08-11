package com.cognitube.consumer.consumer.dao;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Yijing Yang
 * @version 1.0
 * @project consumer
 * @description Message for video encode subtasks
 * @date 2024/7/10 20:20:20
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder(builderClassName = "Builder", setterPrefix = "set")
public class VideoEncodeMessage implements Message {
    private String videoId;
    private String videoUrl;
    private boolean success;
    private double videoDuration;
    private String error;
    private int retryCount;
}
