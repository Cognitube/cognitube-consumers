package com.cognitube.consumer.consumer.dao;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Haozhe Zhang
 * @version 1.0
 * @project cognitube-consumers
 * @description For sending AI extraction request
 * @date 2024/8/11 13:55:39
 */
@AllArgsConstructor
@Data
@NoArgsConstructor
public class VideoAudioExtractionMessage implements Message {
    private String videoId;
    private String audioUrl;
    private boolean success;
    private String error;
    private int retryCount;
}
