package com.cognitube.consumer.consumer.dao;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Haozhe Zhang
 * @version 1.0
 * @project cognitube-consumers
 * @description For processing ai results from ai service
 * @date 2024/7/21 21:15:55
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class VideoAiDataMessage implements Message {
    private String videoId;
    private boolean success;
    private String error;
    private String keywordsUrl;
    private String transcriptUrl;
    private String subtitleUrl;
}
