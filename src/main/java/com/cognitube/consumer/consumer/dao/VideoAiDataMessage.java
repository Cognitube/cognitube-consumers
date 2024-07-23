package com.cognitube.consumer.consumer.dao;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author Haozhe Zhang
 * @version 1.0
 * @project cognitube-consumers
 * @description For processing ai results from ai service
 * @date 2024/7/21 21:15:55
 */
@Data
@AllArgsConstructor
public class VideoAiDataMessage implements Message {
    String videoId;
    boolean success;
    String error;
    String keywordsUrl;
    String transcriptUrl;
}
