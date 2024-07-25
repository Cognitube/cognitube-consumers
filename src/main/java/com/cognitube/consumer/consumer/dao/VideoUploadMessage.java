package com.cognitube.consumer.consumer.dao;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Yijing Yang
 * @version 1.0
 * @project cognitube-backend
 * @description Message for video upload
 * @date 2024/5/23 20:20:20
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class VideoUploadMessage implements Message {
    private String videoId;
    private String videoUrl;
    private Long userId;
    private Integer retryCount;
    private String videoName;
}
