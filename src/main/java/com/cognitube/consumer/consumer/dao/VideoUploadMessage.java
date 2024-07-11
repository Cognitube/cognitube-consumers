package com.cognitube.consumer.consumer.dao;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author Yijing Yang
 * @version 1.0
 * @project cognitube-backend
 * @description Message for video upload
 * @date 2024/5/23 20:20:20
 */
@Data
@AllArgsConstructor
public class VideoUploadMessage implements Message {
    private Long videoId;
    private String videoName;
    private String imageUrl;
    private String videoUrl;
    private Long userId;
    private String description;
    private String originalFilename;
}
