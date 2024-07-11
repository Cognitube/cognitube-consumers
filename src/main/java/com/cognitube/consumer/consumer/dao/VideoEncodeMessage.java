package com.cognitube.consumer.consumer.dao;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author Yijing Yang
 * @version 1.0
 * @project consumer
 * @description Message for video encode subtasks
 * @date 2024/7/10 20:20:20
 */
@Data
@AllArgsConstructor
public class VideoEncodeMessage implements Message {
    Double startindex;
    Double endindex;
    String keywordsUrl;
    VideoUploadMessage videoUploadMessage;
}
