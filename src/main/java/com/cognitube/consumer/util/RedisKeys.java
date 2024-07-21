package com.cognitube.consumer.util;

/**
 * @author Haozhe Zhang
 * @version 1.0
 * @project cognitube-consumers
 * @description Redis keys
 * @date 2024/7/17 13:27:58
 */
public class RedisKeys {

    private static final String SPLITTER = ":";

    private static final String PREFIX_VIDEO_PROCESSING_STATUS = "video-processing-status";

    public static String getVideoProcessingStatusKey(String videoId) {
        return PREFIX_VIDEO_PROCESSING_STATUS + SPLITTER + videoId;
    }
}
