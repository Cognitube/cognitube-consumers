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

    private static final String PREFIX_VIDEO_PROCESSING_STATUS = "video:processing:status";
    
    private static final String USER_WEEKLY_UPLOAD_LIMIT = "user:weekly:upload:limit";

    private static final String PREFIX_VIDEO_PROCESSING_LOCK = "video:processing:lock";

    private static final String PREFIX_VIDEO_PROCESSING_OVERTIME = "video:processing:overtime";

    private static final String PREFIX_VIDEO_PROCESSING_OVERTIME_ALL_CHARS = "video:processing:overtime:all:chars";

    public static String getVideoProcessingStatusKey(String videoId) {
        return PREFIX_VIDEO_PROCESSING_STATUS + SPLITTER + videoId;
    }

    public static String getUserWeeklyUploadLimitKey(Long userId, String dateString) {
        return USER_WEEKLY_UPLOAD_LIMIT + SPLITTER + userId + SPLITTER + dateString;
    }

    public static String getVideoProcessingLockKey(String videoId) {
        return PREFIX_VIDEO_PROCESSING_LOCK + SPLITTER + videoId;
    }

    public static String getVideoProcessingOvertimeKey(String videoId) {
        return PREFIX_VIDEO_PROCESSING_OVERTIME + SPLITTER + videoId.substring(videoId.length() - 1);
    }

    public static String getVideoProcessingOvertimeAllCharsKey() {
        return PREFIX_VIDEO_PROCESSING_OVERTIME_ALL_CHARS;
    }
}
