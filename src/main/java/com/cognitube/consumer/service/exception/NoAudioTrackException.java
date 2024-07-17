package com.cognitube.consumer.service.exception;

/**
 * @author Haozhe Zhang
 * @version 1.0
 * @project cognitube-backend
 * @description Exception for the case a video does not have audio track
 * @date 2024/6/23 00:16:59
 */
public class NoAudioTrackException extends RuntimeException {
    public NoAudioTrackException(String message) {
        super(message);
    }

    public NoAudioTrackException(String message, Throwable cause) {
        super(message, cause);
    }
}
