package com.cognitube.consumer.enums;

/**
 * @author Haozhe Zhang
 * @version 1.0
 * @project cognitube-backend
 * @description Status of video
 * @date 2024/5/23 20:30:52
 */
public enum VideoStatus implements BaseEnum {
    PROCESSING(0),
    OK(1),
    UNDERPROCESSED(2);

    private final int value;

    VideoStatus(int value) {
        this.value = value;
    }

    @Override
    public int getValue() {
        return value;
    }

    public static VideoStatus fromValue(int value) {
        for (VideoStatus status : values()) {
            if (status.getValue() == value) {
                return status;
            }
        }
        throw new IllegalArgumentException("Unknown value: " + value);
    }
}
