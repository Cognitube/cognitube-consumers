package com.cognitube.consumer.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * @author Haozhe Zhang
 * @version 1.0
 * @project cognitube-consumers
 * @description Enum for notifications
 * @date 2024/7/22 18:23:39
 */
@Getter
@AllArgsConstructor
public enum NotificationType {
    SYSTEM_NOTIFICATION("SYSTEM_NOTIFICATION"),
    UNKNOWN_TYPE("UNKNOWN_TYPE");

    private final String type;
}
