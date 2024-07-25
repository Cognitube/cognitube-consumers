package com.cognitube.consumer.model;

import com.cognitube.consumer.enums.NotificationType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

/**
 * @author Haozhe Zhang
 * @version 1.0
 * @project cognitube-consumers
 * @description System notification
 * @date 2024/7/22 18:22:48
 */
@AllArgsConstructor
@Data
@Builder(builderClassName = "Builder", setterPrefix = "set")
public class Notification {
    private final Long id; // auto
    private final Long receiverId; // required
    private final Long senderId; // required
    private final String title; // optional
    private final String content; // required
    private final NotificationType notificationType; // required
    private final Integer isUnread; // default to unread
    private final String dateCreated; // auto
}
