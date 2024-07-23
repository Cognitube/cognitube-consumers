package com.cognitube.consumer.dto.request;

import com.cognitube.consumer.enums.NotificationType;

/**
 * @author Qihang Ao
 * @version 1.0
 * @project cognitube-backend
 * @description CreateMessageRequest Class
 * @date 2024/6/29 10:20:44
 */
public record CreateNotificationRequest(
    Long receiverId,
    Long senderId,
    String title,
    String content,
    NotificationType notificationType) {
}
