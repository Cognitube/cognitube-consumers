package com.cognitube.consumer.service;

import com.cognitube.consumer.dto.request.CreateNotificationRequest;
import com.cognitube.consumer.enums.NotificationType;
import com.cognitube.consumer.mapper.NotificationMapper;
import com.cognitube.consumer.model.Notification;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.mybatis.spring.MyBatisSystemException;
import org.springframework.stereotype.Service;

/**
 * @author Haozhe Zhang
 * @version 1.0
 * @project cognitube-consumers
 * @description Service methods for handling notifications
 * @date 2024/7/22 18:18:31
 */
@Service
@Slf4j
@AllArgsConstructor
public class NotificationService {

    private final NotificationMapper notificationMapper;

    public void addSystemNotification(Long userId, String content) {
        CreateNotificationRequest notificationRequest = new CreateNotificationRequest(
                userId, // receiverId
                0L, // senderId (system)
                "System Notification",
                content,
                NotificationType.SYSTEM_NOTIFICATION
        );
        addNotification(notificationRequest);
    }

    private Notification addNotification(CreateNotificationRequest createNotificationRequest) {
        Notification notification = Notification.builder()
                .setReceiverId(createNotificationRequest.receiverId())
                .setSenderId(createNotificationRequest.senderId())
                .setTitle(createNotificationRequest.title())
                .setContent(createNotificationRequest.content())
                .setNotificationType(createNotificationRequest.notificationType())
                .setIsUnread(1)
                .build();
        try {
            notificationMapper.insertNotification(notification);
        } catch (Exception e) {
            throw new MyBatisSystemException(e);
        }
        return notification;
    }
}
