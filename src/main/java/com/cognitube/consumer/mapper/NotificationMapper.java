package com.cognitube.consumer.mapper;

import com.cognitube.consumer.model.Notification;
import org.apache.ibatis.annotations.Mapper;

/**
 * @author Haozhe Zhang
 * @version 1.0
 * @project cognitube-consumers
 * @description Methods for system notifications
 * @date 2024/7/22 18:21:45
 */
@Mapper
public interface NotificationMapper {

    void insertNotification(Notification notification);
}
