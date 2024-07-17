package com.cognitube.consumer.typehandler;


import com.cognitube.consumer.enums.VideoStatus;

/**
 * @author Haozhe Zhang
 * @version 1.0
 * @project cognitube-backend
 * @description TypeHandler for Video Status
 * @date 2024/5/23 22:06:42
 */
public class VideoStatusTypeHandler extends GenericEnumTypeHandler<VideoStatus> {
    public VideoStatusTypeHandler() {
        super(VideoStatus.class);
    }
}
