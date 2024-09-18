package com.cognitube.consumer.consumer.dao;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

/**
 * @author Haozhe Zhang
 * @version 1.0
 * @project cognitube-consumers
 * @description
 * @date 2024/9/16 18:47:47
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder(builderClassName = "Builder", setterPrefix = "set")
public class VideoKeywordFetchMessage implements Message {
    private String videoId;
    private String id;
    private Long retryTime;
}
