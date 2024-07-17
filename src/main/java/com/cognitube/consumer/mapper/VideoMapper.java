package com.cognitube.consumer.mapper;

import com.cognitube.consumer.model.Video;
import org.apache.ibatis.annotations.Mapper;


//TODO: split table
@Mapper
public interface VideoMapper {

    /**
     * Update video
     * @param video
     */
    void updateVideo(Video video);

}
