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

    /**
     * Get video by id
     * @param videoId
     * @return
     */
    Double getVideoDuration(String videoId);

}
