package com.cognitube.consumer.mapper;

import com.cognitube.consumer.model.Video;
import org.apache.ibatis.annotations.Mapper;


//TODO: split table
@Mapper
public interface VideoMapper {

    /**
     * Uploads a video to the database
     * @param video video object
     */
    void createVideo(Video video);


    /**
     * Update video
     * @param video
     */
    void updateVideo(Video video);

}
