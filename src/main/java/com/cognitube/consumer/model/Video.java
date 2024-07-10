package com.cognitube.consumer.model;

import com.cognitube.consumer.enums.VideoStatus;
import lombok.*;

// TODO: nonnull check & redesign
@AllArgsConstructor
@Data
@Builder(builderClassName = "Builder", setterPrefix = "set")
public class Video {
    private final Long id;
    private final String title;
    private final String fileLink;
    private final String coverImageLink;
    private final Long authorId;
    private final Double length;
    private final String keywordsLink;
    private final Integer isDeleted;
    private final String description;
    private final String dateCreated;
    private final VideoStatus status;
}

