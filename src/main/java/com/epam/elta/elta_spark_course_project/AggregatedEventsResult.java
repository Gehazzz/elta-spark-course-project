package com.epam.elta.elta_spark_course_project;

import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder
public class AggregatedEventsResult {
    private String trackId;
    private List<Event> events;
    private Integer eventsCount;
    private boolean isIntermediate;
}
