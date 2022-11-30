package com.epam.elta.elta_spark_course_project;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

//TODO: 1. linestring 2. distance 3. bearing between start/end 4. avg speed 5. displacement 6. velocity
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Event {
    private Long id;
    private Instant eventTime;
    private String trackId;
    private Double lon;
    private Double lat;
}
