package com.epam.elta.elta_spark_course_project;

import lombok.SneakyThrows;
import org.apache.spark.api.java.function.FlatMapGroupsWithStateFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.GroupStateTimeout;
import org.apache.spark.sql.streaming.OutputMode;

import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

public class Main {
    @SneakyThrows
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("spark streaming")
                .config("spark.master", "local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        Dataset<AggregatedEventsResult> ds = spark.readStream()
                .schema(Encoders.bean(Event.class).schema())
                .json("src/main/resources/data")
                .as(Encoders.bean(Event.class))
                .groupByKey((MapFunction<Event, String>) Event::getTrackId, Encoders.STRING())
                .flatMapGroupsWithState(
                        calculateTrackInfoFlatMap(),
                        OutputMode.Update(),
                        Encoders.bean(EventState.class),
                        Encoders.bean(AggregatedEventsResult.class),
                        GroupStateTimeout.NoTimeout());

        ds.writeStream()
                .format("console")
                .option("truncate", false)
                .outputMode("update")
                .start()
                .awaitTermination();
    }

    private static FlatMapGroupsWithStateFunction<String, Event, EventState, AggregatedEventsResult> calculateTrackInfoFlatMap() {
        return (key, values, groupState) -> {
            EventState eventState = groupState.getOption().getOrElse(() -> EventState.builder().events(new ArrayList<>()).build());
            ArrayList<AggregatedEventsResult> results = new ArrayList<>();

            AtomicReference<AggregatedEventsResult> wrappedResult = new AtomicReference<>();
            values.forEachRemaining(event -> {
                if (eventState.getEvents().isEmpty()) {
                    eventState.setStartEventTime(event.getEventTime());
                    eventState.getEvents().add(event);
                    wrappedResult.set(AggregatedEventsResult.builder()
                            .trackId(key)
                            .events(new ArrayList<>(eventState.getEvents()))
                            .eventsCount(eventState.getEvents().size())
                            .isIntermediate(true)
                            .build());
                } else if (ChronoUnit.MINUTES.between(eventState.getStartEventTime(), event.getEventTime()) >= 10) {
                    Optional.ofNullable(wrappedResult.get()).map(r -> {
                        r.setIntermediate(false);
                        return r;
                    }).ifPresentOrElse(results::add, () -> {
                        AggregatedEventsResult result = AggregatedEventsResult.builder()
                                .trackId(key)
                                .events(new ArrayList<>(eventState.getEvents()))
                                .eventsCount(eventState.getEvents().size())
                                .isIntermediate(false)
                                .build();
                        results.add(result);
                    });
                    eventState.setStartEventTime(event.getEventTime());
                    eventState.setEvents(new ArrayList<>(Collections.singletonList(event)));
                    wrappedResult.set(AggregatedEventsResult.builder()
                            .trackId(key)
                            .events(new ArrayList<>(Collections.singletonList(event)))
                            .eventsCount(1)
                            .isIntermediate(true)
                            .build()
                    );
                } else if (eventState.getEvents().size() >= 2) {
                    eventState.getEvents().add(event);
                    Optional.ofNullable(wrappedResult.get()).ifPresentOrElse(r -> {
                                r.setIntermediate(true);
                                r.getEvents().add(event);
                                r.setEventsCount(r.getEvents().size());
                            }, () -> wrappedResult.set(
                            AggregatedEventsResult.builder()
                                    .trackId(key)
                                    .events(new ArrayList<>(eventState.getEvents()))
                                    .eventsCount(eventState.getEvents().size())
                                    .isIntermediate(true)
                                    .build())
                    );
                } else {
                    eventState.getEvents().add(event);
                    Optional.ofNullable(wrappedResult.get()).ifPresentOrElse(r -> {
                        r.getEvents().add(event);
                        r.setEventsCount(r.getEvents().size());
                    }, () -> wrappedResult.set(
                            AggregatedEventsResult.builder()
                                    .trackId(key)
                                    .events(new ArrayList<>(eventState.getEvents()))
                                    .eventsCount(eventState.getEvents().size())
                                    .isIntermediate(true)
                                    .build()));
                }
            });
            Optional.ofNullable(wrappedResult.get()).ifPresent(results::add);
            groupState.update(eventState);
            return results.iterator();
        };
    }
}
