package com.epam.elta.elta_spark_course_project;

import lombok.SneakyThrows;
import org.apache.spark.api.java.function.FlatMapGroupsWithStateFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.GroupStateTimeout;
import org.apache.spark.sql.streaming.OutputMode;

import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.spark.sql.functions.*;

public class Main {
    @SneakyThrows
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("spark streaming")
                .config("spark.master", "local")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        Dataset<Row> ds = spark.readStream()
                .schema(Encoders.bean(Event.class).schema())
                .json("src/main/resources/data")
                .as(Encoders.bean(Event.class))
                .groupByKey((MapFunction<Event, String>) Event::getTrackId, Encoders.STRING())
                .flatMapGroupsWithState(
                        calculateTrackInfoFlatMap(),
                        OutputMode.Update(),
                        Encoders.bean(EventState.class),
                        Encoders.bean(AggregatedEventsResult.class),
                        GroupStateTimeout.NoTimeout())
                .withColumn("lineString", calculateLineString())
                .withColumn("bearing", calculateBearing())
                .withColumn("distance", calculateDistance())
                .withColumn("velocity", calculateVelocity());

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

    private static Column calculateLineString() {
        return concat_ws(",", transform(col("events"), eventCol -> concat(eventCol.getField("lon"), lit(" "), eventCol.getField("lat"))));
    }

    private static Column calculateBearing() {
        Column lat1Col = element_at(col("events.lat"), 1);
        Column lat2Col = element_at(col("events.lat"), -1);
        Column lon1Col = element_at(col("events.lon"), 1);
        Column lon2Col = element_at(col("events.lon"), -1);

        Column yCol = sin(lon2Col.minus(lon1Col)).multiply(cos(lat2Col));
        Column xCol = cos(lat1Col).multiply(sin(lat2Col)).minus(sin(lat1Col).multiply(cos(lat2Col)).multiply(cos(lon2Col.minus(lon1Col))));
        Column thetaCol = atan2(yCol, xCol);
        return pmod(thetaCol.multiply(180).divide(lit(Math.PI)).plus(360), lit(360));
    }

    private static Column calculateDistance() {
        Column lat1RadCol = element_at(col("events.lat"), 1).multiply(Math.PI / 180);
        Column lat2RadCol = element_at(col("events.lat"), -1).multiply(Math.PI / 180);
        Column deltaLatCol = element_at(col("events.lat"), -1)
                .minus(element_at(col("events.lat"), 1))
                .multiply(Math.PI / 180);
        Column deltaLonCol = element_at(col("events.lon"), -1)
                .minus(element_at(col("events.lon"), 1))
                .multiply(Math.PI / 180);

        Column a = pow(sin(deltaLatCol.divide(2)), lit(2))
                .plus(cos(lat1RadCol).multiply(cos(lat2RadCol)).multiply(pow(sin(deltaLonCol.divide(2)), lit(2))));

        Column c = lit(2).multiply(atan2(sqrt(a), sqrt(lit(1).minus(a))));

        int earthRadiusMiters = 6371000;

        Column distance = lit(earthRadiusMiters).multiply(c);
        return round(distance).divide(1000);
    }

    private static Column calculateVelocity() {
        Column t1Col = element_at(col("events.eventTime"), 1);
        Column t2Col = element_at(col("events.eventTime"), -1);
        //Column velocityCol = col("distance").divide(unix_timestamp(t2Col).minus(unix_timestamp(t1Col))); m/s
        return col("distance").divide(unix_timestamp(t2Col).minus(unix_timestamp(t1Col)).divide(60 * 60)); //km/h
    }

    private static Column calculatePathDistance() {

    }

    private static Column aggregateEventsPath(Column event1, Column event2) {
        Column lat1RadCol = element_at(col("events.lat"), 1).multiply(Math.PI / 180);
        Column lat2RadCol = element_at(col("events.lat"), -1).multiply(Math.PI / 180);
        Column deltaLatCol = element_at(col("events.lat"), -1)
                .minus(element_at(col("events.lat"), 1))
                .multiply(Math.PI / 180);
        Column deltaLonCol = element_at(col("events.lon"), -1)
                .minus(element_at(col("events.lon"), 1))
                .multiply(Math.PI / 180);

        Column a = pow(sin(deltaLatCol.divide(2)), lit(2))
                .plus(cos(lat1RadCol).multiply(cos(lat2RadCol)).multiply(pow(sin(deltaLonCol.divide(2)), lit(2))));

        Column c = lit(2).multiply(atan2(sqrt(a), sqrt(lit(1).minus(a))));

        int earthRadiusMiters = 6371000;

        Column distance = lit(earthRadiusMiters).multiply(c);
        return round(distance).divide(1000);
    }
}
