**Spark Structured streaming project**

As a Big Data engineer, you’ve received the following task - you need to perform stream processing of events coming from sensors installed on drones. “Event” looks like following:

```
{
    "id":1,
    "trackId":"749f1259-1e3b-40bc-92dd-142224ea3590",
    "eventTime":"2022-06-23T00:00:55.000+03:00",
    "lon":0.227893604428281,
    "lat":0.665196507004088
}
```

From these events you are asked to develop following flow:\
**Part 1:**
* Read events from files located at - src/main/resources/data
* Process events within 10 minutes window for each track
* If window not ended yet and you’ve already received more than 2 events, provide result as intermediate data
* Count events per window
* Write result to console

    * Result will looks like following:
        ```
        {
            “trackId”:"749f1259-1e3b-40bc-92dd-142224ea3590",
            “events”: [{"id":1, …}, {"id":2, …}, {"id":3, …}],
            “eventsCount”: 3,
            “isIntermediate”: true
        }
        ```
    * Once you finish, you will get the document with explained cases and examples of expected results.



**Part 2:**
* Using Spark SQL API on Dataframe, calculate line string per window\
```LINESTRING (x1 y1, x2 y3, x3 y3)```
    
**Part 3:**
* Using Spark SQL API on Dataframe, calculate bearing per window, between first and last point in window
    ```
    Formula:
    y = sin(lon2-lon1) * cos(lat2)
    x = cos(lat1)*sin(lat2) - sin(lat1)*cos(lat2)*cos(lon2-lon1)
    θ = atan2(y, x)
    bearing = (θ*180/PI + 360) % 360 // in degrees
    ```

**Part 4:**
* Using Spark SQL API on Dataframe, calculate distance per window
    ```
    Formula:
    R = 6371e3 // earth’s radius in metres
    latRad1 = lat1 * PI/180 // in radians
    latRad2 = lat2 * PI/180
    ΔlatRad = (lat2-lat1) * PI/180
    ΔlonRad = (lon2-lon1) * PI/180
    
    a = sin(ΔlatRad/2) * sin(ΔlatRad/2) + cos(latRad1) * cos(latRad2) * sin(ΔlonRad/2) * sin(ΔlonRad/2)
    const c = 2 * atan2(sqrt(a), sqrt(1-a))
    
    const d = R * c // in metres
    ```

**Part 5:**
* Using Spark SQL API on Dataframe, calculate avg velocity per window
