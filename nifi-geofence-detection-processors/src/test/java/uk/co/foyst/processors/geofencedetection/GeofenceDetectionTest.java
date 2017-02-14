/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.co.foyst.processors.geofencedetection;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.hamcrest.collection.IsEmptyCollection;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;


public class GeofenceDetectionTest {

    private TestRunner testRunner;

    private static final String DUBAI_GEOFENCE_JSON = "{\n" +
            "  \"type\": \"Polygon\",\n" +
            "  \"coordinates\": [\n" +
            "    [\n" +
            "      [\n" +
            "        55.12218475341796,\n" +
            "        24.93407808260045\n" +
            "      ],\n" +
            "      [\n" +
            "        55.16338348388672,\n" +
            "        24.920690474438743\n" +
            "      ],\n" +
            "      [\n" +
            "        55.20011901855468,\n" +
            "        24.893288048031646\n" +
            "      ],\n" +
            "      [\n" +
            "        55.218658447265625,\n" +
            "        24.8531075892375\n" +
            "      ],\n" +
            "      [\n" +
            "        55.19462585449219,\n" +
            "        24.84469607296012\n" +
            "      ],\n" +
            "      [\n" +
            "        55.162010192871094,\n" +
            "        24.858091921595147\n" +
            "      ],\n" +
            "      [\n" +
            "        55.12012481689453,\n" +
            "        24.874289614290948\n" +
            "      ],\n" +
            "      [\n" +
            "        55.101585388183594,\n" +
            "        24.90107344705306\n" +
            "      ],\n" +
            "      [\n" +
            "        55.12218475341796,\n" +
            "        24.93407808260045\n" +
            "      ]\n" +
            "    ]\n" +
            "  ]\n" +
            "}";

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(GeofenceDetection.class);
    }

    @Test
    public void shouldRoutePositionalUpdateToInsideGeofenceRelationship() {

        final String position_within_geofence = "{\"name\":\"asmgcs.dxb.cat11\",\"payload\":" +
                "{\"timeOfDay\":\"2015-08-17T07:21:21.109Z\",\"callsign\":\"KNE410 \",\"destination\":\"OMDB\"," +
                "\"origin\":\"OEJN\",\"altitude\":\"350\",\"latitude\":\"24.890796616653954\"," +
                "\"longitude\":\"55.15033721923828\",\"aircraftType\":\"A320\",\"speed\":14.017261978166859," +
                "\"heading\":213.69006752597977,\"units\":\"knots\",\"feedKey\":\"5245\",\"source\":null," +
                "\"feedType\":\"CAT_11\",\"xvelocity\":-7.7753779698296,\"yvelocity\":-11.663066954744401}}";

        testRunner.setProperty("Geofence Definition", DUBAI_GEOFENCE_JSON);
        testRunner.enqueue(position_within_geofence);

        testRunner.run();
        testRunner.assertValid();
        List<MockFlowFile> positionsInsideGeofence = testRunner.getFlowFilesForRelationship(GeofenceDetection.INSIDE_GEOFENCE_RELATIONSHIP);
        List<MockFlowFile> positionsOutsideGeofence = testRunner.getFlowFilesForRelationship(GeofenceDetection.OUTSIDE_GEOFENCE_RELATIONSHIP);

        assertThat(positionsInsideGeofence, hasSize(1));
        assertThat(positionsOutsideGeofence, IsEmptyCollection.empty());
    }

    @Test
    public void shouldRoutePositionalUpdateToOutsideGeofenceRelationship() {

        final String position_within_geofence = "{\"name\":\"asmgcs.dxb.cat11\",\"payload\":" +
                "{\"timeOfDay\":\"2015-08-17T07:21:21.109Z\",\"callsign\":\"KNE410 \",\"destination\":\"OMDB\"," +
                "\"origin\":\"OEJN\",\"altitude\":\"350\",\"latitude\":\"24.887370816432064\"," +
                "\"longitude\":\"55.208015441894524\",\"aircraftType\":\"A320\",\"speed\":14.017261978166859," +
                "\"heading\":213.69006752597977,\"units\":\"knots\",\"feedKey\":\"5245\",\"source\":null," +
                "\"feedType\":\"CAT_11\",\"xvelocity\":-7.7753779698296,\"yvelocity\":-11.663066954744401}}";

        testRunner.setProperty("Geofence Definition", DUBAI_GEOFENCE_JSON);
        testRunner.enqueue(position_within_geofence);

        testRunner.run();
        testRunner.assertValid();
        List<MockFlowFile> positionsInsideGeofence = testRunner.getFlowFilesForRelationship(GeofenceDetection.INSIDE_GEOFENCE_RELATIONSHIP);
        List<MockFlowFile> positionsOutsideGeofence = testRunner.getFlowFilesForRelationship(GeofenceDetection.OUTSIDE_GEOFENCE_RELATIONSHIP);

        assertThat(positionsInsideGeofence, IsEmptyCollection.empty());
        assertThat(positionsOutsideGeofence, hasSize(1));
    }

}
