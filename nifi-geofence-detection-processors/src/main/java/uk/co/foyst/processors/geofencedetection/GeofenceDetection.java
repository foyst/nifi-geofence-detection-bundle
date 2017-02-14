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

import com.jsoniter.JsonIterator;
import com.jsoniter.any.Any;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.geotools.geojson.geom.GeometryJSON;
import sun.java2d.opengl.OGLRenderQueue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

@Tags({"geofence"})
@CapabilityDescription("Given a geofence defined in json, detects if incoming positional updates are within it")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class GeofenceDetection extends AbstractProcessor {

    public static final PropertyDescriptor GEOFENCE_DEFINITION_PROPERTY = new PropertyDescriptor
            .Builder().name("Geofence Definition")
            .displayName("Geofence Definition")
            .description("Geofence Definition as JSON")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship INSIDE_GEOFENCE_RELATIONSHIP = new Relationship.Builder()
            .name("INSIDE_GEOFENCE_RELATIONSHIP")
            .description("For Positional Updates inside configured Geofence")
            .build();

    public static final Relationship OUTSIDE_GEOFENCE_RELATIONSHIP = new Relationship.Builder()
            .name("OUTSIDE_GEOFENCE_RELATIONSHIP")
            .description("For Positional Updates outside configured Geofence")
            .build();

    private static final GeometryJSON GEOMETRY_JSON = new GeometryJSON();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(GEOFENCE_DEFINITION_PROPERTY);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(INSIDE_GEOFENCE_RELATIONSHIP);
        relationships.add(OUTSIDE_GEOFENCE_RELATIONSHIP);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }
        final AtomicBoolean value = new AtomicBoolean(false);
        final String predefinedGeofenceString = context.getProperty(GEOFENCE_DEFINITION_PROPERTY).getValue();
        // Convert GEOJSON to Geometry
        final Geometry fenceGeometry;
        try {
            fenceGeometry = GEOMETRY_JSON.read(predefinedGeofenceString);
        } catch (IOException e) {
            getLogger().error("Failed to read geofence json.", e);
            return;
        }

        //Parse FlowFile contents to JSON
        session.read(flowFile, in -> {
            try{
                final String json = IOUtils.toString(in, "utf8");

                //Get Longitude and Latitude attributes
                final Any jsonIter = JsonIterator.deserialize(json).get("payload");
                final double longitude = jsonIter.toDouble("longitude");
                final double latitude = jsonIter.toDouble("latitude");
                final Coordinate coordinate = new Coordinate(longitude, latitude);

                //Check if Co-ordinates fall within defined Geofence
                final GeometryFactory geometryFactory = new GeometryFactory();
                final Point point = geometryFactory.createPoint(coordinate);
                final boolean inGeofence = point.within(fenceGeometry);

                value.set(inGeofence);
            }catch(Exception ex){
                ex.printStackTrace();
                getLogger().error("Failed to read json string.");
            }
        });

//        try {
//            Thread.sleep(20);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }

        //Route FlowFile (Positional Update) accordingly
        if (value.get()) session.transfer(flowFile, INSIDE_GEOFENCE_RELATIONSHIP);
        else session.transfer(flowFile, OUTSIDE_GEOFENCE_RELATIONSHIP);
    }
}
