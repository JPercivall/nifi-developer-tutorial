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
package org.apache.nifi.tutorial.geecon.processors;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;


/*
 * Next you will try for yourself to modify a FlowFile. Simply uncomment one of the example methods to interact with the FlowFile.
 *
 * To add your updated processor to the instance, run "mvn clean install" in the "./geecon-tutorial-bundle/" folder. This will put the built nar
 * here "nifi-geecon-tutorial-nar/target/nifi-geecon-tutorial-nar-1.0-SNAPSHOT.nar". Simply copy that nar file to the lib folder of your NiFi
 * instance "nifi-0.6.1/lib/" and start/restart the instance.
 *
 * The NiFi framework's design lends itself very well to act as a wrapper for other Java Libraries. For an example, checkout the ExtractImageMetadata
 * processor in the NiFi standard bundle (github link below). It was the first processor I ever wrote and it simply utilizes an already written
 * Image Metadata extractor java library to grab the metadata key value pairs from the FlowFile content and add them as attributes of the FlowFile.
 *
 * https://github.com/apache/nifi/blob/4afd8f88f8a34cf87f2a06221667166a54c99a15/nifi-nar-bundles/nifi-image-bundle/nifi-image-processors/src/main/java/org/apache/nifi/processors/image/ExtractImageMetadata.java#L65-L65
 */

@Tags({"example"})
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class MyProcessor extends AbstractProcessor {

    public static final PropertyDescriptor MY_PROPERTY = new PropertyDescriptor
            .Builder().name("My Property")
            .description("Example Property")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship MY_RELATIONSHIP = new Relationship.Builder()
            .name("my_relationship")
            .description("Example relationship")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(MY_PROPERTY);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(MY_RELATIONSHIP);
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

        /*
         * Read an attribute from a FlowFile simply by using the getter:
         *
         * VALUE = flowFile.getAttribute(KEY);
         */

        /*
         *  Add an attribute to the FlowFile using "putAttribute":
         *
         *  session.putAttribute(flowFile, KEY, VALUE);
         */

        /*
         * Just like in the previous example, this can be used to write to the FlowFile's contents:
         *
         * flowFile = session.write(flowFile, new OutputStreamCallback() {
         *      @Override
         *      public void process(final OutputStream rawOut) throws IOException {
         *          *** Write to the OutputStream here ***
         *      }
         * });
         */

        /*
         * In order to read the contents of a FlowFile simply use "session.read()" like so:
         *
         *   session.read(flowFile, new InputStreamCallback() {
         *       @Override
         *       public void process(InputStream in) throws IOException {
         *           *** Read InputStream here ***
         *       }
         *   });
         *
         * /

        /*
         * You can also read and write effectively in the same call like so:
         *
         *  flowFile = session.write(flowFile, new StreamCallback() {
         *     @Override
         *     public void process(InputStream in, OutputStream out) throws IOException {
         *         **** Read InputStream and write to the outputstream here ****
         *     }
         *  });
         */

        session.transfer(flowFile, MY_RELATIONSHIP);
    }
}
