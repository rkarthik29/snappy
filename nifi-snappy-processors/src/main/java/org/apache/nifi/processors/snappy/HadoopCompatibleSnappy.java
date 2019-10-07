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
package org.apache.nifi.processors.snappy;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.FlowFileAccessException;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.xerial.snappy.SnappyHadoopCompatibleInputStream;
import org.xerial.snappy.SnappyHadoopCompatibleOutputStream;

@Tags({"example"})
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class HadoopCompatibleSnappy extends AbstractProcessor {

    public static final PropertyDescriptor ACTION = new PropertyDescriptor
            .Builder().name("ACTION")
            .displayName("Compress/DeCompress")
            .description("Compress or Decompress the file")
            .defaultValue("Compress")
            .allowableValues("Compress","DeCompress")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor SNAPPY_BLOCK_SIZE = new PropertyDescriptor.Builder()
            .name("SNAPPY_BLOCK_SIZE")
            .description("Specifies the Block Size for the Snappy Haddop Compressed File")
            .required(true)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .defaultValue("32 KB")
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("SUCCESS")
            .description("SUCCESS Relationship")
            .build();
    
    public static final Relationship FAILURE = new Relationship.Builder()
            .name("FAILURE")
            .description("FAILURE Relationship")
            .build();
    
    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(ACTION);
        descriptors.add(SNAPPY_BLOCK_SIZE);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS);
        relationships.add(FAILURE);
        //relationships.add(ORIGINAL);
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
        
        String action = context.getProperty(ACTION).getValue();
        int blockSize = context.getProperty(SNAPPY_BLOCK_SIZE).asDataSize(DataUnit.B).intValue();
        //FlowFile outputFF=null;
        try {
	        if("DeCompress".equals(action)) {
	        	flowFile=session.write(flowFile, new StreamCallback() {
					
					@Override
					public void process(InputStream in, OutputStream out) throws IOException {
						// TODO Auto-generated method stub
						SnappyHadoopCompatibleInputStream siut = new SnappyHadoopCompatibleInputStream(in);
				        BufferedInputStream input = new BufferedInputStream(siut);
	
				        byte[] tmp = new byte[1024];
				        for (int readBytes = 0; (readBytes = input.read(tmp)) != -1;) {
				        	out.write(tmp, 0, readBytes);
				        }
				        siut.close();
					}
				});
	        }else {
	        	flowFile=session.write(flowFile, new StreamCallback() {
					
					@Override
					public void process(InputStream in, OutputStream out) throws IOException {
						// TODO Auto-generated method stub
						SnappyHadoopCompatibleOutputStream siot = new SnappyHadoopCompatibleOutputStream(out,blockSize);
				        BufferedInputStream input = new BufferedInputStream(in);
	
				        byte[] tmp = new byte[1024];
				        for (int readBytes = 0; (readBytes = input.read(tmp)) != -1;) {
				        	siot.write(tmp, 0, readBytes);
				        }
				        siot.close();
					}
				});
	        	
	        }
	        session.transfer(flowFile,SUCCESS);
        }catch(FlowFileAccessException ex){
        	session.transfer(flowFile,FAILURE);
        }
        // TODO implement
    }
}
