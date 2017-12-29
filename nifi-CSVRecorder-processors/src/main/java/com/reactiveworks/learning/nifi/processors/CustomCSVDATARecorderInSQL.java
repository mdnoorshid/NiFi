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
package com.reactiveworks.learning.nifi.processors;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.io.IOUtils;
import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.insert.InsertInto;
import org.apache.metamodel.jdbc.JdbcDataContext;
import org.apache.metamodel.schema.Table;
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
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

@Tags({ "CSVRecorder", "ReactiveWorks" })
@CapabilityDescription("CSV Recorder in MySql, it will record each line and other detail in MySql database")
@SeeAlso({})
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = "", description = "") })
public class CustomCSVDATARecorderInSQL extends AbstractProcessor {

	public static final Relationship SUCCESS = new Relationship.Builder().name("SUCCESS").description("success flag")
			.build();

	public static final Relationship FAILURE = new Relationship.Builder().name("FAILURE").description("failure flag")
			.build();
	public static final PropertyDescriptor seperator = new PropertyDescriptor.Builder().name("CSV Separator")
			.required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.description("Delimeter by which csv data is seperated").build();

	private Set<Relationship> relationships;
	public static final String MATCH_ATTR = "match";

	@Override
	protected void init(final ProcessorInitializationContext context) {
		ComponentLog logger = getLogger();
		final Set<Relationship> relationships = new HashSet<Relationship>();
		relationships.add(SUCCESS);
		relationships.add(FAILURE);
		this.relationships = Collections.unmodifiableSet(relationships);
		logger.info("relationship for success is builded.....");
	}

	@OnScheduled
	public void onScheduled(final ProcessContext context) {

	}

	@Override
	public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
		ComponentLog logger = getLogger();
		logger.debug(".inside onTrigger method....");
		FlowFile flowFile = session.get();
		if (flowFile == null) {
			return;
		}
		final AtomicReference<String> value = new AtomicReference<String>();
		String dataReceived = readData(flowFile, session);
		logger.debug("dataReceived:: " + dataReceived);
		String lines[] = dataReceived.split("\\n");
		logger.debug("lenght of lines:: "+lines.length);
		for (int i = 0; i < lines.length; i++) {
			try {
				if (i > 0) {
					insertRecords(i+1, lines[i], new Date());
					logger.debug("inserting.....");
				}
			} catch (UnableToGetDataContextException | ConnectionCreationProblem e) {
				logger.error("Unable to Insert Record Error:: ", e);
			}
		}
		logger.debug("Insertion Completed!!!");
		value.set(dataReceived);
		String results = value.get();
		logger.debug("results value CustomGetFileProcessor::: " + results);
		if (results != null && !results.isEmpty()) {
			flowFile = session.putAttribute(flowFile, "match", results);
		}
		// To write the results back out of flow file
		flowFile = session.write(flowFile, new OutputStreamCallback() {

			@Override
			public void process(OutputStream out) throws IOException {
				out.write(value.get().getBytes());
			}
		});
		logger.debug("Final value in fileFlow:: " + readData(flowFile, session));
		session.transfer(flowFile, SUCCESS);
		logger.debug("new flow file  CustomCSVDataRecorder  has been transferred for relationship SUCCESS");
	}

	/**
	 * Method to read the incoming data
	 * 
	 * @param flowFile
	 * @param session
	 * @return
	 */
	public String readData(FlowFile flowFile, final ProcessSession session) {
		ComponentLog logger = getLogger();
		logger.debug("inside readData method......");
		final AtomicReference<String> value = new AtomicReference<>();
		session.read(flowFile, new InputStreamCallback() {

			@Override
			public void process(InputStream in) throws IOException {
				try {
					StringWriter writer = new StringWriter();
					IOUtils.copy(in, writer, "UTF-8");
					String incomingData = writer.toString();
					logger.debug("incomingData::: " + incomingData);
					value.set(incomingData);
				} catch (Exception e) {
					logger.error("Unable to parse the json data", e);
				}

			}
		});
		return value.get();
	}

	/**
	 * Method to get UpdateableDataContext
	 * 
	 * @return
	 * @throws UnableGetClassException
	 * @throws ConnectionCreationProblem
	 */
	public UpdateableDataContext getDataContext() throws UnableGetClassException, ConnectionCreationProblem {
		ComponentLog logger = getLogger();
		UpdateableDataContext dataContext;
		Connection con;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			con = DriverManager.getConnection("jdbc:mysql://localhost:3306/nifi", "root", "root");
			dataContext = new JdbcDataContext(con);
			logger.debug("dataContext created successfully......");
		} catch (ClassNotFoundException e) {
			throw new UnableGetClassException("Class Not Found Exception", e);
		} catch (SQLException e) {
			throw new ConnectionCreationProblem("Unable to get the connection object", e);
		}
		return dataContext;
	}

	/**
	 * Method to insert record in mysql database
	 * 
	 * @throws UnableToGetDataContextException
	 * @throws ConnectionCreationProblem
	 */
	public void insertRecords(int csvlineNumber, String data, Date date)
			throws UnableToGetDataContextException, ConnectionCreationProblem {
		ComponentLog logger=getLogger();
		logger.debug(".inside insertRecords method....");
		UpdateableDataContext dataContext = null;
		try {
			dataContext = getDataContext();
			logger.debug("dataContext builded successfully");
			Table table = dataContext.getTableByQualifiedLabel("csv");
			dataContext.executeUpdate(new InsertInto(table).value("csvlineNumber",csvlineNumber).value("data", data).value("recieveDate",date));
			logger.debug("executeUpdate done.....");
		} catch (UnableGetClassException e) {
			throw new UnableToGetDataContextException("Unable to get data context object", e);
		} catch (ConnectionCreationProblem e) {
			throw new ConnectionCreationProblem("Unable to get the connection object", e);
		}finally{
			if(dataContext!=null){
				dataContext=null;
			}
		}
	}

	@Override
	public Set<Relationship> getRelationships() {
		return this.relationships;
	}

}
