package com.reactiveworks.learning.nifi.processors.customGetFileProcessor;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
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

/**
 * Custom Class to get file
 * 
 * @author Niyamat
 *
 */
@TriggerWhenEmpty
@Tags({"CustomGetFileProcessor","reactiveworks"})
@CapabilityDescription("Get a file and read all data from it, preserve all lines")
public class CustomGetFileProcessor extends AbstractProcessor {
	public static final Relationship SUCCESS = new Relationship.Builder().name("SUCCESS").description("success flag")
			.build();

	public static final PropertyDescriptor filePath = new PropertyDescriptor.Builder().name("File Path")
			.required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).description("Give File Path")
			.build();
	private Set<Relationship> relationships;
	private List<PropertyDescriptor> descriptors;

	public static final String MATCH_ATTR = "match";

	@Override
	protected void init(ProcessorInitializationContext context) {
		ComponentLog logger = getLogger();
		logger.debug(".inside init get  CustomGetFileProcessor.....");
		Set<Relationship> relationships = new HashSet<>();
		relationships.add(SUCCESS);
		this.relationships = Collections.unmodifiableSet(relationships);
		final List<PropertyDescriptor> descriptors = new ArrayList<>();
		descriptors.add(filePath);
		this.descriptors = Collections.unmodifiableList(descriptors);
		logger.debug("relationship for success is builded.....");
	}

	final Map<String, String> attributes = new HashMap<>();

	@Override
	public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
		ComponentLog logger = getLogger();
		logger.info("inside the onTrigger processor CustomGetFileProcessor....");
		FlowFile flowFile =null;
		final AtomicReference<String> value = new AtomicReference<String>();
		
		try {
			flowFile = session.create();
			logger.debug("flow file is created successfully....");
			String finalData = null;
			String filePathReceied = context.getProperty(filePath).getValue();
			logger.info("file path received:: " + filePathReceied);
			BufferedReader br = new BufferedReader(new FileReader(filePathReceied));
			StringBuilder sb = new StringBuilder();
			String line = br.readLine();
			while (line != null) {
				sb.append(line);
				sb.append("\n");
				line = br.readLine();
			}
			finalData = sb.toString();
			logger.debug("finalData:: " + finalData);

			value.set(finalData);
			String results = value.get();
			logger.debug("results value CustomGetFileProcessor::: "+results);
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
			logger.debug("Final value in fileFlow:: "+readData(flowFile, session));
			session.transfer(flowFile, SUCCESS);
			logger.debug("new flow file  CustomGetFileProcessor  has been transferred for relationship SUCCESS");
            session.commit();
            logger.debug("session is committed successfully.....");
			
		} catch (FileNotFoundException e) {
			logger.error("File Not Found:: ", e);
		} catch (IOException e) {
			logger.error("IO Exception:: ", e);
		}
	}

	
	/**
	 * Method to read incoming csv file data
	 * 
	 * @param flowFile
	 * @param session
	 * @return
	 */
	public String readData(FlowFile flowFile, final ProcessSession session) {
		ComponentLog logger = getLogger();
		logger.info("inside readData method.....");
		final AtomicReference<String> value = new AtomicReference<>();
		session.read(flowFile, new InputStreamCallback() {

			@Override
			public void process(InputStream in) throws IOException {
				try {
					StringWriter writer = new StringWriter();
					IOUtils.copy(in, writer, "UTF-8");
					String data = writer.toString();
					logger.info("data receive from session:: " + data);
					value.set(data);
				} catch (Exception e) {
					logger.error("Failed to parse csv data:: ", e);
				}
			}
		});

		return value.get();
}

	@Override
	public Set<Relationship> getRelationships() {
		return relationships;
	}

	@Override
	protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return descriptors;
	}
}
