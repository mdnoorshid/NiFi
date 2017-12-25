package com.reactiveworks.learning.nifi.processors.reactiveworks;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
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
import org.apache.nifi.annotation.behavior.SideEffectFree;
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
import org.json.JSONArray;

/**
 * This class is to convert CSV to Json
 * 
 * @author Md Noorshid
 *
 */
@SideEffectFree
@Tags({ "CSVToJSON", "Reactiveworks" })
@CapabilityDescription("Remove headers and convert all csv data into Json Array Object")
public class CSVToJSONProcessor extends AbstractProcessor {
	public static final Relationship SUCCESS = new Relationship.Builder().name("SUCCESS").description("success flag")
			.build();
	public static final Relationship FAILURE = new Relationship.Builder().name("FAILURE").description("failure flag")
			.build();

	public static final PropertyDescriptor headers = new PropertyDescriptor.Builder().name("Number Of Headers")
			.required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).description("Total Headers in CSV")
			.build();
	public static final PropertyDescriptor seperator = new PropertyDescriptor.Builder().name("CSV Separator")
			.required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.description("Delimeter by which csv data is seperated").build();
	private Set<Relationship> relationships;
	private List<PropertyDescriptor> descriptors;

	public static final String MATCH_ATTR = "match";

	@Override
	protected void init(ProcessorInitializationContext context) {
		ComponentLog logger = getLogger();
		logger.info(".inside init.....");
		Set<Relationship> relationships = new HashSet<>();
		relationships.add(SUCCESS);
		relationships.add(FAILURE);
		this.relationships = Collections.unmodifiableSet(relationships);
		final List<PropertyDescriptor> descriptors = new ArrayList<>();
		descriptors.add(headers);
		descriptors.add(seperator);
		this.descriptors = Collections.unmodifiableList(descriptors);
		logger.info("relationship for success is builded.....");
	}

	final Map<String, String> attributes = new HashMap<>();

	@Override
	public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
		ComponentLog logger = getLogger();
		InputStream is;
		BufferedReader br = null;
		JSONArray jsonArray = new JSONArray();
		logger.info("inside onTrigger method.....");
		final AtomicReference<String> value = new AtomicReference<String>();
		FlowFile flowFile = session.get();
		if (flowFile == null) {
			logger.info("flow file is null so whole process is getting back.....");
			return;
		}
		String incomingDataAsCsv = readData(flowFile, session);
		incomingDataAsCsv = incomingDataAsCsv.replaceAll("[\r\n]+", " ");
		logger.info("incoming data as csv got:: " + incomingDataAsCsv);
		String cvsSplitBy = context.getProperty(seperator).toString();
		logger.info("cvsSplitBy:: " + cvsSplitBy);
		int headersNum = Integer.parseInt(context.getProperty(headers).toString());
		logger.info("headersNum:: " + headersNum);
		String line = "";
		is = new ByteArrayInputStream(incomingDataAsCsv.getBytes());
		br = new BufferedReader(new InputStreamReader(is));

		try {
			while ((line = br.readLine()) != null) {
				logger.info("line::: " + line);
				String[] linesArr = line.split(cvsSplitBy);
				System.out.println("linesArr:: " + linesArr.length);
				/*
				 * String[] headerArr=new String[headers-1];//4 String[]
				 * valueArr=new String[(linesArr.length)+1+headers];//9
				 * 
				 * System.arraycopy(linesArr, 0, headerArr, 0,
				 * headerArr.length); System.arraycopy(linesArr,headers ,
				 * valueArr, 0, valueArr.length-1);
				 */
				for (int i = headersNum; i < linesArr.length; i++) {
					jsonArray.put(linesArr[i]);
				}
			}
		} catch (IOException e) {
			logger.error("Error:: ", e);
		}

		logger.info("csvConvertedToJsonarray:: " + jsonArray);
		value.set(jsonArray.toString());
		String results = value.get();
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
		logger.info("new flow file has been transferred for relationship SUCCESS");
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
					String csvData = writer.toString();
					logger.info("csv data received:: " + csvData);
					value.set(csvData);
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
