package com.reactiveworks.learning.nifi;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringWriter;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
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
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.XML;

/**
 * This is the custom processor to convert json data into xml
 * 
 * @author Md Noorshid
 *
 */
@SideEffectFree
@Tags({"JSONToXML","Reactivworks"})
@CapabilityDescription("Change JSON Object or Json Array into XML")
public class JSONToXMLProcessor extends AbstractProcessor {
	public static final Relationship SUCCESS = new Relationship.Builder().name("SUCCESS").description("success flag")
			.build();
	public static final Relationship FAILURE = new Relationship.Builder().name("FAILURE").description("failure flag")
			.build();

	public static final String MATCH_ATTR = "match";
	private Set<Relationship> relationships;

	@Override
	protected void init(ProcessorInitializationContext context) {
		ComponentLog logger = getLogger();
		logger.debug("inside init method......");
		Set<Relationship> relationships = new HashSet<>();
		relationships.add(SUCCESS);
		this.relationships = Collections.unmodifiableSet(relationships);
	}

	@Override
	public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
		ComponentLog logger = getLogger();
		logger.debug(".inside onTrigger method....");
		final AtomicReference<String> value = new AtomicReference<String>();
		FlowFile flowFile = session.get();
		if (flowFile == null) {
			logger.debug("flow file is null so whole process is getting back.....");
			return;
		}
		JSONObject incomingJsonObj = null;
		JSONArray incomingJsonArr = null;
		String xml = null ;
		String dataAsJson = readData(flowFile, session);
		logger.debug("incoming data as String got:: " + dataAsJson);
		if (dataAsJson.startsWith("{")) {
			incomingJsonObj = new JSONObject(dataAsJson);
			logger.debug("incoming data as Json Object:: " + incomingJsonObj);
			xml = XML.toString(incomingJsonObj);
		} else if (dataAsJson.startsWith("[")) {
			incomingJsonArr = new JSONArray(dataAsJson);
			logger.debug("incoming data as Json Array:: " + incomingJsonArr);
			xml = XML.toString(incomingJsonArr);
		}
		logger.debug("xml:: " + xml);
		value.set(xml);
		String result = value.get();
		if (result != null && result.isEmpty()) {
			flowFile = session.putAttribute(flowFile, "match", result);
		}

		// To write the results back out of flow file
		flowFile = session.write(flowFile, new OutputStreamCallback() {

			@Override
			public void process(OutputStream out) throws IOException {
				out.write(value.get().getBytes());
			}
		});
		logger.info("Final value in fileFlow:: " + readData(flowFile, session));
		session.transfer(flowFile, SUCCESS);
		logger.info("new flow file has been transferred for relationship SUCCESS");
	}

	/**
	 * Method to read the json data
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
					String jsondata = writer.toString();
					logger.debug("jsonData::: " + jsondata);
					value.set(jsondata);
				} catch (Exception e) {
					logger.error("Unable to parse the json data", e);
				}

			}
		});
		return value.get();
	}

	@Override
	public Set<Relationship> getRelationships() {
		return relationships;
	}

}
