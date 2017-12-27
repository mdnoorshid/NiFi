package com.reactiveworks.learning.nifi;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

public class CSVToJSONWithHeadersProcessorTest {
	@Test
	public void testOnTrigger() {
		String csv = "lastName,dob,age,hobby,firstName\n"+
				"Kasireddi,Thu May 06 00:00:00 IST 2010,2,Singing,Sriram\n"+
				"Kasireddi,Mon Sep 06 00:00:00 IST 1982,29,Painting,Sudhakar";
		
		InputStream content = new ByteArrayInputStream(csv.getBytes());
		TestRunner runner = TestRunners.newTestRunner(new CSVToJSONWithHeadersProcessor());
		runner.enqueue(content);
		runner.run(1);
		// All results were processed with out failure
		runner.assertQueueEmpty();

	}
}
