package com.reactiveworks.learning.nifi.processors.customGetFileProcessor;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

public class CustomGetFileProcessorTest {

	String csv = "lastName,dob,age,hobby,firstName\n" + "Kasireddi,Thu May 06 00:00:00 IST 2010,2,Singing,Sriram\n"
			+ "Kasireddi,Mon Sep 06 00:00:00 IST 1982,29,Painting,Sudhakar";

	@Test
	public void testOnTrigger() {
		InputStream content = new ByteArrayInputStream(csv.getBytes());
		TestRunner runner = TestRunners.newTestRunner(new CustomGetFileProcessor());
		runner.setProperty(CustomGetFileProcessor.filePath,
				"C:\\Users\\Niyamat\\Desktop\\DROPBOX\\InputDir\\all-all-unprocessed-CISCO2.csv");
		runner.enqueue(content);
		runner.run(1);
	}
}