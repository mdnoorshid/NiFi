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

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;


public class CSVDATARecorderInSQLTest {

    private TestRunner testRunner;



static String readFile() throws IOException {
	File file = new File("C:\\Users\\Niyamat\\Desktop\\DROPBOX\\InputDir\\all-all-unprocessed-CISCO2.csv");
	BufferedInputStream bin = new BufferedInputStream(new FileInputStream(file));
	byte[] buffer = new byte[(int) file.length()];
	bin.read(buffer);
	String fileStr = new String(buffer);
	System.out.println("fileStr:: "+fileStr);
	return fileStr;
}






@Test
public void testOnTrigger() throws IOException {
	InputStream content = new ByteArrayInputStream(readFile().getBytes());
	TestRunner runner = TestRunners.newTestRunner(new CustomCSVDATARecorderInSQL());
	runner.enqueue(content);
	runner.run(1);
}
}
