package com.reactiveworks.learning.nifi;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

public class JSONToXMLProcessorTest {

	String jsonAsStr = "[" + "	{" + "		\"ticketId\": 1," + "		\"passengerName\": \"Md Noorshid\","
			+ "		\"bookingDate\": 1511136000000," + "		\"sourceStation\": \"Bangalore\","
			+ "		\"destination\": \"Kolkata\"," + "		\"email\": \"mdnoorshid254@gmail.com\"" + "	}," + "	{"
			+ "		\"ticketId\": 3," + "		\"passengerName\": \"Deelip\"," + "		\"bookingDate\": 1511136000000,"
			+ "		\"sourceStation\": \"Bangalore Central\"," + "		\"destination\": \"Tirupati JN\","
			+ "		\"email\": \"deelipa@gmail.com\"" + "	}," + "	{" + "		\"ticketId\": 4,"
			+ "		\"passengerName\": \"Chetan\"," + "		\"bookingDate\": 1511308800000,"
			+ "		\"sourceStation\": \"Bangalore\"," + "		\"destination\": \"Mumbai\","
			+ "		\"email\": \"chetan@gmail.com\"" + "	}," + "	{" + "		\"ticketId\": 5,"
			+ "		\"passengerName\": \"Raja D\"," + "		\"bookingDate\": 1511308800000,"
			+ "		\"sourceStation\": \"Bangalore\"," + "		\"destination\": \"Chennai\","
			+ "		\"email\": \"rajaD@gmail.com\"" + "	}," + "	{" + "		\"ticketId\": 6,"
			+ "		\"passengerName\": \"Shruti\"," + "		\"bookingDate\": 1512172800000,"
			+ "		\"sourceStation\": \"Bangalore\"," + "		\"destination\": \"Kolkata\","
			+ "		\"email\": \"mdnoorshid254@gmail.com\"" + "	}" + "]";
	
	String jsonAsObj="{"+
			"    \"glossary\": {"+
			"        \"title\": \"example glossary\","+
			"		\"GlossDiv\": {"+
			"            \"title\": \"S\","+
			"			\"GlossList\": {"+
			"                \"GlossEntry\": {"+
			"                    \"ID\": \"SGML\","+
			"					\"SortAs\": \"SGML\","+
			"					\"GlossTerm\": \"Standard Generalized Markup Language\","+
			"					\"Acronym\": \"SGML\","+
			"					\"Abbrev\": \"ISO 8879:1986\","+
			"					\"GlossDef\": {"+
			"                        \"para\": \"A meta-markup language, used to create markup languages such as DocBook.\","+
			"						\"GlossSeeAlso\": [\"GML\", \"XML\"]"+
			"                    },"+
			"					\"GlossSee\": \"markup\""+
			"                }"+
			"            }"+
			"        }"+
			"    }"+
			"}";

	@Test
	public void testOnTrigger() {
		InputStream content = new ByteArrayInputStream(jsonAsObj.getBytes());
		TestRunner runner = TestRunners.newTestRunner(new JSONToXMLProcessor());
		runner.enqueue(content);
		runner.run(1);
		// All results were processed with out failure
		runner.assertQueueEmpty();

	}
}
