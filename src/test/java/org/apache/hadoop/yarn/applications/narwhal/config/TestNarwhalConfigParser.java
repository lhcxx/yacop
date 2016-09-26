package org.apache.hadoop.yarn.applications.narwhal.config;

import static org.junit.Assert.*;

import org.codehaus.jettison.json.JSONException;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

/**
 * Created by zyluo on 6/3/16.
 */
public class TestNarwhalConfigParser {

    @Rule
    public ExpectedException thrown= ExpectedException.none();

    @org.junit.Test
    public void parse() throws Exception {
        NarwhalConfig config = NarwhalConfigParser.parse(NarwhalConfigCorpus.readmeInput);
        String actual = config.toString();
        String expected = NarwhalConfigCorpus.readmeOutput;
        assertEquals(expected, actual);
    }

    @org.junit.Test
    public void testVolumeMountConfigParse() throws Exception {
        NarwhalConfig config = NarwhalConfigParser.parse(NarwhalConfigCorpus.testVolumeMountInput);
        String actual = config.toString();
        String expected = NarwhalConfigCorpus.testVolumeMountOutput;
        assertEquals(expected, actual);
    }

    @org.junit.Test
    public void testVolumeMountErrorContainerPathInputParse() throws Exception {
        thrown.expect(BuilderException.class);
        thrown.expectMessage("illegal ContainerPath");
        NarwhalConfigParser.parse(NarwhalConfigCorpus.testVolumeMountErrorContainerPathInput);
    }

    @org.junit.Test
    public void testVolumeMountErrorNoHostPathInputParse() throws Exception {
        thrown.expect(BuilderException.class);
        thrown.expectMessage("Invalid volume config");
        NarwhalConfigParser.parse(NarwhalConfigCorpus.testVolumeMountErrorNoHostPathInput);
    }
    
    //TODO: deal the test about these java code invoke shell script
    public void testNetworkConfigParse() throws Exception {
        NarwhalConfig config = NarwhalConfigParser.parse(NarwhalConfigCorpus.testNetworkInput);
        String actual = config.toString();
        String expected = NarwhalConfigCorpus.testNetworkOutput;
        assertEquals(expected, actual);  
    }

    @org.junit.Test
    public void testNetworkConfigParseWithEmptyName() throws Exception {
        thrown.expect(BuilderException.class);
        thrown.expectMessage("network cannot be empty");
    	NarwhalConfigParser.parse(NarwhalConfigCorpus.testNetworkInputWithEmptyName);
    }    

}