package org.apache.hadoop.yarn.applications.narwhal.client;

import org.apache.commons.cli.ParseException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertTrue;

public class TestActionDoPortMapping {

  private ActionDoPortMapping action;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setup() {
    action = new ActionDoPortMapping();
  }

  @Test
  public void testInitWithRightParameters() throws ParseException {
    String[] args = {"-applicationId", "application_1465186316357_0001", "-containerHost", "localhost", "-containerId", "f464455b8358", "-containerPort", "5000"};
    boolean inited = action.init(args);
    assertTrue(inited);
  }

  @Test
  public void testInitWithWrongParameters() throws ParseException {
    thrown.expectMessage("no container id specified");
    String[] args = {"-applicationId", "application_1465186316357_0001", "-containerHost", "localhost", "-containerPort", "5000"};
    action.init(args);
  }

  @After
  public void tearDown() {

  }

}
