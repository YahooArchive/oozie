package org.apache.oozie.cli;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import org.apache.oozie.client.CoordinatorAction;
import junit.framework.TestCase;

public class TestOozieCLI extends TestCase {
	
	private CoordinatorAction createDummyCoordinatorAction(final String id, final int actionNumber) {
		return new CoordinatorAction() {
			
			@Override
			public void setErrorMessage(String errorMessage) {
			}
			
			@Override
			public void setErrorCode(String errorCode) {
			}
			
			@Override
			public String getTrackerUri() {
				return null;
			}
			
			@Override
			public Status getStatus() {
				return null;
			}
			
			@Override
			public String getRunConf() {
				return null;
			}
			
			@Override
			public Date getNominalTime() {
				return null;
			}
			
			@Override
			public String getMissingDependencies() {
				return null;
			}
			
			@Override
			public Date getLastModifiedTime() {
				return null;
			}
			
			@Override
			public String getJobId() {
				return null;
			}
			
			@Override
			public String getId() {
				return id;
			}
			
			@Override
			public String getExternalStatus() {
				return null;
			}
			
			@Override
			public String getExternalId() {
				return null;
			}
			
			@Override
			public String getErrorMessage() {
				return null;
			}
			
			@Override
			public String getErrorCode() {
				return null;
			}
			
			@Override
			public Date getCreatedTime() {
				return null;
			}
			
			@Override
			public String getCreatedConf() {
				return null;
			}
			
			@Override
			public String getConsoleUrl() {
				return null;
			}
			
			@Override
			public int getActionNumber() {
				return actionNumber;
			}
		}; 
	}
	
	public void testSortingActionsByActionNumber() throws Exception {
		List<CoordinatorAction> actions = new ArrayList<CoordinatorAction>();
		actions.add(createDummyCoordinatorAction("0000426-100422050556688-oozie-marc-C@82", 82));
		actions.add(createDummyCoordinatorAction("0000426-100422050556688-oozie-marc-C@58", 58));
		actions.add(createDummyCoordinatorAction("0000426-100422050556688-oozie-marc-C@30", 30));
		actions.add(createDummyCoordinatorAction("0000426-100422050556688-oozie-marc-C@5", 5));
		actions.add(createDummyCoordinatorAction("0000426-100422050556688-oozie-marc-C@18", 18));
		
		new OozieCLI().sortActionsByActionNumber(actions, new Comparator<CoordinatorAction>() {

			@Override
			public int compare(CoordinatorAction action1, CoordinatorAction action2) {
				return action1.getActionNumber() - action2.getActionNumber();
			}
		});
		
		assertEquals(5, actions.get(0).getActionNumber());
		assertEquals(18, actions.get(1).getActionNumber());
		assertEquals(30, actions.get(2).getActionNumber());
		assertEquals(58, actions.get(3).getActionNumber());
		assertEquals(82, actions.get(4).getActionNumber());
	}
}
