/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License. See accompanying LICENSE file.
 */
package org.apache.oozie.workflow.lite;

import org.apache.oozie.workflow.WorkflowException;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XmlUtils;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.ActionService;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.Namespace;
import org.xml.sax.SAXException;

import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.Validator;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Class to parse and validate workflow xml
 */
public class LiteWorkflowAppParser {

    private static final String DECISION_E = "decision";
    private static final String ACTION_E = "action";
    private static final String END_E = "end";
    private static final String START_E = "start";
    private static final String JOIN_E = "join";
    private static final String FORK_E = "fork";
    private static final Object KILL_E = "kill";

    private static final String SLA_INFO = "info";

    private static final String NAME_A = "name";
    private static final String TO_A = "to";

    private static final String FORK_PATH_E = "path";
    private static final String FORK_START_A = "start";

    private static final String ACTION_OK_E = "ok";
    private static final String ACTION_ERROR_E = "error";

    private static final String DECISION_SWITCH_E = "switch";
    private static final String DECISION_CASE_E = "case";
    private static final String DECISION_DEFAULT_E = "default";

    private static final String KILL_MESSAGE_E = "message";

    private Schema schema;
    private Class<? extends DecisionNodeHandler> decisionHandlerClass;
    private Class<? extends ActionNodeHandler> actionHandlerClass;

    private static enum VisitStatus {
        VISITING, VISITED
    }

    ;


    public LiteWorkflowAppParser(Schema schema, Class<? extends DecisionNodeHandler> decisionHandlerClass,
                                 Class<? extends ActionNodeHandler> actionHandlerClass) throws WorkflowException {
        this.schema = schema;
        this.decisionHandlerClass = decisionHandlerClass;
        this.actionHandlerClass = actionHandlerClass;
    }

    /**
     * Parse and validate xml to {@link LiteWorkflowApp}
     *
     * @param reader
     * @return LiteWorkflowApp
     * @throws WorkflowException
     */
    public LiteWorkflowApp validateAndParse(Reader reader) throws WorkflowException {
        try {
            StringWriter writer = new StringWriter();
            IOUtils.copyCharStream(reader, writer);
            String strDef = writer.toString();

            if (schema != null) {
                Validator validator = schema.newValidator();
                validator.validate(new StreamSource(new StringReader(strDef)));
            }

            Element wfDefElement = XmlUtils.parseXml(strDef);
            LiteWorkflowApp app = parse(strDef, wfDefElement);
            Map<String, VisitStatus> traversed = new HashMap<String, VisitStatus>();
            traversed.put(app.getNode(StartNodeDef.START).getName(), VisitStatus.VISITING);
            validate(app, app.getNode(StartNodeDef.START), traversed);
            
            Set<String> reachableNodes = traversed.keySet();
            setExecutionPathLengthEstimate(app, reachableNodes);
            return app;
        }
        catch (JDOMException ex) {
            throw new WorkflowException(ErrorCode.E0700, ex.getMessage(), ex);
        }
        catch (SAXException ex) {
            throw new WorkflowException(ErrorCode.E0701, ex.getMessage(), ex);
        }
        catch (IOException ex) {
            throw new WorkflowException(ErrorCode.E0702, ex.getMessage(), ex);
        }
    }

    /**
     * Parse xml to {@link LiteWorkflowApp}
     *
     * @param strDef
     * @param root
     * @return LiteWorkflowApp
     * @throws WorkflowException
     */
    @SuppressWarnings({"unchecked", "ConstantConditions"})
    private LiteWorkflowApp parse(String strDef, Element root) throws WorkflowException {
        Namespace ns = root.getNamespace();
        LiteWorkflowApp def = null;
        for (Element eNode : (List<Element>) root.getChildren()) {
            if (eNode.getName().equals(START_E)) {
                def = new LiteWorkflowApp(root.getAttributeValue(NAME_A), strDef,
                                          new StartNodeDef(eNode.getAttributeValue(TO_A)));
            }
            else {
                if (eNode.getName().equals(END_E)) {
                    def.addNode(new EndNodeDef(eNode.getAttributeValue(NAME_A)));
                }
                else {
                    if (eNode.getName().equals(KILL_E)) {
                        def.addNode(new KillNodeDef(eNode.getAttributeValue(NAME_A), eNode.getChildText(KILL_MESSAGE_E, ns)));
                    }
                    else {
                        if (eNode.getName().equals(FORK_E)) {
                            List<String> paths = new ArrayList<String>();
                            for (Element tran : (List<Element>) eNode.getChildren(FORK_PATH_E, ns)) {
                                paths.add(tran.getAttributeValue(FORK_START_A));
                            }
                            def.addNode(new ForkNodeDef(eNode.getAttributeValue(NAME_A), paths));
                        }
                        else {
                            if (eNode.getName().equals(JOIN_E)) {
                                def.addNode(new JoinNodeDef(eNode.getAttributeValue(NAME_A), eNode.getAttributeValue(TO_A)));
                            }
                            else {
                                if (eNode.getName().equals(DECISION_E)) {
                                    Element eSwitch = eNode.getChild(DECISION_SWITCH_E, ns);
                                    List<String> transitions = new ArrayList<String>();
                                    for (Element e : (List<Element>) eSwitch.getChildren(DECISION_CASE_E, ns)) {
                                        transitions.add(e.getAttributeValue(TO_A));
                                    }
                                    transitions.add(eSwitch.getChild(DECISION_DEFAULT_E, ns).getAttributeValue(TO_A));

                                    String switchStatement = XmlUtils.prettyPrint(eSwitch).toString();
                                    def.addNode(new DecisionNodeDef(eNode.getAttributeValue(NAME_A), switchStatement, decisionHandlerClass,
                                                                    transitions));
                                }
                                else {
                                    if (ACTION_E.equals(eNode.getName())) {
                                        String[] transitions = new String[2];
                                        Element eActionConf = null;
                                        for (Element elem : (List<Element>) eNode.getChildren()) {
                                            if (ACTION_OK_E.equals(elem.getName())) {
                                                transitions[0] = elem.getAttributeValue(TO_A);
                                            }
                                            else {
                                                if (ACTION_ERROR_E.equals(elem.getName())) {
                                                    transitions[1] = elem.getAttributeValue(TO_A);
                                                }
                                                else {
                                                    if (SLA_INFO.equals(elem.getName())) {
                                                        continue;
                                                    }
                                                    else {
                                                        eActionConf = elem;
                                                    }
                                                }
                                            }
                                        }
                                        String actionConf = XmlUtils.prettyPrint(eActionConf).toString();
                                        def.addNode(new ActionNodeDef(eNode.getAttributeValue(NAME_A), actionConf, actionHandlerClass,
                                                                      transitions[0], transitions[1]));
                                    }
                                    else {
                                        if (SLA_INFO.equals(eNode.getName())) {
                                            // No operation is required
                                        }
                                        else {
                                            throw new WorkflowException(ErrorCode.E0703, eNode.getName());
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        return def;
    }

    /**
     * Validate workflow xml
     *
     * @param app
     * @param node
     * @param traversed
     * @throws WorkflowException
     */
    private void validate(LiteWorkflowApp app, NodeDef node, Map<String, VisitStatus> traversed) throws WorkflowException {
        if (!(node instanceof StartNodeDef)) {
            try {
                ParamChecker.validateActionName(node.getName());
            }
            catch (IllegalArgumentException ex) {
                throw new WorkflowException(ErrorCode.E0724, ex.getMessage());
            }
        }
        if (node instanceof ActionNodeDef) {
            try {
                Element action = XmlUtils.parseXml(node.getConf());
                boolean supportedAction = Services.get().get(ActionService.class).getExecutor(action.getName()) != null;
                if (!supportedAction) {
                    throw new WorkflowException(ErrorCode.E0723, node.getName(), action.getName());
                }
            }
            catch (JDOMException ex) {
                throw new RuntimeException("It should never happen, " + ex.getMessage(), ex);
            }
        }

        if (node instanceof EndNodeDef) {
            traversed.put(node.getName(), VisitStatus.VISITED);
            return;
        }
        if (node instanceof KillNodeDef) {
            traversed.put(node.getName(), VisitStatus.VISITED);
            return;
        }
        for (String transition : node.getTransitions()) {

            if (app.getNode(transition) == null) {
                throw new WorkflowException(ErrorCode.E0708, node.getName(), transition);
            }

            //check if it is a cycle
            if (traversed.get(app.getNode(transition).getName()) == VisitStatus.VISITING) {
                throw new WorkflowException(ErrorCode.E0707, app.getNode(transition).getName());
            }
            //ignore validated one
            if (traversed.get(app.getNode(transition).getName()) == VisitStatus.VISITED) {
                continue;
            }

            traversed.put(app.getNode(transition).getName(), VisitStatus.VISITING);
            validate(app, app.getNode(transition), traversed);
        }
        traversed.put(node.getName(), VisitStatus.VISITED);
    }

    /*
     * Get source nodes (with no edges pointing to them) from a DAG;
     */
    List<String> getSourceNodes(Map<String, List<String>> DAG) {
        Set<String> cloneNodes = new HashSet<String>();
        for (String node : DAG.keySet()) {
            cloneNodes.add(node);
        }
        
        Iterator<String> iter = cloneNodes.iterator();
        
        Set<String> nonSourceNodes = new HashSet<String>();
        while (iter.hasNext()) {
            String name = iter.next();
            List<String> edges = DAG.get(name);
            for (String e : edges) {
                nonSourceNodes.add(e);
            }
        }
        
        cloneNodes.removeAll(nonSourceNodes);
        
        return new ArrayList<String>(cloneNodes);
    }
    
    /*
     * Perform topological sort on a DAG;
     */
    @SuppressWarnings("unchecked")
    private List<String> topologicalSort(Map<String, List<String>> DAG) {
        Map<String, List<String>> clonedDAG = (Map<String, List<String>>)((HashMap<String, List<String>>)DAG).clone();
        List<String> R = new ArrayList<String>();
        clonedDAG.remove(StartNodeDef.START);
        R.add(StartNodeDef.START);

        while (clonedDAG.size() > 0) {
            List<String> sourceNodes = getSourceNodes(clonedDAG);
            R.addAll(sourceNodes);

            for (String node : sourceNodes) {
                clonedDAG.remove(node);
            }
        }

        return R;
    }

    /*
     * Check if a node is control node or dummy node.
     */
    private boolean isControlNode(NodeDef node) {
        if (node instanceof StartNodeDef 
                || node instanceof EndNodeDef
                || node instanceof DecisionNodeDef
                || node instanceof ForkNodeDef
                || node instanceof JoinNodeDef
                || node instanceof KillNodeDef) {
            return true;
        }
        
        return false;
    }

    /*
     * Using dynamic programming to compute longest execution path of a DAG;
     */
    private int getLongestExecutionPathLength(LiteWorkflowApp app, Set<String> reachableNodes) {
        Map<String, List<String>> DAG = new HashMap<String, List<String>>();
        Collection<NodeDef> C = app.getNodeDefs();
        Iterator<NodeDef> iterator = C.iterator();
        while (iterator.hasNext()) {
            NodeDef node = iterator.next();
            if (reachableNodes.contains(node.getName())) { // only add reachable nodes from "start"
                DAG.put(node.getName(), node.getTransitions());
            }
        }

        List<String> topoSort = topologicalSort(DAG);
        Map<String, Integer> length_to = new HashMap<String, Integer>();
        // initialize
        for (String node : topoSort) {
            length_to.put(node, 0);
        }

        for (String node_v : topoSort) { 
            List<String> nodes = DAG.get(node_v);
            
            for (String node_w : nodes) { // v -> w is an edge
                int weight = isControlNode(app.getNode(node_w))? 0 : 1; 
                if (length_to.get(node_w) <= length_to.get(node_v) + weight) {
                    length_to.put(node_w, length_to.get(node_v) + weight);
                }
            }
        }

        int maxLength = -1;
        Set<String> nodes = length_to.keySet();
        for (String node : nodes) {
            if (length_to.get(node) > maxLength) {
                maxLength = length_to.get(node);
            }
        }

        return maxLength;
    }

    /*
     * Check if a wf app contains fork/join actions.
     */
    private boolean containFork(LiteWorkflowApp app, Set<String> reachableNodeNames) {
        for (String nodeName : reachableNodeNames) {
            if (app.getNode(nodeName) instanceof ForkNodeDef) {
                return true;
            }
        }

        return false;
    }

    /*
     * Evaluate total count of work actions in a wf app.
     */
    private int getTotalActionCount(LiteWorkflowApp app, Set<String> reachableNodeNames) {
        int cnt = 0;
        for (String nodeName : reachableNodeNames) {
            if (!isControlNode(app.getNode(nodeName))) {
                cnt ++;
            }
        }

        return cnt;
    }

    /*
     * Estimate a wf app's execution path length. This is for estimating the app's progress.
     * 
     * @param app wf app
     * @param reachableNodeNames names of nodes that are reachable from "start". 
     *        This is to save one extra traversal of the DAG since we already traversed it previously,
     *        so reuse that result.
     */
    private void setExecutionPathLengthEstimate(LiteWorkflowApp app, Set<String> reachableNodeNames) {
        int length = -1;

        // TODO
        // if DAG contain fork/join, we simply check total number of work actions.
        // This can be improved in future.
        if (containFork(app, reachableNodeNames) == true) {
            length = getTotalActionCount(app, reachableNodeNames);
        }
        // Otherwise (it can contain decision node), we use longest possible execution path.
        else {
            length = getLongestExecutionPathLength(app, reachableNodeNames);
        }

        app.setExecutionPathLengthEstimate(length);
    }
}