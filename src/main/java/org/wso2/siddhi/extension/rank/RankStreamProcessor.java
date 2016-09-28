/*
 *
 *  * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *  *
 *  * WSO2 Inc. licenses this file to you under the Apache License,
 *  * Version 2.0 (the "License"); you may not use this file except
 *  * in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied. See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.wso2.siddhi.extension.rank;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.exception.OperationNotSupportedException;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class RankStreamProcessor extends StreamProcessor {

    public enum SortOrder {
        ASC, DES
    }

    SortOrder sortOrder;

    private Map<String, Double> teamNameToRankScore;
    private Map<String, Double> sortedRankScore;
    private Map<String, StreamEvent> teamNameToStreamEvent;

    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition,
            ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        if (attributeExpressionExecutors.length != 3) {
            throw new ExecutionPlanValidationException(
                    "Invalid no of arguments passed to RankStreamProcessor, required 3, but found "
                            + attributeExpressionExecutors.length);
        }

        if (attributeExpressionExecutors[0].getReturnType() != Attribute.Type.STRING) {
            throw new ExecutionPlanValidationException(
                    "Invalid parameter type found for the 1st argument of RankStreamProcessor, required "
                            + Attribute.Type.STRING + ". but found "
                            + attributeExpressionExecutors[0].getReturnType().toString());
        }

        if (attributeExpressionExecutors[1].getReturnType() != Attribute.Type.DOUBLE) {
            throw new ExecutionPlanValidationException(
                    "Invalid parameter type found for the 2nd argument of RankStreamProcessor, required "
                            + Attribute.Type.DOUBLE + ". but found "
                            + attributeExpressionExecutors[1].getReturnType().toString());
        }

        if (attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor) {
            Object sortOrderObject = attributeExpressionExecutors[2].execute(null);
            if (sortOrderObject instanceof String) {
                if (((String) sortOrderObject).equalsIgnoreCase("asc")) {
                    sortOrder = SortOrder.ASC;
                } else if (((String) sortOrderObject).equalsIgnoreCase("dec")) {
                    sortOrder = SortOrder.DES;
                } else {
                    throw new ExecutionPlanValidationException(
                            "SortOrder parameter should be asc or dec. But found " + sortOrderObject);
                }
            } else {
                throw new ExecutionPlanValidationException("SortOrder parameter should be of type String. But found "
                        + attributeExpressionExecutors[2].getReturnType());
            }
        } else {
            throw new OperationNotSupportedException("SortOrder has to be a constant");
        }

        teamNameToRankScore = new HashMap<String, Double>();
        sortedRankScore = new LinkedHashMap<String, Double>();
        teamNameToStreamEvent = new HashMap<String, StreamEvent>();

        List<Attribute> attributeList = new ArrayList<Attribute>();
        attributeList.add(new Attribute("rank", Attribute.Type.INT));
        return attributeList;

    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
            StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {
        ComplexEventChunk<StreamEvent> returnEventChunk = new ComplexEventChunk<StreamEvent>(false);
        synchronized (this) {
            while (streamEventChunk.hasNext()) {

                StreamEvent event = streamEventChunk.next();
                streamEventChunk.remove();

                String teamName = (String) attributeExpressionExecutors[0].execute(event);
                Double rankScore = (Double) attributeExpressionExecutors[1].execute(event);

                teamNameToStreamEvent.put(teamName, event);
                teamNameToRankScore.put(teamName, rankScore);

                sortedRankScore = sortByValue(teamNameToRankScore);
                Double prevValue = null;
                int rank = 0;
                int count = 0;
                StreamEvent returnStreamEvent;

                for (Map.Entry<String, Double> entry : sortedRankScore.entrySet()) {
                    count++;
                    returnStreamEvent = teamNameToStreamEvent.get(entry.getKey());
                    StreamEvent clonedEvent = streamEventCloner.copyStreamEvent(returnStreamEvent);

                    if (!entry.getValue().equals(prevValue)) {
                        rank = count;
                    }

                    complexEventPopulater.populateComplexEvent(clonedEvent, new Object[] { rank });
                    returnEventChunk.add(clonedEvent);
                    prevValue = entry.getValue();
                }
                sortedRankScore.clear();
            }
        }
        if (returnEventChunk.getFirst() != null) {
            nextProcessor.process(returnEventChunk);
        }
    }

    private Map<String, Double> sortByValue(Map<String, Double> unsortMap) {

        List<Map.Entry<String, Double>> list = new LinkedList<Map.Entry<String, Double>>(unsortMap.entrySet());

        if (sortOrder == SortOrder.ASC) {
            Collections.sort(list, new Comparator<Map.Entry<String, Double>>() {
                public int compare(Map.Entry<String, Double> o1, Map.Entry<String, Double> o2) {
                    return (o1.getValue()).compareTo(o2.getValue());
                }
            });
        } else {
            Collections.sort(list, new Comparator<Map.Entry<String, Double>>() {
                public int compare(Map.Entry<String, Double> o1, Map.Entry<String, Double> o2) {
                    return (o2.getValue()).compareTo(o1.getValue());
                }
            });
        }

        Map<String, Double> result = new LinkedHashMap<String, Double>();
        for (Map.Entry<String, Double> entry : list) {
            result.put(entry.getKey(), entry.getValue());
        }

        return result;
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public Object[] currentState() {
        return new Object[] { teamNameToRankScore, sortedRankScore, teamNameToStreamEvent };
    }

    @Override
    public void restoreState(Object[] state) {
        teamNameToRankScore = (HashMap<String, Double>) state[0];
        sortedRankScore = (LinkedHashMap<String, Double>) state[1];
        teamNameToStreamEvent = (HashMap<String, StreamEvent>) state[2];
    }

}
