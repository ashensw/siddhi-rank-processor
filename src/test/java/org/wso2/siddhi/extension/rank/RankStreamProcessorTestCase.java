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

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;

import java.util.concurrent.CountDownLatch;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class RankStreamProcessorTestCase {
    private static Logger logger = Logger.getLogger(RankStreamProcessorTestCase.class);
    private volatile int count;
    private volatile boolean eventArrived;

    @Before
    public void init() {
        count = 0;
        eventArrived = false;
    }

    @Test
    public void RankStreamProcessorTestCase1() throws Exception {
        logger.info("RankStreamProcessor TestCase 1");

        final int EXPECTED_NO_OF_EVENTS = 10;
        CountDownLatch countDownLatch = new CountDownLatch(EXPECTED_NO_OF_EVENTS);
        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition = "define stream inputStream (teamName string, rankScore double, maxSpeed double);";

        String eventFuseExecutionPlan = ("@info(name = 'query1') from inputStream#leaderboard:rank(teamName, rankScore, 'dec') "
                + "select rank, teamName, rankScore, maxSpeed "
                + "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager
                .createExecutionPlanRuntime(inStreamDefinition + eventFuseExecutionPlan);

        executionPlanRuntime.addCallback("outputStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                eventArrived = true;
                EventPrinter.print(events);
                count++;
                switch (count) {
                case 1:
                    Assert.assertEquals("Output event count", 1, events.length);
                    break;
                case 2:
                    Assert.assertEquals("Output event count", 2, events.length);
                    break;
                case 3:
                    Assert.assertEquals("Output event count", 3, events.length);
                    break;
                case 4:
                    Assert.assertEquals("Output event count", 4, events.length);
                    break;
                case 5:
                    Assert.assertEquals("Output event count", 5, events.length);
                    break;
                case 6:
                    Assert.assertEquals("Output event count", 5, events.length);
                    break;
                case 7:
                    Assert.assertEquals("Output event count", 5, events.length);
                    break;
                case 8:
                    Assert.assertEquals("Output event count", 5, events.length);
                    break;
                case 9:
                    Assert.assertEquals("Output event count", 5, events.length);
                    break;
                case 10:
                    Assert.assertEquals("Output event count", 5, events.length);
                    break;
                default:
                    Assert.fail();
                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();

        inputHandler.send(new Object[] { "Team1", 100d, 67d });
        inputHandler.send(new Object[] { "Team2", 90d, 60d });
        inputHandler.send(new Object[] { "Team3", 110d, 80d });
        inputHandler.send(new Object[] { "Team4", 80d, 56d });
        inputHandler.send(new Object[] { "Team5", 70d, 65d });
        inputHandler.send(new Object[] { "Team1", 150d, 70d });
        inputHandler.send(new Object[] { "Team2", 160d, 75d });
        inputHandler.send(new Object[] { "Team3", 150d, 80d });
        inputHandler.send(new Object[] { "Team4", 120d, 67d });
        inputHandler.send(new Object[] { "Team5", 125d, 70d });

        countDownLatch.await(1000, MILLISECONDS);
        Assert.assertEquals("Number of success events", 10, count);
        Assert.assertEquals("Event arrived", true, eventArrived);
        executionPlanRuntime.shutdown();
    }

}
