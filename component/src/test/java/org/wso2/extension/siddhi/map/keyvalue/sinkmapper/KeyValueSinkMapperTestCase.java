
/*
 * Copyright (c)  2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.extension.siddhi.map.keyvalue.sinkmapper;

import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.SiddhiTestHelper;
import org.wso2.siddhi.core.util.transport.InMemoryBroker;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class KeyValueSinkMapperTestCase {

    private static final Logger log = Logger.getLogger(KeyValueSinkMapper.class);
    private AtomicInteger wso2Count = new AtomicInteger(0);
    private AtomicInteger ibmCount = new AtomicInteger(0);

    @BeforeMethod
    public void init() {
        wso2Count.set(0);
        ibmCount.set(0);
    }

    /*
    * Default keyvalue output mapping
    */
    @Test
    public void keyvalueSinkMapperDefaultTestCase1() throws InterruptedException {
        log.info("KeyValueSinkMapper-Default TestCase 1");
        InMemoryBroker.Subscriber subscriberWSO2 = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {

                HashMap<String, Object> map;
                switch (wso2Count.incrementAndGet()) {
                    case 1:
                        map = new HashMap<>();
                        map.put("symbol", "WSO2");
                        map.put("price", 55.6f);
                        map.put("volume", 100L);
                        AssertJUnit.assertEquals(map, msg);
                        break;
                    case 2:
                        map = new HashMap<>();
                        map.put("symbol", "WSO2");
                        map.put("price", 57.678f);
                        map.put("volume", 100L);
                        AssertJUnit.assertEquals(map, msg);
                        break;

                    case 3:
                        map = new HashMap<>();
                        map.put("symbol", "WSO2");
                        map.put("price", 50f);
                        map.put("volume", 100L);
                        AssertJUnit.assertEquals(map, msg);
                        break;

                    case 4:
                        map = new HashMap<>();
                        map.put("symbol", "WSO2#$%");
                        map.put("price", 50f);
                        map.put("volume", 100L);
                        AssertJUnit.assertEquals(map, msg);
                        break;

                    case 5:
                        map = new HashMap<>();
                        map.put("symbol", "WSO2");
                        map.put("price", 55.6f);
                        map.put("volume", 100L);
                        AssertJUnit.assertEquals(map, msg);
                        break;

                    case 6:
                        map = new HashMap<>();
                        map.put("symbol", "IBM");
                        map.put("price", 32.6f);
                        map.put("volume", 160L);
                        AssertJUnit.assertEquals(map, msg);
                        break;

                    default:
                        log.error("Received more than expected number of events. Expected maximum : 6," +
                                "Received : " + wso2Count.get());
                        AssertJUnit.fail();
                }
            }

            @Override
            public String getTopic() {
                return "WSO2";
            }
        };

        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriberWSO2);

        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='inMemory', topic='WSO2', @map(type='keyvalue')) " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");

        siddhiAppRuntime.start();


        Event wso2Event = new Event();
        Event ibmEvent = new Event();
        Object[] wso2Data = {"WSO2", 55.6f, 100L};
        Object[] ibmData = {"IBM", 32.6f, 160L};
        wso2Event.setData(wso2Data);
        ibmEvent.setData(ibmData);

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.678f, 100L});
        stockStream.send(new Object[]{"WSO2", 50f, 100L});
        stockStream.send(new Object[]{"WSO2#$%", 50f, 100L});
        stockStream.send(new Event[]{wso2Event, ibmEvent});

        SiddhiTestHelper.waitForEvents(100, 6, wso2Count, 200);

        //assert event count
        AssertJUnit.assertEquals(6, wso2Count.get());
        siddhiAppRuntime.shutdown();

        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriberWSO2);
    }

    @Test
    public void keyvalueSinkMapperDefaultTestCase2() throws InterruptedException {
        log.info("KeyValueSinkMapper-Default TestCase 2");
        InMemoryBroker.Subscriber subscriberWSO2 = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {

                HashMap<String, Object> map;
                switch (wso2Count.incrementAndGet()) {
                    case 1:
                        map = new HashMap<>();
                        map.put("symbol", "WSO2");
                        map.put("price", null);
                        map.put("volume", 100L);
                        AssertJUnit.assertEquals(map, msg);
                        break;
                    case 2:
                        map = new HashMap<>();
                        map.put("symbol", "WSO2");
                        map.put("price", 57.678f);
                        map.put("volume", null);
                        AssertJUnit.assertEquals(map, msg);
                        break;

                    case 3:
                        map = new HashMap<>();
                        map.put("symbol", null);
                        map.put("price", 50f);
                        map.put("volume", 100L);
                        AssertJUnit.assertEquals(map, msg);
                        break;


                    default:
                        log.error("Received more than expected number of events. Expected maximum : 3," +
                                "Received : " + wso2Count.get());
                        AssertJUnit.fail();
                }
            }

            @Override
            public String getTopic() {
                return "WSO2";
            }
        };

        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriberWSO2);

        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='inMemory', topic='WSO2', @map(type='keyvalue')) " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");

        siddhiAppRuntime.start();


        stockStream.send(new Object[]{"WSO2", null, 100L});
        stockStream.send(new Object[]{"WSO2", 57.678f, null});
        stockStream.send(new Object[]{null, 50f, 100L});


        SiddhiTestHelper.waitForEvents(100, 3, wso2Count, 200);
        //assert event count
        AssertJUnit.assertEquals(3, wso2Count.get());
        siddhiAppRuntime.shutdown();

        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriberWSO2);
    }

    @Test
    public void keyvalueSinkMapperDefaultTestCase3() throws InterruptedException {
        log.info("KeyValueSinkMapper-Default TestCase 3");
        InMemoryBroker.Subscriber subscriberWSO2 = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {

                HashMap<String, Object> map;
                switch (wso2Count.incrementAndGet()) {
                    case 1:
                        map = new HashMap<>();
                        map.put("symbol", "WSO2");
                        map.put("price", 55.6f);
                        map.put("volume", 100L);
                        AssertJUnit.assertEquals(map, msg);
                        break;
                    case 2:
                        map = new HashMap<>();
                        map.put("symbol", "WSO2");
                        map.put("price", 57.678f);
                        map.put("volume", 101L);
                        AssertJUnit.assertEquals(map, msg);
                        break;


                    default:
                        log.error("Received more than expected number of events. Expected maximum : 2," +
                                "Received : " + wso2Count.get());
                        AssertJUnit.fail();
                }
            }

            @Override
            public String getTopic() {
                return "WSO2";
            }
        };

        InMemoryBroker.Subscriber subscriberIBM = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {

                HashMap<String, Object> map;
                switch (ibmCount.incrementAndGet()) {
                    case 1:
                        map = new HashMap<>();
                        map.put("symbol", "IBM");
                        map.put("price", 50f);
                        map.put("volume", 100L);
                        AssertJUnit.assertEquals(map, msg);
                        break;

                    default:
                        log.error("Received more than expected number of events. Expected maximum : 1," +
                                "Received : " + ibmCount.get());
                        AssertJUnit.fail();
                }
            }

            @Override
            public String getTopic() {
                return "IBM";
            }
        };
        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriberWSO2);
        InMemoryBroker.subscribe(subscriberIBM);

        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='inMemory', topic='{{symbol}}', @map(type='keyvalue')) " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");

        siddhiAppRuntime.start();


        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.678f, 101L});
        stockStream.send(new Object[]{"IBM", 50f, 100L});



        SiddhiTestHelper.waitForEvents(100, 2, wso2Count, 200);
        SiddhiTestHelper.waitForEvents(100, 1, ibmCount, 200);
        //assert event count
        AssertJUnit.assertEquals(2, wso2Count.get());
        AssertJUnit.assertEquals(1, ibmCount.get());
        siddhiAppRuntime.shutdown();

        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriberWSO2);
    }

    /*
    * Custom keyvalue output mapping
    */
    @Test
    public void keyvalueSinkMapperCustomTestCase1() throws InterruptedException {
        log.info("KeyValueSinkMapper-Custom TestCase 1");
        InMemoryBroker.Subscriber subscriberWSO2 = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {

                HashMap<String, Object> map;
                switch (wso2Count.incrementAndGet()) {
                    case 1:
                        map = new HashMap<>();
                        map.put("a", "WSO2");
                        map.put("b", 55.6f);
                        map.put("c", 100L);
                        AssertJUnit.assertEquals(map, msg);
                        break;
                    case 2:
                        map = new HashMap<>();
                        map.put("a", "WSO2");
                        map.put("b", 57.678f);
                        map.put("c", 100L);
                        AssertJUnit.assertEquals(map, msg);
                        break;

                    case 3:
                        map = new HashMap<>();
                        map.put("a", "WSO2");
                        map.put("b", 50f);
                        map.put("c", 100L);
                        AssertJUnit.assertEquals(map, msg);
                        break;

                    case 4:
                        map = new HashMap<>();
                        map.put("a", "WSO2#$%");
                        map.put("b", 50f);
                        map.put("c", 100L);
                        AssertJUnit.assertEquals(map, msg);
                        break;

                    case 5:
                        map = new HashMap<>();
                        map.put("a", "WSO2");
                        map.put("b", 55.6f);
                        map.put("c", 100L);
                        AssertJUnit.assertEquals(map, msg);
                        break;

                    case 6:
                        map = new HashMap<>();
                        map.put("a", "IBM");
                        map.put("b", 32.6f);
                        map.put("c", 160L);
                        AssertJUnit.assertEquals(map, msg);
                        break;

                    default:
                        log.error("Received more than expected number of events. Expected maximum : 6," +
                                "Received : " + wso2Count.get());
                        AssertJUnit.fail();
                }
            }

            @Override
            public String getTopic() {
                return "WSO2";
            }
        };

        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriberWSO2);

        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='inMemory', topic='WSO2', @map(type='keyvalue', @payload(a='symbol',b='price',c='volume')))"
                + " define stream `from` (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into `from`; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");

        siddhiAppRuntime.start();

        Event wso2Event = new Event();
        Event ibmEvent = new Event();
        Object[] wso2Data = {"WSO2", 55.6f, 100L};
        Object[] ibmData = {"IBM", 32.6f, 160L};
        wso2Event.setData(wso2Data);
        ibmEvent.setData(ibmData);

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.678f, 100L});
        stockStream.send(new Object[]{"WSO2", 50f, 100L});
        stockStream.send(new Object[]{"WSO2#$%", 50f, 100L});
        stockStream.send(new Event[]{wso2Event, ibmEvent});

        SiddhiTestHelper.waitForEvents(100, 6, wso2Count, 200);
        //assert event count
        AssertJUnit.assertEquals(6, wso2Count.get());
        siddhiAppRuntime.shutdown();

        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriberWSO2);
    }

    @Test
    public void keyvalueSinkMapperCustomTestCase2() throws InterruptedException {
        log.info("KeyValueSinkMapper-Custom TestCase 2");
        InMemoryBroker.Subscriber subscriberWSO2 = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {

                HashMap<String, Object> map;
                switch (wso2Count.incrementAndGet()) {
                    case 1:
                        map = new HashMap<>();
                        map.put("a", "WSO2 is here");
                        map.put("b", "price");
                        map.put("c", 100L);
                        AssertJUnit.assertEquals(map, msg);
                        break;
                    case 2:
                        map = new HashMap<>();
                        map.put("a", "WSO2 is here");
                        map.put("b", "price");
                        map.put("c", 100L);
                        AssertJUnit.assertEquals(map, msg);
                        break;
                    case 3:
                        map = new HashMap<>();
                        map.put("a", "WSO2#$% is here");
                        map.put("b", "price");
                        map.put("c", 100L);
                        AssertJUnit.assertEquals(map, msg);
                        break;
                    default:
                        log.error("Received more than expected number of events. Expected maximum : 3," +
                                "Received : " + wso2Count.get());
                        AssertJUnit.fail();
                }
            }

            @Override
            public String getTopic() {
                return "WSO2";
            }
        };

        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriberWSO2);

        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='inMemory', topic='WSO2', @map(type='keyvalue', " +
                "@payload(a='{{symbol}} is here',b='`price`',c='volume'))) " +
                "define stream `from` (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into `from`; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");

        siddhiAppRuntime.start();


        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.678f, 100L});
        stockStream.send(new Object[]{"WSO2#$%", 50f, 100L});


        SiddhiTestHelper.waitForEvents(100, 3, wso2Count, 200);
        //assert event count
        AssertJUnit.assertEquals(3, wso2Count.get());
        siddhiAppRuntime.shutdown();

        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriberWSO2);
    }

    @Test
    public void keyvalueSinkMapperCustomTestCase3() throws InterruptedException {
        log.info("KeyValueSinkMapper-Custom TestCase 2");
        InMemoryBroker.Subscriber subscriberWSO2 = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {

                HashMap<String, Object> map;
                switch (wso2Count.incrementAndGet()) {
                    case 1:
                        map = new HashMap<>();
                        map.put("a", "WSO2 is here");
                        map.put("b", "price");
                        map.put("c", 100L);
                        AssertJUnit.assertEquals(map, msg);
                        break;
                    case 2:
                        map = new HashMap<>();
                        map.put("a", "WSO2 is here");
                        map.put("b", "price");
                        map.put("c", null);
                        AssertJUnit.assertEquals(map, msg);
                        break;
                    case 3:
                        map = new HashMap<>();
                        map.put("a", "null is here");
                        map.put("b", "price");
                        map.put("c", 100L);
                        AssertJUnit.assertEquals(map, msg);
                        break;
                    default:
                        log.error("Received more than expected number of events. Expected maximum : 3," +
                                "Received : " + wso2Count.get());
                        AssertJUnit.fail();
                }
            }

            @Override
            public String getTopic() {
                return "WSO2";
            }
        };

        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriberWSO2);

        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='inMemory', topic='WSO2', @map(type='keyvalue', " +
                "@payload(a='{{symbol}} is here',b='`price`',c='volume'))) " +
                "define stream `from` (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into `from`; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");

        siddhiAppRuntime.start();


        stockStream.send(new Object[]{"WSO2", null, 100L});
        stockStream.send(new Object[]{"WSO2", 57.678f, null});
        stockStream.send(new Object[]{null, 50f, 100L});


        SiddhiTestHelper.waitForEvents(100, 3, wso2Count, 200);
        //assert event count
        AssertJUnit.assertEquals(3, wso2Count.get());
        siddhiAppRuntime.shutdown();

        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriberWSO2);
    }

    @Test
    public void keyvalueSinkMapperCustomTestCase4() throws InterruptedException {
        log.info("KeyValueSinkMapper-Custom TestCase 4");
        InMemoryBroker.Subscriber subscriberWSO2 = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {

                HashMap<String, Object> map;
                switch (wso2Count.incrementAndGet()) {
                    case 1:
                        map = new HashMap<>();
                        map.put(null, "WSO2 is here");
                        AssertJUnit.assertEquals(map, msg);
                        break;
                    case 2:
                        map = new HashMap<>();
                        map.put(null, "IBM is here");
                        AssertJUnit.assertEquals(map, msg);
                        break;

                    default:
                        log.error("Received more than expected number of events. Expected maximum : 2," +
                                "Received : " + wso2Count.get());
                        AssertJUnit.fail();
                }
            }

            @Override
            public String getTopic() {
                return "WSO2";
            }
        };

        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriberWSO2);

        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='inMemory', topic='WSO2', @map(type='keyvalue', @payload('{{symbol}} is here'))) " +
                //not supporting duplicate keys
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");

        siddhiAppRuntime.start();


        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 57.678f, 100L});

        SiddhiTestHelper.waitForEvents(100, 2, wso2Count, 200);
        //assert event count
        AssertJUnit.assertEquals(2, wso2Count.get());
        siddhiAppRuntime.shutdown();

        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriberWSO2);
    }

    @Test
    public void keyvalueSinkMapperCustomTestCase5() throws InterruptedException {
        log.info("KeyValueSinkMapper-Custom TestCase 5");
        InMemoryBroker.Subscriber subscriberWSO2 = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {

                HashMap<String, Object> map;
                switch (wso2Count.incrementAndGet()) {
                    case 1:
                        map = new HashMap<>();
                        map.put("a", "WSO2");
                        map.put("b", "WSO2");
                        map.put("c", "55.6");
                        map.put("d", 55.6f);
                        AssertJUnit.assertEquals(map, msg);
                        break;
                    case 2:
                        map = new HashMap<>();
                        map.put("a", "WSO2");
                        map.put("b", "WSO2");
                        map.put("c", "57.678");
                        map.put("d", 57.678f);
                        AssertJUnit.assertEquals(map, msg);
                        break;

                    default:
                        log.error("Received more than expected number of events. Expected maximum : 2," +
                                "Received : " + wso2Count.get());
                        AssertJUnit.fail();
                }
            }

            @Override
            public String getTopic() {
                return "WSO2";
            }
        };

        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriberWSO2);

        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='inMemory', topic='WSO2', @map(type='keyvalue', " +
                "@payload(a='{{symbol}}',b='symbol',c='{{price}}',d='price'))) " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");

        siddhiAppRuntime.start();


        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.678f, 100L});

        SiddhiTestHelper.waitForEvents(100, 2, wso2Count, 200);
        //assert event count
        AssertJUnit.assertEquals(2, wso2Count.get());
        siddhiAppRuntime.shutdown();

        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriberWSO2);
    }

}
