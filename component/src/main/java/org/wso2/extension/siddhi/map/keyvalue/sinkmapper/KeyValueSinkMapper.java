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

import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.output.sink.SinkListener;
import org.wso2.siddhi.core.stream.output.sink.SinkMapper;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.core.util.transport.OptionHolder;
import org.wso2.siddhi.core.util.transport.TemplateBuilder;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

import java.util.HashMap;
import java.util.Map;

/**
 * Mapper class convert a Siddhi message to a Key Value pairs
 */
@Extension(
        name = "keyvalue",
        namespace = "sinkMapper",
        description = "The `Event to Key-Value Map` output mapper extension allows you to convert Siddhi events " +
                "processed by WSO2 SP to key-value map events before publishing them. You can either use " +
                "pre-defined keys where conversion takes place without extra configurations, or use custom keys " +
                "with which the messages can be published.",
        examples = {
                @Example(
                        syntax = "@sink(type='inMemory', topic='stock', @map(type='keyvalue'))\n"
                                + "define stream FooStream (symbol string, price float, volume long);\n",
                        description = "This query performs a default Key-Value output mapping. The expected output " +
                                "is something similar to the following:"
                                + "symbol:'WSO2'\n"
                                + "price : 55.6f\n"
                                + "volume: 100L"
                ),

                @Example(
                        syntax = "@sink(type='inMemory', topic='stock', @map(type='keyvalue', "
                                + "@payload(a='symbol',b='price',c='volume')))\n"
                                + "define stream FooStream (symbol string, price float, volume long);\n",
                        description = "This query performs a custom Key-Value output mapping where values are passed" +
                                " as objects. Values for `symbol`, `price`, and `volume` attributes are published " +
                                "with the keys `a`, `b` and `c` respectively. The expected output is a map similar " +
                                "to the following:\n"
                                + "a:'WSO2'\n"
                                + "b : 55.6f\n"
                                + "c: 100L"
                ),

                @Example(
                        syntax = "@sink(type='inMemory', topic='stock', @map(type='keyvalue', "
                                + "@payload(a='{{symbol}} is here',b='`price`',c='volume')))\n"
                                + "define stream FooStream (symbol string, price float, volume long);\n",
                        description = "This query performs a custom Key-Value output mapping where the values of " +
                                "the `a` and `b` attributes are strings and c is object. The expected output should " +
                                "be a Map similar to the following:"
                                + "a:'WSO2 is here'\n"
                                + "b : 'price'\n"
                                + "c: 100L"
                )
        }
)
public class KeyValueSinkMapper extends SinkMapper {

    private String[] attributeNameArray;

    @Override
    public String[] getSupportedDynamicOptions() {
        return new String[0];
    }

    @Override
    public void init(StreamDefinition streamDefinition, OptionHolder optionHolder,
                     Map<String, TemplateBuilder> templateBuilder,
                     ConfigReader configReader, SiddhiAppContext siddhiAppContext) {
        attributeNameArray = streamDefinition.getAttributeNameArray();
    }

    @Override
    public Class[] getOutputEventClasses() {
        return new Class[]{Map.class};
    }

    @Override
    public void mapAndSend(Event[] events, OptionHolder optionHolder, Map<String,
            TemplateBuilder> payloadTemplateBuilderMap, SinkListener sinkListener) {
        for (Event event : events) {
            mapAndSend(event, optionHolder, payloadTemplateBuilderMap, sinkListener);
        }
    }

    @Override
    public void mapAndSend(Event event, OptionHolder optionHolder,
                           Map<String, TemplateBuilder> payloadTemplateBuilderMap, SinkListener sinkListener) {

        Map<String, Object> result = new HashMap<>();

        if (payloadTemplateBuilderMap != null) {

            for (Map.Entry<String, TemplateBuilder> entry : payloadTemplateBuilderMap.entrySet()) {
                result.put(entry.getKey(), entry.getValue().build(event));
            }
        } else {
            Object data[] = event.getData();
            for (int i = 0; i < data.length; i++) {
                String attributeName = attributeNameArray[i];
                result.put(attributeName, data[i]);
            }
        }

        sinkListener.publish(result);
    }
}
