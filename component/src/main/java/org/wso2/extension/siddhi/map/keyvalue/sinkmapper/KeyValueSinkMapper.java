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
        description = "Event to Key-Value Map output mapper. Transports which publish messages can utilize this "
                + "extension to convert the Siddhi event to Key-Value Map message. Users can either user " +
                "predefined keys and values or use custom keys and values",
        examples = {
                @Example(
                        syntax = "@sink(type='inMemory', topic='stock', @map(type='keyvalue'))\n"
                                + "define stream FooStream (symbol string, price float, volume long);\n",
                        description = "Above configuration will perform a default Key-Value output mapping."
                                + "Expected output will be a Map as follows,"
                                + "symbol:'WSO2'"
                                + "price : 55.6f"
                                + "volume: 100L"
                ),

                @Example(
                        syntax = "@sink(type='inMemory', topic='stock', @map(type='keyvalue', "
                                + "@payload(a='symbol',b='price',c='volume')))\n"
                                + "define stream FooStream (symbol string, price float, volume long);\n",
                        description = "Above configuration will perform a custom Key-Value output mapping where values"
                                + "values are passed as objects"
                                + "Expected output will be a Map as follows,"
                                + "a:'WSO2'"
                                + "b : 55.6f"
                                + "c: 100L"
                ),

                @Example(
                        syntax = "@sink(type='inMemory', topic='stock', @map(type='keyvalue', "
                                + "@payload(a='{{symbol}} is here',b='`price`',c='volume')))\n"
                                + "define stream FooStream (symbol string, price float, volume long);\n",
                        description = "Above configuration will perform a custom Key-Value output mapping where values"
                                + "of a,b are Strings and c is object"
                                + "Expected output will be a Map as follows,"
                                + "a:'WSO2 is here'"
                                + "b : 'price'"
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
