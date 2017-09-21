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

package org.wso2.extension.siddhi.map.keyvalue.sourcemapper;

import org.apache.log4j.Logger;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.source.AttributeMapping;
import org.wso2.siddhi.core.stream.input.source.InputEventHandler;
import org.wso2.siddhi.core.stream.input.source.SourceMapper;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.core.util.transport.OptionHolder;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

import java.util.List;
import java.util.Map;

/**
 * This mapper converts Key Value pair set input to {@link org.wso2.siddhi.core.event.ComplexEventChunk}.
 */
@Extension(
        name = "keyvalue",
        namespace = "sourceMapper",
        description = "`Key-Value Map to Event` input mapper extension allows transports that accept events as key " +
                "value maps to convert those events to Siddhi events. You can either receive pre-defined keys where " +
                "conversion takes place without extra configurations, or use custom keys to map from the message.",
        parameters = {
                @Parameter(name = "fail.on.missing.attribute",
                        description = " If this parameter is set to `true`, if an event arrives without a matching " +
                                "key for a specific attribute in the connected stream, it is dropped and not " +
                                "processed by the Stream Processor. If this parameter is set to `false` the Stream " +
                                "Processor adds the required key to such events with a null value, and the event is " +
                                "converted to a Siddhi event so that you could handle them as required before they " +
                                "are further processed.",
                        defaultValue = "true",
                        optional = true,
                        type = {DataType.BOOL})
        },
        examples = {
                @Example(
                        syntax = "@source(type='inMemory', topic='stock', @map(type='keyvalue'))\n"
                                + "define stream FooStream (symbol string, price float, volume long);\n",
                        description = "This query performs a default key value input mapping. The expected "
                                + "input is a map similar to the following:\n"
                                + "symbol: 'WSO2'\n"
                                + "price: 55.6f\n"
                                + "volume: 100"
                ),
                @Example(
                        syntax = "@source(type='inMemory', topic='stock', " +
                                "@map(type='keyvalue', fail.on.missing.attribute='true', " +
                                "@attributes(symbol = 's', price = 'p', volume = 'v')))" +
                                "define stream FooStream (symbol string, price float, volume long); ",
                        description = "This query performs a custom key value input mapping. The matching keys " +
                                "for the `symbol`, `price` and `volume` attributes are be `s`, `p, and `v` " +
                                "respectively.  The expected input is a map similar to the following:\n"
                                + "s: 'WSO2'\n"
                                + "p: 55.6\n"
                                + "v: 100"
                )

        }
)
public class KeyValueSourceMapper extends SourceMapper {

    private static final String FAIL_ON_MISSING_ATTRIBUTE_IDENTIFIER = "fail.on.missing.attribute";
    private static final Logger log = Logger.getLogger(KeyValueSourceMapper.class);

    private StreamDefinition streamDefinition;
    private MappingPositionData[] mappingPositions;
    private List<Attribute> streamAttributes;
    private boolean failOnMissingAttribute = true;
    private int attributesSize;

    @Override
    public void init(StreamDefinition streamDefinition, OptionHolder optionHolder,
                     List<AttributeMapping> attributeMappingList, ConfigReader configReader,
                     SiddhiAppContext siddhiAppContext) {

        this.streamDefinition = streamDefinition;
        this.streamAttributes = this.streamDefinition.getAttributeList();
        this.attributesSize = this.streamDefinition.getAttributeList().size();
        this.mappingPositions = new MappingPositionData[attributesSize];
        this.failOnMissingAttribute = Boolean.parseBoolean(optionHolder.
                validateAndGetStaticValue(FAIL_ON_MISSING_ATTRIBUTE_IDENTIFIER, "true"));

        if (attributeMappingList != null && attributeMappingList.size() > 0) {

            for (int i = 0; i < attributeMappingList.size(); i++) {
                AttributeMapping attributeMapping = attributeMappingList.get(i);
                String attributeName = attributeMapping.getName();
                int position = this.streamDefinition.getAttributePosition(attributeName);
                this.mappingPositions[i] = new MappingPositionData(position, attributeMapping.getMapping());
            }
        } else {
            for (int i = 0; i < attributesSize; i++) {
                this.mappingPositions[i] = new MappingPositionData(i, this
                        .streamDefinition.getAttributeList().get(i).getName());
            }
        }
    }


    @Override
    public Class[] getSupportedInputEventClasses() {
        return new Class[]{Map.class};
    }

    @Override
    protected void mapAndProcess(Object eventObject, InputEventHandler inputEventHandler) throws InterruptedException {
        Event convertedEvent = createEventForMapping(eventObject);
        if (convertedEvent != null) {
            inputEventHandler.sendEvent(convertedEvent);
        }

    }

    private Event createEventForMapping(Object eventObject) {
        if (eventObject == null) {
            log.error("Null object received");
            return null;
        }
        if (!(eventObject instanceof Map)) {
            log.error("Invalid Map object received. Expected Map, but found " +
                    eventObject.getClass().getCanonicalName());
            return null;
        }
        Event event = new Event(attributesSize);
        Object data[] = event.getData();
        Map<String, Object> keyValueEvent = (Map<String, Object>) eventObject;

        for (MappingPositionData mapData : mappingPositions) {
            int position = mapData.position;
            String key = mapData.mapping;
            Attribute.Type type = streamAttributes.get(position).getType();
            Object value = keyValueEvent.get(key);
            if (!keyValueEvent.containsKey(key)) {
                if (failOnMissingAttribute) {
                    log.error("Stream \"" + streamDefinition.getId() +
                            "\" has an attribute named \"" + key +
                            "\", but the received event " + eventObject.toString() +
                            " does not has a value for that attribute. Hence dropping the message.");
                    return null;
                } else {
                    log.debug("Stream \"" + streamDefinition.getId() +
                            "\" has an attribute named \"" + key +
                            "\", but the received event " + eventObject.toString() +
                            " does not has a value for that attribute. Since fail.on.missing.attribute is false" +
                            "null value inserted");
                    data[position] = null;
                    continue;
                }
            }
            if (value == null) {
                data[position] = null;
                continue;
            }
            switch (type) {
                case BOOL:
                    if (value instanceof Boolean) {
                        data[position] = value;
                    } else {
                        log.error("Message " + eventObject.toString() +
                                " contains incompatible attribute types and values. Value " +
                                value + " is not compatible with type BOOL," +
                                "Hence dropping the message");
                        return null;
                    }
                    break;
                case INT:
                    if (value instanceof Integer) {
                        data[position] = value;
                    } else {
                        log.error("Message " + eventObject.toString() +
                                " contains incompatible attribute types and values. Value " +
                                value + " is not compatible with type INTEGER," +
                                "Hence dropping the message");
                        return null;
                    }
                    break;
                case DOUBLE:
                    if (value instanceof Double) {
                        data[position] = value;
                    } else {
                        log.error("Message " + eventObject.toString() +
                                " contains incompatible attribute types and values. Value " +
                                value + " is not compatible with type DOUBLE," +
                                "Hence dropping the message");
                        return null;
                    }
                    break;
                case STRING:
                    if (value instanceof String) {
                        data[position] = value;
                    } else {
                        log.error("Message " + eventObject.toString() +
                                " contains incompatible attribute types and values. Value " +
                                value + " is not compatible with type STRING," +
                                "Hence dropping the message");
                        return null;
                    }
                    break;
                case FLOAT:
                    if (value instanceof Float) {
                        data[position] = value;
                    } else {
                        log.error("Message " + eventObject.toString() +
                                " contains incompatible attribute types and values. Value " +
                                value + " is not compatible with type FLOAT," +
                                "Hence dropping the message");
                        return null;
                    }
                    break;
                case LONG:
                    if (value instanceof Long) {
                        data[position] = value;
                    } else {
                        log.error("Message " + eventObject.toString() +
                                " contains incompatible attribute types and values. Value " +
                                value + " is not compatible with type LONG," +
                                "Hence dropping the message");
                        return null;
                    }
                    break;
                default:
                    log.error("Stream Definition's attribute type, \"" + type + "\", is not supported." +
                            "Hence dropping the message");
                    return null;
            }
        }

        return event;
    }

    private static class MappingPositionData {
        /**
         * Attribute position in the output stream.
         */
        private int position;

        /**
         * The mapping as defined by the user.
         */
        private String mapping;

        public MappingPositionData(int position, String mapping) {
            this.position = position;
            this.mapping = mapping;
        }

        public int getPosition() {
            return position;
        }

        public void setPosition(int position) {
            this.position = position;
        }

        public String getMapping() {
            return mapping;
        }

        public void setMapping(String mapping) {
            this.mapping = mapping;
        }
    }
}
