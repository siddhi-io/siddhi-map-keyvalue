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

package io.siddhi.extension.map.keyvalue.sourcemapper;

import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.event.Event;
import io.siddhi.core.stream.input.source.AttributeMapping;
import io.siddhi.core.stream.input.source.InputEventHandler;
import io.siddhi.core.stream.input.source.SourceMapper;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.StreamDefinition;
import org.apache.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This mapper converts Key Value pair set input to {@link io.siddhi.core.event.ComplexEventChunk}.
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
                                + "input is a map similar to the following: \n"
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
                                "for the `symbol`, `price` and `volume` attributes are be `s`, `p`, and `v` " +
                                "respectively. The expected input is a map similar to the following: \n"
                                + "s: 'WSO2' \n"
                                + "p: 55.6 \n"
                                + "v: 100 \n"
                )

        }
)
public class KeyValueSourceMapper extends SourceMapper {

    private static final String FAIL_ON_MISSING_ATTRIBUTE_IDENTIFIER = "fail.on.missing.attribute";
    private static final Logger log = Logger.getLogger(KeyValueSourceMapper.class);

    private StreamDefinition streamDefinition;
    private List<AttributeMapping> attributeMappingList;
    private List<Attribute> streamAttributes;
    private boolean customMapping = false;
    private boolean failOnMissingAttribute = true;
    private int attributesSize;

    @Override
    public void init(StreamDefinition streamDefinition, OptionHolder optionHolder,
                     List<AttributeMapping> attributeMappingList, ConfigReader configReader,
                     SiddhiAppContext siddhiAppContext) {

        this.streamDefinition = streamDefinition;
        this.attributeMappingList = attributeMappingList;
        this.streamAttributes = this.streamDefinition.getAttributeList();
        this.attributesSize = this.streamDefinition.getAttributeList().size();
        this.failOnMissingAttribute = Boolean.parseBoolean(optionHolder.
                validateAndGetStaticValue(FAIL_ON_MISSING_ATTRIBUTE_IDENTIFIER, "true"));

        if (attributeMappingList != null && attributeMappingList.size() > 0) {
            customMapping = true;
        } else {
            this.attributeMappingList = new ArrayList<>(streamDefinition.getAttributeList().size());
            for (int i = 0; i < attributesSize; i++) {
                String name = this.streamDefinition.getAttributeList().get(i).getName();
                this.attributeMappingList.add(new AttributeMapping(name, i, name));
            }
        }
    }


    @Override
    public Class[] getSupportedInputEventClasses() {
        return new Class[]{Map.class, byte[].class};
    }

    @Override
    protected void mapAndProcess(Object eventObject, InputEventHandler inputEventHandler) throws InterruptedException {
        Event convertedEvent = createEventForMapping(eventObject);
        if (convertedEvent != null) {
            inputEventHandler.sendEvent(convertedEvent);
        }
    }

    @Override
    protected boolean allowNullInTransportProperties() {
        return !failOnMissingAttribute;
    }

    private Event createEventForMapping(Object eventObject) {
        Map<String, Object> keyValueEvent = null;

        if (eventObject == null) {
            log.error("Null object received");
            return null;
        }

        if (eventObject instanceof byte[]) {
            Object deserializeObject = deserialize((byte[]) eventObject);
            if (deserializeObject instanceof Map) {
                keyValueEvent = (Map<String, Object>) deserializeObject;
            }
        } else if (eventObject instanceof Map) {
            keyValueEvent = (Map<String, Object>) eventObject;
        } else {
            log.error("Invalid Map object received. Expected Map, but found " +
                    eventObject.getClass().getCanonicalName());
            return null;
        }

        Event event = new Event(attributesSize);
        Object data[] = event.getData();

        for (AttributeMapping attributeMapping : attributeMappingList) {
            int position = attributeMapping.getPosition();
            Attribute.Type type = streamAttributes.get(position).getType();
            Object value = keyValueEvent.get(attributeMapping.getMapping());
            if (value == null) {
                data[position] = null;
                if (failOnMissingAttribute) {
                    log.error("Stream \"" + streamDefinition.getId() +
                            "\" has an attribute named \"" + attributeMapping.getName() +
                            "\", but the received event " + keyValueEvent.toString() +
                            " does not has a value for that attribute. Hence dropping the message.");
                    return null;
                } else {
                    log.debug("Stream \"" + streamDefinition.getId() +
                            "\" has an attribute named \"" + attributeMapping.getName() +
                            "\", but the received event " + keyValueEvent.toString() +
                            " does not has a value for that attribute. Since fail.on.missing.attribute is false" +
                            "null value inserted");
                    data[position] = null;
                    continue;
                }
            }
            switch (type) {
                case BOOL:
                    if (value instanceof Boolean) {
                        data[position] = value;
                    } else {
                        log.error("Message " + keyValueEvent.toString() +
                                " contains incompatible attribute types and values. Value " +
                                value + " is not compatible with type BOOL," +
                                "Hence dropping the message");
                        return null;
                    }
                    break;
                case INT:
                    if (value instanceof Integer) {
                        data[position] = value;
                    } else if (value instanceof BigInteger) {
                        data[position] = ((BigInteger) value).intValue();
                    } else if (value instanceof BigDecimal) {
                        data[position] = ((BigDecimal) value).intValue();
                    } else {
                        log.error("Message " + keyValueEvent.toString() +
                                " contains incompatible attribute types and values. Value " +
                                value + " is not compatible with type INTEGER," +
                                "Hence dropping the message");
                        return null;
                    }
                    break;
                case DOUBLE:
                    if (value instanceof Double) {
                        data[position] = value;
                    } else if (value instanceof BigDecimal) {
                        data[position] = ((BigDecimal) value).doubleValue();
                    } else if (value instanceof BigInteger) {
                        data[position] = ((BigInteger) value).doubleValue();
                    } else {
                        log.error("Message " + keyValueEvent.toString() +
                                " contains incompatible attribute types and values. Value " +
                                value + " is not compatible with type DOUBLE," +
                                "Hence dropping the message");
                        return null;
                    }
                    break;
                case STRING:
                    if (value instanceof String || value instanceof BigInteger || value instanceof BigDecimal) {
                        data[position] = value.toString();
                    } else {
                        log.error("Message " + keyValueEvent.toString() +
                                " contains incompatible attribute types and values. Value " +
                                value + " is not compatible with type STRING," +
                                "Hence dropping the message");
                        return null;
                    }
                    break;
                case FLOAT:
                    if (value instanceof Float) {
                        data[position] = value;
                    } else if (value instanceof BigInteger) {
                        data[position] = ((BigInteger) value).floatValue();
                    } else if (value instanceof BigDecimal) {
                        data[position] = ((BigDecimal) value).floatValue();
                    } else {
                        log.error("Message " + keyValueEvent.toString() +
                                " contains incompatible attribute types and values. Value " +
                                value + " is not compatible with type FLOAT," +
                                "Hence dropping the message");
                        return null;
                    }
                    break;
                case LONG:
                    if (value instanceof Long) {
                        data[position] = value;
                    } else if (value instanceof BigInteger) {
                        data[position] = ((BigInteger) value).longValue();
                    } else if (value instanceof BigDecimal) {
                        data[position] = ((BigDecimal) value).longValue();
                    } else if (value instanceof Timestamp) {
                        data[position] = ((Timestamp) value).getTime();
                    } else {
                        log.error("Message " + keyValueEvent.toString() +
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

    /**
     * Method to deserialize the byte array into the original object.
     *
     * @param eventObject byte array to deserialize.
     * @return Object after deserialized the byte array or null if error is occurred while deserializing the byte array.
     */
    private Object deserialize(byte[] eventObject) {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(eventObject);
        ObjectInputStream objectInputStream;
        try {
            objectInputStream = new ObjectInputStream(byteArrayInputStream);
            return objectInputStream.readObject();
        } catch (IOException | ClassNotFoundException e) {
            log.error("Error is encountered when deserialize the byte array to Map Object"
                    + e.getMessage(), e);
            return null;
        }
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
