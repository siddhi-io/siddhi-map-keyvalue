siddhi-map-keyvalue
======================================

The **siddhi-map-keyvalue extension** is an extension to <a target="_blank" href="https://wso2.github.io/siddhi">Siddhi</a>
that provides functionality to convert events in KeyValue Map format to WSO2Event format and convert back from WSO2Event format to Key Value Map format.

Find some useful links below:

* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-map-keyvalue">Source code</a>
* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-map-keyvalue/releases">Releases</a>
* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-map-keyvalue/issues">Issue tracker</a>

## Latest API Docs

Latest API Docs is <a target="_blank" href="https://wso2-extensions.github.io/siddhi-map-keyvalue/api/1.0.0-SNAPSHOT">1.0.0-SNAPSHOT</a>.

## How to use

**Using the extension in <a target="_blank" href="https://github.com/wso2/product-sp">WSO2 Stream Processor</a>**

* You can use this extension in the latest <a target="_blank" href="https://github.com/wso2/product-sp/releases">WSO2 Stream Processor</a> that is a part of <a target="_blank" href="http://wso2.com/analytics?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">WSO2 Analytics</a> offering, with editor, debugger and simulation support.

* This extension is shipped by default with WSO2 Stream Processor, if you wish to use an alternative version of this extension you can replace the component <a target="_blank" href="https://github.com/wso2-extensions/siddhi-map-keyvalue/releases">jar</a> that can be found in the `<STREAM_PROCESSOR_HOME>/lib` directory.

**Using the extension as a <a target="_blank" href="https://wso2.github.io/siddhi/documentation/running-as-a-java-library">java library</a>**

* This extension can be added as a maven dependency along with other Siddhi dependencies to your project.

```
     <dependency>
        <groupId>org.wso2.extension.siddhi.map.keyvalue</groupId>
        <artifactId>siddhi-map-keyvalue</artifactId>
        <version>x.x.x</version>
     </dependency>
```

## Jenkins Build Status

---

|  Branch | Build Status |
| :------ |:------------ |
| master  | [![Build Status](https://wso2.org/jenkins/job/siddhi/job/siddhi-map-keyvalue/badge/icon)](https://wso2.org/jenkins/job/siddhi/job/siddhi-map-keyvalue/) |

---

## Features

* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-map-keyvalue/api/1.0.0-SNAPSHOT/#keyvalue-source-mapper">keyvalue</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#source-mappers">Source Mapper</a>)*<br><div style="padding-left: 1em;"><p>Key-Value Map to Event input mapper. Transports which accepts key value maps can utilize this extension to convert the incoming key value pairs to Siddhi event. Users can either send predefined keys where conversion will happen without any configs or can use custom keys to map from message</p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-map-keyvalue/api/1.0.0-SNAPSHOT/#keyvalue-sink-mapper">keyvalue</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#sink-mappers">Sink Mapper</a>)*<br><div style="padding-left: 1em;"><p>Event to Key-Value Map output mapper. Transports which publish messages can utilize this extension to convert the Siddhi event to Key-Value Map message. Users can either user predefined keys and values or use custom keys and values</p></div>

## How to Contribute

  * Please report issues at <a target="_blank" href="https://github.com/wso2-extensions/siddhi-map-keyvalue/issues">GitHub Issue Tracker</a>.

  * Send your contributions as pull requests to <a target="_blank" href="https://github.com/wso2-extensions/siddhi-map-keyvalue/tree/master">master branch</a>.

## Contact us

 * Post your questions with the <a target="_blank" href="http://stackoverflow.com/search?q=siddhi">"Siddhi"</a> tag in <a target="_blank" href="http://stackoverflow.com/search?q=siddhi">Stackoverflow</a>.

 * Siddhi developers can be contacted via the mailing lists:

    Developers List   : [dev@wso2.org](mailto:dev@wso2.org)

    Architecture List : [architecture@wso2.org](mailto:architecture@wso2.org)

## Support

* We are committed to ensuring support for this extension in production. Our unique approach ensures that all support leverages our open development methodology and is provided by the very same engineers who build the technology.

* For more details and to take advantage of this unique opportunity contact us via <a target="_blank" href="http://wso2.com/support?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">http://wso2.com/support/</a>.
