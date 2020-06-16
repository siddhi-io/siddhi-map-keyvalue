Siddhi Map KeyValue
====================

  [![Jenkins Build Status](https://wso2.org/jenkins/job/siddhi/job/siddhi-map-keyvalue/badge/icon)](https://wso2.org/jenkins/job/siddhi/job/siddhi-map-keyvalue/)
  [![GitHub Release](https://img.shields.io/github/release/siddhi-io/siddhi-map-keyvalue.svg)](https://github.com/siddhi-io/siddhi-map-keyvalue/releases)
  [![GitHub Release Date](https://img.shields.io/github/release-date/siddhi-io/siddhi-map-keyvalue.svg)](https://github.com/siddhi-io/siddhi-map-keyvalue/releases)
  [![GitHub Open Issues](https://img.shields.io/github/issues-raw/siddhi-io/siddhi-map-keyvalue.svg)](https://github.com/siddhi-io/siddhi-map-keyvalue/issues)
  [![GitHub Last Commit](https://img.shields.io/github/last-commit/siddhi-io/siddhi-map-keyvalue.svg)](https://github.com/siddhi-io/siddhi-map-keyvalue/commits/master)
  [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

The **siddhi-map-keyvalue extension** is an extension to <a target="_blank" href="https://wso2.github.io/siddhi">Siddhi</a> that converts events having Key-Value maps to/from Siddhi events.

For information on <a target="_blank" href="https://siddhi.io/">Siddhi</a> and it's features refer <a target="_blank" href="https://siddhi.io/redirect/docs.html">Siddhi Documentation</a>. 

## Download

* Versions 2.x and above with group id `io.siddhi.extension.*` from <a target="_blank" href="https://mvnrepository.com/artifact/io.siddhi.extension.map.keyvalue/siddhi-map-keyvalue/">here</a>.
* Versions 1.x and lower with group id `org.wso2.extension.siddhi.*` from <a target="_blank" href="https://mvnrepository.com/artifact/org.wso2.extension.siddhi.map.keyvalue/siddhi-map-keyvalue">here</a>.

## Latest API Docs 

Latest API Docs is <a target="_blank" href="https://siddhi-io.github.io/siddhi-map-keyvalue/api/2.0.7">2.0.7</a>.

## Features

* <a target="_blank" href="https://siddhi-io.github.io/siddhi-map-keyvalue/api/2.0.7/#keyvalue-sink-mapper">keyvalue</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#sink-mapper">Sink Mapper</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;">The <code>Event to Key-Value Map</code> output mapper extension allows you to convert Siddhi events processed by WSO2 SP to key-value map events before publishing them. You can either use pre-defined keys where conversion takes place without extra configurations, or use custom keys with which the messages can be published.</p></p></div>
* <a target="_blank" href="https://siddhi-io.github.io/siddhi-map-keyvalue/api/2.0.7/#keyvalue-source-mapper">keyvalue</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#source-mapper">Source Mapper</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;"><code>Key-Value Map to Event</code> input mapper extension allows transports that accept events as key value maps to convert those events to Siddhi events. You can either receive pre-defined keys where conversion takes place without extra configurations, or use custom keys to map from the message.</p></p></div>

## Dependencies 

There are no other dependencies needed for this extension. 

## Installation

For installing this extension on various siddhi execution environments refer Siddhi documentation section on <a target="_blank" href="https://siddhi.io/redirect/add-extensions.html">adding extensions</a>.

## Support and Contribution

* We encourage users to ask questions and get support via <a target="_blank" href="https://stackoverflow.com/questions/tagged/siddhi">StackOverflow</a>, make sure to add the `siddhi` tag to the issue for better response.

* If you find any issues related to the extension please report them on <a target="_blank" href="https://github.com/siddhi-io/siddhi-execution-string/issues">the issue tracker</a>.

* For production support and other contribution related information refer <a target="_blank" href="https://siddhi.io/community/">Siddhi Community</a> documentation.

