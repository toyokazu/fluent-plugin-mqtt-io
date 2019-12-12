# Fluent::Plugin::Mqtt::IO

Fluent plugin for MQTT Input/Output.
Mqtt::IO Plugin is deeply inspired by Fluent::Plugin::Mqtt.

https://github.com/yuuna/fluent-plugin-mqtt

Mqtt::IO plugin focus on federating components, e.g. sensors, messaging platform and databases. Encryption/Decryption is not supported in this plugin but [fluent-plugin-jwt-filter](https://github.com/toyokazu/fluent-plugin-jwt-filter) can be used to encrypt/decrypt messages using JSON Web Token technology.

## Installation

Add this line to your application's Gemfile:

    gem 'fluent-plugin-mqtt-io'

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install fluent-plugin-mqtt-io


## Usage

fluent-plugin-mqtt-io provides Input and Output Plugins for MQTT.

### Input Plugin (Fluet::MqttInput)

Input Plugin can be used via source directive in the configuration.

For fluent-plugin-mqtt-io <= 0.2.3

```

<source>
  type mqtt
  host 127.0.0.1
  port 1883
  format json
</source>

```

For fluent-plugin-mqtt-io >= v0.3.0

```

<source>
  @type mqtt
  host 127.0.0.1
  port 1883
  <parse>
    @type json
  </parse>
</source>

```


The default MQTT topic is "#". Configurable options are the following:

- **host**: IP address of MQTT broker
- **port**: Port number of MQTT broker
- **client_id**: Client ID that to connect to MQTT broker
- **format** (mandatory): Input parser can be chosen, e.g. json, xml
  - In order to use xml format, you need to install [fluent-plugin-xml-parser](https://github.com/toyokazu/fluent-plugin-xml-parser).
  - Default time_key field for json format is 'time'
- **topic**: Topic name to be subscribed
- **bulk_trans**: Enable bulk transfer to support buffered output (mqtt_buf, Fluent::MqttBufferedOutput, defaut: true) only for fluent-plugin-mqtt-io <= 0.2.3
- **bulk_trans_sep**: A message separator for bulk transfer. The default separator is "\t". only for fluent-plugin-mqtt-io <= 0.2.3
- **username**: User name for authentication
- **password**: Password for authentication
- **clean_session**: Setting for clean session client option (false persists session offline)
- **keep_alive**: An interval of sending keep alive packet (default 15 sec)
- **ssl**: set true if you want to use SSL/TLS. If set to true, the following parameter must be provided
  - **ca_file**: CA certificate file path
  - **key_file**: private key file path
  - **cert_file**: certificate file path
- **recv_time**: Add receive time to message in millisecond (ms) as integer for debug and performance/delay analysis. only for fluent-plugin-mqtt-io <= 0.2.3
- **recv_time_key**: An attribute of recv_time. only for fluent-plugin-mqtt-io <= 0.2.3
- **monitor**: monitor section. only for fluent-plugin-mqtt-io >= 0.3.0
  - **recv_time**: Add receive time to message in millisecond (ms) as integer for debug and performance/delay analysis (default: false). only for fluent-plugin-mqtt-io >= 0.3.0
  - **recv_time_key**: An attribute of recv_time (default: "recv_time"). only for fluent-plugin-mqtt-io >= 0.3.0
  - **time_type**: Type of time format (string (default), unixtime, float) only for fluent-plugin-mqtt-io >= 0.3.0
  - **time_format**: Time format e.g. %FT%T.%N%:z (refer strftime) only for fluent-plugin-mqtt-io >= 0.3.0
- **initial_interval**: An initial value of retry interval (s) (default 1)
- **retry_inc_ratio**: An increase ratio of retry interval per connection failure (default 2 (double)). It may be better to set the value to 1 in a mobile environment for eager reconnection.
- **max_retry_interval**: Maximum value of retry interval (default 300) only for fluent-plugin-mqtt-io >= 0.3.0

Input Plugin supports @label directive.

### Output Plugin (Fluent::MqttOutput, Fluent::MqttBufferedOutput)

Output Plugin can be used via match directive.

For fluent-plugin-mqtt-io <= 0.2.3

```

<match topic.**>
  type mqtt
  host 127.0.0.1
  port 1883
</match>

```

For fluent-plugin-mqtt-io >= v0.3.0

```

<match topic.**>
  @type mqtt
  host 127.0.0.1
  port 1883
  <format>
    @type json
    add_newline false
  </format>
</match>

```

The options are basically the same as Input Plugin except for "format" and "bulk_trans" (only for Input). Additional options for Output Plugin are the following.

- **time_key**: An attribute name used for timestamp field genarated from fluentd time field. Default is nil (omitted).
  If this option is omitted, timestamp field is not appended to the output record.
- **time_format**: Output format of timestamp field. Default is ISO8601. You can specify your own format by using TimeParser.
- **topic_rewrite_pattern**: Regexp pattern to extract replacement words from received topic or tag name
- **topic_rewrite_replacement**: Topic name used for the publish using extracted pattern
- **send_time**: Add send time to message in millisecond (ms) as integer for debug and performance/delay analysis. only for fluent-plugin-mqtt-io <= 0.2.3
- **send_time_key**: An attribute of send_time. only for fluent-plugin-mqtt-io <= 0.2.3
- **monitor**: monitor section. only for fluent-plugin-mqtt-io >= 0.3.0
  - **send_time**: Add send time to message in millisecond (ms) as integer for debug and performance/delay analysis. only for fluent-plugin-mqtt-io >= 0.3.0
  - **send_time_key**: An attribute of send_time. only for fluent-plugin-mqtt-io >= 0.3.0
  - **time_type**: Type of time format (string (default), unixtime, float) only for fluent-plugin-mqtt-io >= 0.3.0
  - **time_format**: Time format e.g. %FT%T.%N%:z (refer strftime) only for fluent-plugin-mqtt-io >= 0.3.0

If you use different source, e.g. the other MQTT broker, log file and so on, there is no need to specifie topic rewriting. Skip the following descriptions.

The topic name or tag name, e.g. "topic", received from an event can not be published without modification because if MQTT input plugin connecting to the identical MQTT broker is used as a source, the same message will become an input repeatedly. In order to support data conversion in single MQTT domain, simple topic rewriting should be supported. Since topic is rewritten using #gsub method, 'pattern' and 'replacement' are the same as #gsub arguments.

For fluent-plugin-mqtt-io <= v0.2.3

```

<match topic.**>
  type mqtt
  host 127.0.0.1
  port 1883
  topic_rewrite_pattern '^([\w\/]+)$'
  topic_rewrite_replacement '\1/rewritten'
</match>

```

For fluent-plugin-mqtt-io >= v0.3.0

```

<match topic.**>
  @type mqtt
  host 127.0.0.1
  port 1883
  <format>
    @type json
    add_newline false
  </format>
  topic_rewrite_pattern '^([\w\/]+)$'
  topic_rewrite_replacement '\1/rewritten'
</match>

```

You can also use mqtt_buf type which is implemented as Fluent::MqttBufferedOutput.

```

<match topic.**>
  type mqtt_buf
  host 127.0.0.1
  port 1883
  topic_rewrite_pattern '^([\w\/]+)$'
  topic_rewrite_replacement '\1/rewritten'
  # You can specify Buffer Plugin options
  buffer_type memory
  flush_interval 1s
</match>

```

For fluent-plugin-mqtt-io >= v0.3.0

```

<match topic.**>
  @type mqtt
  host 127.0.0.1
  port 1883
  <format>
    @type json
  </format>
  topic_rewrite_pattern '^([\w\/]+)$'
  topic_rewrite_replacement '\1/rewritten'
  # You can specify Buffer Plugin options
  <buffer>
    buffer_type memory
    flush_interval 1s
  </buffer>
</match>

```

When a plugin implemented as Fluent::BufferedOutput, fluentd stores the received messages into buffers (Fluent::MemoryBuffer is used as default) separated by tag names. Writer Threads emit those stored messages periodically in the specified interval. In MqttBufferedOutput, the stored messages are concatenated with 'bulk_trans_sep' (default: "\t"). This function reduces the number of messages sent via MQTT if data producing interval of sensors are smaller than publish interval (flush_interval). The concatinated messages can be properly handled by Fluent::MqttInput plugin by specifying 'bulk_trans' option.

## Use case examples

### Sensor data collection (not for Libelium now)

*additional description (2015-12-25)*: Thanks to the updates by Libelium, Meshlium farmware now supports flexible output format configuration including JSON. As a result, data conversion by XmlParser is not required for this use case. The following description is kept just for sharing know-how of data conversion in fluentd.

There are many kinds of commercial sensor products on the market, e.g. [Libelium](http://www.libelium.com/). Major sensor products support MQTT to upload sensor data. The following example shows how to store uploaded Libelium sensor data into ElasticSearch.

![Libelium sensor data collection](https://github.com/toyokazu/fluent-plugin-mqtt-io/blob/master/images/libelium_sensor_data_collection.png "Libelium sensor data")

As described in the figure, fluent-plugin-mqtt-io, fluent-plugin-xml-parser and fluent-plugin-elasticsearch are used. The following is an example configuration.

```
<source>
  type mqtt
  host 192.168.1.100
  port 1883
  topic 'Libelium/+/+'
  format xml
  time_xpath '["cap:alert/cap:info/cap:onset", "text"]'
  time_key '@timestamp'
  attr_xpaths '[["cap:alert/cap:info/cap:parameter/cap:valueName", "text"]]'
  value_xpaths '[["cap:alert/cap:info/cap:parameter/cap:value", "text"]]'
  @label @MQTT_OUT
</source>

<label @MQTT_OUT>
  <match **>
    <store>
      type elasticsearch
      host localhost
      port 9200
      index_name libelium
      type_name smartcity
      include_tag_key true
      tag_key sensor_id
      logstash_format false
    </store>
  </match>
</label>

```

The following mapping is assumed to be created at ElasticSearch.

```

curl -XPUT 'http://localhost:9200/libelium/_mapping/smartcity' -d '
  {"smartcity":
    {"properties":
      {
        "sensor_id":{"type": "string"},
        "DUST":{"type": "float"},
        "MCP":{"type": "float"},
        "HUMA":{"type": "float"},
        "TCA":{"type": "float"},
        "BAT":{"type": "float"},
        "@timestamp":{"type":"date","format":"dateOptionalTime"}
      }
    }
  }'

```

### MQTT message conversion

Sometimes, MQTT message conversion must be done in the network because the processing entities does not have the conversion function. In that case, the configuration similar to the above example can be used. The difference resides output configuration. In this example, since the same MQTT broker is used to upload converted data, topic rewriting function is used for separating messages before and after conversion.

![MQTT message conversion](https://github.com/toyokazu/fluent-plugin-mqtt-io/blob/master/images/mqtt_message_conversion.png "MQTT message conversion")

```
<source>
  type mqtt
  host 192.168.1.100
  port 1883
  topic 'Libelium/+/+'
  format xml
  time_xpath '["cap:alert/cap:info/cap:onset", "text"]'
  time_key '@timestamp'
  attr_xpaths '[["cap:alert/cap:info/cap:parameter/cap:valueName", "text"]]'
  value_xpaths '[["cap:alert/cap:info/cap:parameter/cap:value", "text"]]'
  @label @MQTT_OUT
</source>

<label @MQTT_OUT>
  <match **>
    type mqtt
    host 192.168.1.100
    port 1883
    topic_rewrite_pattern '^([\w\/]+)$'
    topic_rewrite_replacement '\1/rewritten'
  </match>
</label>

```


### Sensor data uploads from tiny computers, e.g. Raspberry Pi, Edison, etc

MQTT output plugin can be used as the following. If you have tiny computers like Raspberry Pi equipped with sensors and their data are outputted as files, you can use fluent-plugin-mqtt-io for uploading those data.

![Sensor data uploads from tiny computers](https://github.com/toyokazu/fluent-plugin-mqtt-io/blob/master/images/sensor_data_uploads_from_tiny_computers.png "Sensor data uploads from tiny computers")


## Contributing

1. Fork it ( http://github.com/toyokazu/fluent-plugin-mqtt-io/fork )
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request


## License

The gem is available as open source under the terms of the [Apache License Version 2.0](https://www.apache.org/licenses/LICENSE-2.0).
