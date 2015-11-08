# Fluent::Plugin::Mqtt::IO

Fluent plugin for MQTT Input/Output
Mqtt::IO Plugin is deeply inspired by Fluent::Plugin::Mqtt.

https://github.com/yuuna/fluent-plugin-mqtt

Mqtt::IO plugin focus on federating components, e.g. sensors, messaging platform and databases.

## Installation

Add this line to your application's Gemfile:

    gem 'fluent-plugin-mqtt-io'

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install fluent-plugin-mqtt-io


## Usage

fluent-plugin-mqtt provides Input and Output Plugins for MQTT.

Input Plugin can be used via source directive in the configuration.

```

<source>
  type mqtt
  bind 127.0.0.1
  port 1883
</source>

```

The default MQTT topic is "#". Configurable options are the following:

- bind: IP address of MQTT broker
- port: Port number of MQTT broker
- topic: Topic name to be subscribed
- username: User name for authentication
- password: Password for authentication
- ssl: set true if you want to use SSL/TLS. If set to true, the following parameter must be provided
  - ca_file: CA certificate file path
  - key_file: private key file path
  - cert_file: certificate file path
- format: Input parser can be chosen, e.g. json, xml
  - in order to use xml format, you need to install [fluent-plugin-xml-parser](https://github.com/toyokazu/fluent-plugin-xml-parser).

Input Plugin supports @label directive.

Output Plugin can be used via match directive.

```

<match topic.**>
  type mqtt
  bind 127.0.0.1
  port 1883
</match>

```

The options are basically the same as Input Plugin. The difference is the following.

- time_key: An attribute name used for timestamp field genarated from fluentd time field. Default is nil (omitted).
  If this option is omitted, timestamp field is not appended to the output record.
- time_format: Output format of timestamp field. Default is ISO8601. You can specify your own format by using TimeParser.
- topic_rewrite_pattern: Regexp pattern to extract replacement words from received topic or tag name
- topic_rewrite_replacement: Topic name used for the publish using extracted pattern

If you use different source, e.g. the other MQTT broker, log file and so on, there is no need to specifie topic rewriting. Skip the following descriptions.

The topic name or tag name, e.g. "topic", received from an event can not be published without modification because if MQTT input plugin connecting to the identical MQTT broker is used as a source, the same message will become an input repeatedly. In order to support data conversion in single MQTT domain, simple topic rewriting should be supported. Since topic is rewritten using #gsub method, 'pattern' and 'replacement' are the same as #gsub arguments.


```

<match topic.**>
  type mqtt
  bind 127.0.0.1
  port 1883
  topic_rewrite_pattern '^([\w\/]+)$'
  topic_rewrite_replacement '\1/rewritten'
</match>

```

You can also use mqtt_buf type which is implemented as BufferedOutput.

```

<match topic.**>
  type mqtt_buf
  bind 127.0.0.1
  port 1883
  topic_rewrite_pattern '^([\w\/]+)$'
  topic_rewrite_replacement '\1/rewritten'
  # You can specify Buffer Plugin options
  buffer_type memory
  flush_interval 1s
</match>

```


## Use case examples

### Libelium sensor data collection

There are many kinds of commercial sensor products on the market, e.g. [Libelium](http://www.libelium.com/). Major sensor products support MQTT to upload sensor data. The following example shows how to store uploaded Libelium sensor data into ElasticSearch.

![Libelium sensor data collection](https://github.com/toyokazu/fluent-plugin-mqtt-io/images/libelium_sensor_data_collection.png "Libelium sensor data")

In this example, as described in the figure, fluent-plugin-mqtt-io, fluent-plugin-xml-parser and fluent-plugin-elasticsearch. The following is an example configuration.

```
<source>
  type mqtt
  bind 192.168.1.100
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
    type copy
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
</label>

```

In this example, the following mapping is assumed to be created at ElasticSearch.

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

![MQTT message conversion](https://github.com/toyokazu/fluent-plugin-mqtt-io/images/mqtt_message_conversion.png "MQTT message conversion")

```
<source>
  type mqtt
  bind 192.168.1.100
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
    bind 192.168.1.100
    port 1883
    topic_rewrite_pattern '^([\w\/]+)$'
    topic_rewrite_replacement '\1/rewritten'
  </match>
</label>

```


### Sensor data uploads from tiny computers, e.g. Raspberry Pi, Edison, etc

MQTT output plugin can be used as the following. If you have tiny computers like Raspberry Pi equipped with sensors and their data are outputted as files, you can use fluent-plugin-mqtt-io for uploading those data.

![Sensor data uploads from tiny computers](https://github.com/toyokazu/fluent-plugin-mqtt-io/images/sensor_data_uploads_from_tiny_computers.png "Sensor data uploads from tiny computers")


## Contributing

1. Fork it ( http://github.com/toyokazu/fluent-plugin-mqtt-io/fork )
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request


## License

The gem is available as open source under the terms of the [MIT License](http://opensource.org/licenses/MIT).

