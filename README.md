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
- **parser**: Parser plugin can be specified ![Parser Plugin](https://docs.fluentd.org/v1.0/articles/parser-plugin-overview)
- **topic**: Topic name to be subscribed
- **security**
  - **username**: User name for authentication
  - **password**: Password for authentication
  - **use_tls**: set true if you want to use SSL/TLS. If set to true, the following parameter must be provided
    - **ca_file**: CA certificate file path
    - **key_file**: private key file path
    - **cert_file**: certificate file path
- **keep_alive**: An interval of sending keep alive packet (default 15 sec)
- **monitor**: monitor section. only for fluent-plugin-mqtt-io
  - **recv_time**: Add receive time to message in millisecond (ms) as integer for debug and performance/delay analysis (default: false). only for fluent-plugin-mqtt-io
  - **recv_time_key**: An attribute of recv_time (default: "recv_time"). only for fluent-plugin-mqtt-io
  - **time_type**: Type of time format (string (default), unixtime, float) only for fluent-plugin-mqtt-io
  - **time_format**: Time format e.g. %FT%T.%N%:z (refer strftime) only for fluent-plugin-mqtt-io
- **initial_interval**: An initial value of retry interval (s) (default 1)
- **retry_inc_ratio**: An increase ratio of retry interval per connection failure (default 2 (double)). It may be better to set the value to 1 in a mobile environment for eager reconnection.
- **max_retry_interval**: Maximum value of retry interval (default 300)

Input Plugin supports @label directive.

### Output Plugin (Fluent::MqttOutput, Fluent::MqttBufferedOutput)

Output Plugin can be used via match directive.

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

The options are basically the same as Input Plugin except for "parser (for Input)/format (for Output)". Additional options for Output Plugin are the following.

- **qos**: Quality of Service (QoS) for message publishing, 0 or 1 is valid. 2 is not supported by mqtt client. Default is 1.
- **retain**: If set true the broker will keep the message even after sending it to all current subscribers. Default is false
- **time_key**: An attribute name used for timestamp field genarated from fluentd time field. Default is nil (omitted).
  If this option is omitted, timestamp field is not appended to the output record.
- **topic_rewrite_pattern**: Regexp pattern to extract replacement words from received topic or tag name
- **topic_rewrite_replacement**: Topic name used for the publish using extracted pattern
- **format**: Formatter plugin can be specified. ![Formatter Plugin](https://docs.fluentd.org/v1.0/articles/formatter-plugin-overview)
- **monitor**: monitor section. only for fluent-plugin-mqtt-io
  - **send_time**: Add send time to message in millisecond (ms) as integer for debug and performance/delay analysis. only for fluent-plugin-mqtt-io
  - **send_time_key**: An attribute of send_time. only for fluent-plugin-mqtt-io
  - **time_type**: Type of time format (string (default), unixtime, float) only for fluent-plugin-mqtt-io
  - **time_format**: Time format e.g. %FT%T.%N%:z (refer strftime) only for fluent-plugin-mqtt-io
- **buffer**: synchronous/asynchronous buffer is supported. Refer ![Buffer section configurations](https://docs.fluentd.org/v1.0/articles/buffer-section) for detailed configuration.
  - **async**: Enable asynchronous buffer transfer. Default is false.

If you use different source, e.g. the other MQTT broker, log file and so on, there is no need to specifie topic rewriting. Skip the following descriptions.

The topic name or tag name, e.g. "topic", received from an event can not be published without modification because if MQTT input plugin connecting to the identical MQTT broker is used as a source, the same message will become an input repeatedly. In order to support data conversion in single MQTT domain, simple topic rewriting should be supported. Since topic is rewritten using #gsub method, 'pattern' and 'replacement' are the same as #gsub arguments.


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


## Contributing

1. Fork it ( http://github.com/toyokazu/fluent-plugin-mqtt-io/fork )
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request


## License

The gem is available as open source under the terms of the [Apache License Version 2.0](https://www.apache.org/licenses/LICENSE-2.0).
