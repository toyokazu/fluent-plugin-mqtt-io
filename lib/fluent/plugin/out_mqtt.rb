require 'fluent/plugin/output'
require 'fluent/event'
require 'fluent/time'
require 'fluent/plugin/mqtt_proxy'

module Fluent::Plugin
  class MqttOutput < Output
    include MqttProxy
    include Fluent::TimeMixin::Formatter

    Fluent::Plugin.register_output('mqtt', self)

    helpers :compat_parameters, :formatter, :inject


    desc 'Topic rewrite matching pattern.'
    config_param :topic_rewrite_pattern, :string, default: nil
    desc 'Topic rewrite replacement string.'
    config_param :topic_rewrite_replacement, :string, default: nil

    desc 'Retain option which publishing'
    config_param :retain, :bool, default: false
    desc 'QoS option which publishing'
    config_param :qos, :integer, default: 1

    config_section :format do
      desc 'The format to publish'
      config_param :@type, :string, default: 'single_value'
      desc 'Add newline'
      config_param :add_newline, :bool, default: false
    end

    config_section :monitor, required: false, multi: false do
      desc 'Recording send time for monitoring.'
      config_param :send_time, :bool, default: false
      desc 'Recording key name of send time for monitoring.'
      config_param :send_time_key, :string, default: "send_time"
      desc 'Specify time type of send_time (string, unixtime, float).'
      config_param :time_type, :string, default: 'string'
      desc 'Specify time format of send_time (e.g. %FT%T.%N%:z).'
      config_param :time_format, :string, default: nil
    end

    config_section :buffer, required: false, multi: false do
      desc 'Prefer asynchronous buffering'
      config_param :async, :bool, default: false
    end

    # This method is called before starting.
    # 'conf' is a Hash that includes configuration parameters.
    # If the configuration is invalid, raise Fluent::ConfigError.
    def configure(conf)
      super
      compat_parameters_convert(conf, :formatter, :inject, :buffer, default_chunk_key: "time")
      formatter_config = conf.elements(name: 'format').first
      @formatter = formatter_create(conf: formatter_config)
      @has_buffer_section = conf.elements(name: 'buffer').size > 0
      if !@monitor.nil?
        @send_time_formatter = time_formatter_create(
          type: @monitor.time_type.to_sym, format: @monitor.time_format
        )
      end
    end

    def rewrite_tag(tag)
      if @topic_rewrite_pattern.nil?
        tag.gsub("\.", "/")
      else
        tag.gsub("\.", "/").gsub(Regexp.new(@topic_rewrite_pattern), @topic_rewrite_replacement)
      end
    end

    def prefer_buffered_processing
      @has_buffer_section
    end

    def prefer_delayed_commit
      @has_buffer_section && @buffer_config.async
    end

    # This method is called when starting.
    # Open sockets or files here.
    def start
      super
      start_proxy
    end

    # This method is called when shutting down.
    # Shutdown the thread and close sockets or files here.
    def shutdown
      shutdown_proxy
      exit_thread
      super
    end

    def exit_thread
      @dummy_thread.exit if !@dummy_thread.nil?
    end

    def disconnect
      begin
        @client.disconnect if @client.connected?
      rescue => e
        log.error "Error in out_mqtt#disconnect,#{e.class},#{e.message}"
      end
      exit_thread
    end

    def terminate
      exit_thread
      super
    end

    def after_connection
      @dummy_thread = thread_create(:out_mqtt_dummy) do
        Thread.stop
      end
      @dummy_thread
    end

    def current_plugin_name
      :out_mqtt
    end

    def add_send_time(record)
      if !@monitor.nil? && @monitor.send_time
        # send_time is recorded in ms
        record.merge({"#{@monitor.send_time_key}": @send_time_formatter.format(Fluent::EventTime.now)})
      else
        record
      end
    end

    def publish_event_stream(tag, es)
      log.debug "publish_event_stream: #{es.class}"
      es = inject_values_to_event_stream(tag, es)
      es.each do |time, record|
        rescue_disconnection do
          publish(tag, time, record)
        end
      end
      log.flush
    end

    def process(tag, es)
      publish_event_stream(tag, es)
    end

    def format(tag, time, record)
      record = inject_values_to_record(tag, time, record)
      [tag, time, record].to_msgpack
    end

    def formatted_to_msgpack_binary
      true
    end

    def publish(tag, time, record)
      log.debug "MqttOutput::#{caller_locations(1,1)[0].label}: #{rewrite_tag(tag)}, #{time}, #{add_send_time(record)}"
      @client.publish(
        rewrite_tag(tag),
        @formatter.format(tag, time, add_send_time(record)),
        @retain,
        @qos
      )
    end

    def write(chunk)
      return if chunk.empty?
      chunk.each do |tag, time, record|
        rescue_disconnection do
          publish(tag, time, record)
        end
      end
    end

    def try_write(chunk)
      return if chunk.empty?
      rescue_disconnection do
        chunk.each do |tag, time, record|
          publish(tag, time, record)
        end
        commit_write(chunk.unique_id)
      end
    end
  end
end
