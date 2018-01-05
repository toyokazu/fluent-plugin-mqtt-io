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
      if es.class == Fluent::OneEventStream
        es = inject_values_to_event_stream(tag, es)
        es.each do |time, record|
          log.debug "MqttOutput#publish_event_stream: #{rewrite_tag(tag)}, #{time}, #{add_send_time(record)}"
          rescue_disconnection do
            @client.publish(rewrite_tag(tag), @formatter.format(tag, time, add_send_time(record)))
          end
        end
      else
        es = inject_values_to_event_stream(tag, es)
        array = []
        es.each do |time, record|
          log.debug "MqttOutput#publish_event_stream: #{rewrite_tag(tag)}, #{time}, #{add_send_time(record)}"
          array << add_send_time(record)
        end
        rescue_disconnection do
          @client.publish(rewrite_tag(tag), @formatter.format(tag, Fluent::EventTime.now, array))
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

    def write(chunk)
      return if chunk.empty?
      chunk.each do |tag, time, record|
        rescue_disconnection do
          log.debug "MqttOutput#write: #{rewrite_tag(rewrite_tag(tag))}, #{time}, #{add_send_time(record)}"
          @client.publish(rewrite_tag(tag), @formatter.format(tag, time, add_send_time(record)))
        end
      end
    end
  end
end
