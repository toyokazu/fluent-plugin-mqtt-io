require 'fluent/plugin/input'
require 'fluent/event'
require 'fluent/time'
require 'fluent/plugin/mqtt_proxy'

module Fluent::Plugin
  class MqttInput < Input
    include MqttProxy
    include Fluent::TimeMixin::Formatter

    Fluent::Plugin.register_input('mqtt', self)

    helpers :compat_parameters, :parser

    desc 'The topic to subscribe.'
    config_param :topic, :string, default: '#'

    desc 'Topic with qos to subscribe in array format ["a/b",1]'
    config_param :topic_with_qos, :array, default: nil

    config_section :parse do
      desc 'The format to receive.'
      config_param :@type, :string, default: 'none'
    end

    config_section :monitor, required: false, multi: false do
      desc 'Record received time into message or not.'
      config_param :recv_time, :bool, default: false
      desc 'Specify the attribute name of received time.'
      config_param :recv_time_key, :string, default: 'recv_time'
      desc 'Specify time type of recv_time (string, unixtime, float).'
      config_param :time_type, :string, default: 'string'
      desc 'Specify time format of recv_time (e.g. %FT%T.%N%:z).'
      config_param :time_format, :string, default: nil
    end

    def configure(conf)
      super
      configure_parser(conf)
      if !@monitor.nil?
        @recv_time_formatter = time_formatter_create(
          type: @monitor.time_type.to_sym, format: @monitor.time_format
        )
      end
    end

    def configure_parser(conf)
      compat_parameters_convert(conf, :parser)
      parser_config = conf.elements('parse').first
      @parser = parser_create(conf: parser_config)
    end

    def start
      super
      start_proxy
    end

    def shutdown
      shutdown_proxy
      super
    end

    def current_plugin_name
      :in_mqtt
    end

    def exit_thread
      @get_thread.exit if !@get_thread.nil?
    end

    def disconnect
      begin
        @client.disconnect if @client.connected?
      rescue => e
        log.error "Error in in_mqtt#disconnect,#{e.class},#{e.message}"
      end
      exit_thread
    end

    def terminate
      exit_thread
      super
    end

    def after_connection
      if @client.connected?
        if @topic_with_qos.nil?
          @client.subscribe(@topic)
        else
          @client.subscribe(@topic_with_qos)
        end

        @get_thread = thread_create(:in_mqtt_get) do
          @client.get do |topic, message|
            emit(topic, message)
          end
        end
      end
      @get_thread
    end

    def add_recv_time(record)
      if !@monitor.nil? && @monitor.recv_time
        # recv_time is recorded in ms
        record.merge({"#{@monitor.recv_time_key}": @recv_time_formatter.format(Fluent::EventTime.now)})
      else
        record
      end
    end

    def parse(message)
      @parser.parse(message) do |time, record|
        if time.nil?
          log.debug "Since time_key field is nil, Fluent::EventTime.now is used."
          time = Fluent::EventTime.now
        end
        return [time, record]
      end
    end

    def emit(topic, message)
      begin
        tag = topic.gsub("/","\.")
        time, record = parse(message)
        if record.is_a?(Array)
          mes = Fluent::MultiEventStream.new
          record.each do |single_record|
            log.debug "MqttInput#emit: #{tag}, #{time}, #{add_recv_time(single_record)}"
            mes.add(@parser.parse_time(single_record), add_recv_time(single_record))
          end
          router.emit_stream(tag, mes)
        else
          log.debug "MqttInput#emit: #{tag}, #{time}, #{add_recv_time(record)}"
          router.emit(tag, time, add_recv_time(record))
        end
      rescue Exception => e
        log.error error: e.to_s
        log.debug_backtrace(e.backtrace)
      end
    end
  end
end
