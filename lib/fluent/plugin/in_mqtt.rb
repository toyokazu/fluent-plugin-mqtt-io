require 'fluent/plugin/input'
require 'fluent/event'
require 'fluent/time'
require 'fluent/plugin/mqtt_proxy'

module Fluent::Plugin
  class MqttInput < Input
    include MqttProxy

    Fluent::Plugin.register_input('mqtt', self)

    helpers :compat_parameters, :parser

    desc 'The topic to subscribe.'
    config_param :topic, :string, default: '#'
    desc 'The format to receive.'
    config_param :format, :string, default: 'json'

    # bulk_trans is deprecated
    # multiple entries must be inputted as an Array
    #config_param :bulk_trans, :bool, default: true
    #config_param :bulk_trans_sep, :string, default: "\t"

    config_section :monitor, required: false, multi: false do
      desc 'Record received time into message or not.'
      config_param :recv_time, :bool, default: false
      desc 'Specify the attribute name of received time.'
      config_param :recv_time_key, :string, default: "recv_time"
    end

    def configure(conf)
      super
      configure_parser(conf)
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

    def after_disconnection
    end

    def after_connection
      if @client.connected?
        @client.subscribe(@topic)
        @get_thread = thread_create(:in_mqtt_get) do
          @client.get do |topic, message|
            emit(topic, message)
          end
        end
      end
      @get_thread
    end

    def add_recv_time(record)
      if @recv_time
        # recv_time is recorded in ms
        record.merge({"#{@recv_time_key}": Fluent::EventTime.now})
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
        log.debug "#{topic}, #{time}, #{add_recv_time(record)}"
        return [time, add_recv_time(record)]
      end
    end

    def emit(topic, message)
      begin
        tag = topic.gsub("/","\.")
        time, record = parse(message)
        if record.is_a?(Array)
          mes = Fluent::MultiEventStream.new
          record.each do |single_record|
            mes.add(@parser.parse_time(single_record), single_record)
          end
          router.emit_stream(tag, mes)
        else
          router.emit(tag, time, record)
        end
      rescue Exception => e
        log.error error: e.to_s
        log.debug_backtrace(e.backtrace)
      end
    end
  end
end
