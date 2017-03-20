require 'fluent/plugin/input'
require 'fluent/event'
require 'fluent/time'
require 'mqtt'

module Fluent::Plugin
  class MqttInput < Input
    Fluent::Plugin.register_input('mqtt', self)

    helpers :thread

    MQTT_PORT = 1883

    desc 'The port to connect to.'
    config_param :port, :integer, :default => MQTT_PORT
    desc 'The address to connect to.'
    config_param :host, :string, :default => '127.0.0.1'
    desc 'The topic to subscribe.'
    config_param :topic, :string, :default => '#'
    desc 'The format to receive.'
    config_param :format, :string, :default => 'json'
    desc 'Specify keep alive interval.'
    config_param :keep_alive, :integer, :default => 15
    desc 'Specify initial interval for reconnection.'
    config_param :initial_interval, :integer, :default => 1
    desc 'Specify increasing ratio of reconnection interval.'
    config_param :retry_inc_ratio, :integer, :default => 2

    # bulk_trans is deprecated
    # multiple entries must be inputted as an Array
    #config_param :bulk_trans, :bool, :default => true
    #config_param :bulk_trans_sep, :string, :default => "\t"

    config_section :security, required: false, multi: false do
      ### User based authentication
      desc 'The username for authentication'
      config_param :username, :string, :default => nil
      desc 'The password for authentication'
      config_param :password, :string, :default => nil
      desc 'Use TLS or not.'
      config_param :use_tls, :bool, :default => nil
      config_section :tls, required: false, multi: true do
        desc 'Specify TLS ca file.'
        config_param :ca_file, :string, :default => nil
        desc 'Specify TLS key file.'
        config_param :key_file, :string, :default => nil
        desc 'Specify TLS cert file.'
        config_param :cert_file, :string, :default => nil
      end
    end

    config_section :monitor, required: false, multi: false do
      desc 'Record received time into message or not.'
      config_param :recv_time, :bool, :default => false
      desc 'Specify the attribute name of received time.'
      config_param :recv_time_key, :string, :default => "recv_time"
    end

    # Define `router` method of v0.12 to support v0.10 or earlier
    unless method_defined?(:router)
      define_method("router") { Fluent::Engine }
    end

    def configure(conf)
      super
      configure_parser(conf)
      init_retry_interval
    end

    def configure_parser(conf)
      @parser = Plugin.new_parser(conf['format'])
      @parser.configure(conf)
    end

    def init_retry_interval
      @retry_interval = @initial_interval
    end

    def increment_retry_interval
      @retry_interval = @retry_interval * @retry_inc_ratio
    end

    def sleep_retry_interval(e, message)
      $log.error "#{message},#{e.class},#{e.message}"
      $log.error "Retry in #{@retry_interval} sec"
      sleep @retry_interval
      increment_retry_interval
    end

    def start
      $log.debug "start mqtt #{@host}"
      opts = {
        host: @host,
        port: @port,
        username: @security.username,
        password: @security.password,
        keep_alive: @keep_alive
      }
      if @security.use_tls
        opts[:ssl] = @security.use_tls
        opts[:ca_file] = @security.tls.ca_file
        opts[:cert_file] = @security.tls.cert_file
        opts[:key_file] = @security.tls.key_file
      end

      # In order to handle Exception raised from reading Thread
      # in MQTT::Client caused by network disconnection (during read_byte),
      # @connect_thread generates connection.
      @client = MQTT::Client.new(opts)
      @get_thread = nil
      @connect_thread = thread_create(:mqtt_input_connect, &method(:connect_loop))
    end

    def connect_loop
      while (true)
        begin
          @get_thread.kill if !@get_thread.nil? && @get_thread.alive?
          @client.disconnect if @client.connected?
          @client.connect
          @client.subscribe(@topic)
          @get_thread = Thread.new do
            @client.get do |topic, message|
              emit(topic, message)
            end
          end
          init_retry_interval
          sleep
        rescue MQTT::ProtocolException => e
          sleep_retry_interval(e, "Protocol error occurs.")
          next
        rescue Timeout::Error => e
          sleep_retry_interval(e, "Timeout error occurs.")
          next
        rescue SystemCallError => e
          sleep_retry_interval(e, "System call error occurs.")
          next
        rescue StandardError=> e
          sleep_retry_interval(e, "The other error occurs.")
          next
        end
      end
    end

    def add_recv_time(record)
      if @recv_time
        # recv_time is recorded in ms
        record.merge({@recv_time_key => Fluent::EventTime.now})
      else
        record
      end
    end

    def parse(message)
      @parser.parse(message) do |time, record|
        if time.nil?
          $log.debug "Since time_key field is nil, Time.now is used."
          time = Fluent::EventTime.now
        end
        $log.debug "#{topic}, #{time}, #{add_recv_time(record)}"
        return [time, add_recv_time(record)]
      end
    end

    def emit(topic, message)
      begin
        topic.gsub!("/","\.")
        time, record = parse(message)
        if record.is_a?(Array)
          mes = Fluent::MultiEventStream.new
          record.each do |single_record|
            single_time = single_record.delete("time") || time
            mes.add(single_time, single_record)
          end
          router.emit_stream(tag, mes)
        else
          router.emit(tag, time, record)
        end
      rescue Exception => e
        $log.error :error => e.to_s
        $log.debug_backtrace(e.backtrace)
      end
    end

    def shutdown
      @get_thread.kill
      @client.disconnect
    end
  end
end
