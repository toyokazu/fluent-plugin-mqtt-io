require 'fluent/plugin/input'
require 'fluent/event'
require 'fluent/time'
require 'mqtt'

module Fluent::Plugin
  class MqttOutput < Output
    Fluent::Plugin.register_output('mqtt', self)

    helpers :formatter, :inject, :compat_parameters

    MQTT_PORT = 1883

    desc 'The port to connect to.'
    config_param :port, :integer, :default => MQTT_PORT
    desc 'The address to connect to.'
    config_param :host, :string, :default => '127.0.0.1'
    desc 'MQTT keep alive interval.'
    config_param :keep_alive, :integer, :default => 15
    config_param :topic_rewrite_pattern, :string, :default => nil
    config_param :topic_rewrite_replacement, :string, :default => nil
    config_param :initial_interval, :integer, :default => 1
    config_param :retry_inc_ratio, :integer, :default => 2

    config_section :security, required: false, multi: false do
      ### User based authentication
      desc 'The username for authentication'
      config_param :username, :string, :default => nil
      desc 'The password for authentication'
      config_param :password, :string, :default => nil
      desc 'Use TLS or not.'
      config_param :use_tls, :bool, :default => nil
      config_section :tls, required: false, multi: true do
        desc 'TLS ca file.'
        config_param :ca_file, :string, :default => nil
        desc 'TLS key file.'
        config_param :key_file, :string, :default => nil
        desc 'TLS cert file.'
        config_param :cert_file, :string, :default => nil
      end
    end

    config_section :monitor, required: false, multi: false do
      config_param :send_time, :bool, :default => false
      config_param :send_time_key, :string, :default => "send_time"
    end

    # This method is called before starting.
    # 'conf' is a Hash that includes configuration parameters.
    # If the configuration is invalid, raise Fluent::ConfigError.
    def configure(conf)
      compat_parameters_convert(conf, :formatter, :buffer, :inject, default_chunk_key: "time")
      @formatter = formatter_create(conf: conf)
      init_retry_interval
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

    def rewrite_tag(tag)
      if @topic_rewrite_pattern.nil?
        tag.gsub("\.", "/")
      else
        tag.gsub("\.", "/").gsub(Regexp.new(@topic_rewrite_pattern), @topic_rewrite_replacement)
      end
    end

    # This method is called when starting.
    # Open sockets or files here.
    def start
      super

      $log.debug "start mqtt #{@host}"
      opts = {
        host: @host,
        port: @port,
        username: @security.username,
        password: @secuiryt.password,
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
      @client_mutex = Mutex.new
      @client = MQTT::Client.new(opts)
      mqtt_error_handler do
        @client.disconnect if @client.connected?
        @client.connect
        init_retry_interval
      end
      connect_loop
    end

    # This method is called when shutting down.
    # Shutdown the thread and close sockets or files here.
    def shutdown
      super

      @client.disconnect
    end

    def connect_loop
      while (true)
        @client_mutex.synchronize do
          begin
            @client.disconnect if @client.connected?
            @client.connect
            init_retry_interval
            break
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
    end

    def publish_error_handler
      while(true)
        begin
          yield
        rescue MQTT::ProtocolException => e
          sleep_retry_interval(e, "Protocol error occurs.")
          connect_loop
        rescue Timeout::Error => e
          sleep_retry_interval(e, "Timeout error occurs.")
          connect_loop
        rescue SystemCallError => e
          sleep_retry_interval(e, "System call error occurs.")
          connect_loop
        rescue StandardError=> e
          sleep_retry_interval(e, "The other error occurs.")
          connect_loop
        end
      end
    end

    def publish_event_stream(tag, es)
      if es.class == OneEventStream
        es = inject_values_to_event_stream(tag, es)
        es.each do |time, record|
          $log.debug "#{rewrite_tag(tag)}, #{add_send_time(record)}"
          publish_error_handler do
            @client.publish(rewrite_tag(tag), @formatter.format(tag, time, add_send_time(record)))
          end
        end
      else
        es = inject_values_to_event_stream(tag, es)
        array = []
        es.each do |time,record|
          $log.debug "#{rewrite_tag(tag)}, #{add_send_time(record)}"
          array << add_send_time(record)
        end
        publish_error_handler do
          @client.publish(rewrite_tag(tag), @formatter.format(tag, time, array))
        end
      end
      $log.flush
    end

    def process(tag, es)
      publish_event_stream(tag, es)
    end

    def write(chunk)
      return if chunk.empty?
      tag = chunk.metadata.tag

      publish_event_stream(tag, es)
    end
  end
end
