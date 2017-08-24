require 'mqtt'
module Fluent::Plugin
  module MqttProxy
    MQTT_PORT = 1883

    def self.included(base)
      base.helpers :timer, :thread

      base.desc 'The address to connect to.'
      base.config_param :host, :string, default: '127.0.0.1'
      base.desc 'The port to connect to.'
      base.config_param :port, :integer, default: MQTT_PORT
      base.desc 'Specify keep alive interval.'
      base.config_param :keep_alive, :integer, default: 15
      base.desc 'Specify initial connection retry interval.'
      base.config_param :initial_interval, :integer, default: 1
      base.desc 'Specify increasing ratio of connection retry interval.'
      base.config_param :retry_inc_ratio, :integer, default: 2
      base.desc 'Specify maximum connection retry interval.'
      base.config_param :max_retry_interval, :integer, default: 300

      base.config_section :security, required: false, multi: false do
        ### User based authentication
        base.desc 'The username for authentication'
        base.config_param :username, :string, default: nil
        base.desc 'The password for authentication'
        base.config_param :password, :string, default: nil
        base.desc 'Use TLS or not.'
        base.config_param :use_tls, :bool, default: nil
        base.config_section :tls, required: false, multi: true do
          base.desc 'Specify TLS ca file.'
          base.config_param :ca_file, :string, default: nil
          base.desc 'Specify TLS key file.'
          base.config_param :key_file, :string, default: nil
          base.desc 'Specify TLS cert file.'
          base.config_param :cert_file, :string, default: nil
        end
      end
    end

    class MqttProxyError
    end

    def current_plugin_name
      # should be implemented
    end

    def start_proxy
      log.debug "start mqtt proxy for #{current_plugin_name}"
      log.debug "start to connect mqtt broker #{@host}:#{@port}"
      opts = {
        host: @host,
        port: @port,
        keep_alive: @keep_alive
      }
      opts[:username] = @security.username if @security.respond_to?(:username)
      opts[:password] = @security.password if @security.respond_to?(:password)
      if @security.respond_to?(:use_tls) && @security.use_tls
        opts[:ssl] = @security.use_tls
        opts[:ca_file] = @security.tls.ca_file
        opts[:cert_file] = @security.tls.cert_file
        opts[:key_file] = @security.tls.key_file
      end

      init_retry_interval
      @client = MQTT::Client.new(opts)
      connect
    end

    def shutdown_proxy
      @client.disconnect
    end

    def init_retry_interval
      @retry_interval = @initial_interval
    end

    def increment_retry_interval
      return @retry_interval if @retry_interval >= @max_retry_interval
      @retry_interval = @retry_interval * @retry_inc_ratio
    end

    def retry_connect(e, message)
      log.error "#{message},#{e.class},#{e.message}"
      log.error "Retry in #{@retry_interval} sec"
      timer_execute("#{current_plugin_name}_connect".to_sym, @retry_interval, repeat: false, &method(:connect))
      increment_retry_interval
      after_disconnection
      @client.disconnect if @client.connected?
    end

    def after_disconnection
      # should be implemented
    end

    def rescue_disconnection(*block)
      begin
        yield *block
      rescue MQTT::ProtocolException => e
        # TODO:
        # Currently MQTT::ProtocolException cannot be caught during @client.get
        # and @client.publish. The reason must be investigated...
        retry_connect(e, "Protocol error occurs.")
      rescue Timeout::Error => e
        retry_connect(e, "Timeout error occurs.")
      rescue SystemCallError => e
        retry_connect(e, "System call error occurs.")
      rescue StandardError=> e
        retry_connect(e, "The other error occurs.")
      end
    end

    def after_connection
      # should be implemented
      # returns thread instance for monitor thread to wait
      # for Exception raised by MQTT I/O
    end

    def connect
      thread_create("#{current_plugin_name}_monitor".to_sym) do
        rescue_disconnection do
          @client.connect
          log.debug "connected to mqtt broker #{@host}:#{@port} for #{current_plugin_name}"
          init_retry_interval
          thread = after_connection
          thread.join
        end
      end
    end
  end
end
