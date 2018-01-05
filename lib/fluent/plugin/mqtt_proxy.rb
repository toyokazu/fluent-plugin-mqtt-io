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
      base.desc 'Client ID of MQTT Connection'
      base.config_param :client_id, :string, default: nil
      base.desc 'Specify keep alive interval.'
      base.config_param :keep_alive, :integer, default: 15

      base.config_section :security, required: false, multi: false do
        ### User based authentication
        desc 'The username for authentication'
        config_param :username, :string, default: nil
        desc 'The password for authentication'
        config_param :password, :string, default: nil
        desc 'Use TLS or not.'
        config_param :use_tls, :bool, default: nil
        config_section :tls, required: false, multi: false do
          desc 'Specify TLS ca file.'
          config_param :ca_file, :string, default: nil
          desc 'Specify TLS key file.'
          config_param :key_file, :string, default: nil
          desc 'Specify TLS cert file.'
          config_param :cert_file, :string, default: nil
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
        client_id: @client_id,
        keep_alive: @keep_alive
      }
      opts[:username] = @security.username if @security.to_h.has_key?(:username)
      opts[:password] = @security.password if @security.to_h.has_key?(:password)
      if @security.to_h.has_key?(:use_tls) && @security.use_tls
        opts[:ssl] = @security.use_tls
        opts[:ca_file] = @security.tls.ca_file
        opts[:cert_file] = @security.tls.cert_file
        opts[:key_file] = @security.tls.key_file
      end

      @client = MQTT::Client.new(opts)
      @client.connect
    end

    def shutdown_proxy
      @client.disconnect
    end
  end
end
