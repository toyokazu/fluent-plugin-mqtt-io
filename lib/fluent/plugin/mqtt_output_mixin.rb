module Fluent
  module MqttOutputMixin
    # config_param defines a parameter. You can refer a parameter via @path instance variable
    # Without :default, a parameter is required.
    
    def self.included(base)
      base.config_param :port, :integer, :default => 1883
      base.config_param :bind, :string, :default => '127.0.0.1'
      base.config_param :username, :string, :default => nil
      base.config_param :password, :string, :default => nil
      base.config_param :ssl, :bool, :default => nil
      base.config_param :ca_file, :string, :default => nil
      base.config_param :key_file, :string, :default => nil
      base.config_param :cert_file, :string, :default => nil
      base.config_param :time_key, :string, :default => nil
      base.config_param :time_format, :string, :default => nil
      base.config_param :topic_rewrite_pattern, :string, :default => nil
      base.config_param :topic_rewrite_replacement, :string, :default => nil
    end

    require 'mqtt'

    # This method is called before starting.
    # 'conf' is a Hash that includes configuration parameters.
    # If the configuration is invalid, raise Fluent::ConfigError.
    def configure(conf)
      super

      # You can also refer raw parameter via conf[name].
      @bind ||= conf['bind']
      @port ||= conf['port']
      @username ||= conf['username']
      @password ||= conf['password']
      @time_key ||= conf['time_key']
      @time_format ||= conf['time_format']
      @topic_rewrite_pattern ||= conf['topic_rewrite_pattern']
      @topic_rewrite_replacement ||= conf['topic_rewrite_replacement']
    end

    # This method is called when starting.
    # Open sockets or files here.
    def start
      super

      $log.debug "start mqtt #{@bind}"
      opts = {
        host: @bind,
        port: @port,
        username: @username,
        password: @password
      }
      opts[:ssl] = @ssl if @ssl
      opts[:ca_file] = @ca if @ca
      opts[:cert_file] = @crt if @crt
      opts[:key_file] = @key if @key
      @connect = MQTT::Client.connect(opts)
    end

    # This method is called when shutting down.
    # Shutdown the thread and close sockets or files here.
    def shutdown
      super

      @connect.disconnect
    end

    def format_time(time)
      if @time_format.nil?
        Time.at(time).iso8601
      else
        Time.at(time).strftime(@time_format)
      end
    end

    def timestamp_hash(time)
      if @time_key.nil?
        {}
      else
        {@time_key => format_time(time)}
      end
    end

    def rewrite_tag(tag)
      if @topic_rewrite_pattern.nil?
        tag.gsub("\.", "/")
      else
        tag.gsub("\.", "/").gsub(Regexp.new(@topic_rewrite_pattern), @topic_rewrite_replacement)
      end
    end

    def json_parse message
      begin
        y = Yajl::Parser.new
        y.parse(message)
      rescue
        $log.error "JSON parse error", :error => $!.to_s, :error_class => $!.class.to_s
        $log.warn_backtrace $!.backtrace         
      end
    end
  end
end
