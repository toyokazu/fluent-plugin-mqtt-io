module Fluent
  class MqttInput < Input
    Plugin.register_input('mqtt', self)

    config_param :port, :integer, :default => 1883
    config_param :bind, :string, :default => '127.0.0.1'
    config_param :topic, :string, :default => '#'
    config_param :format, :string, :default => 'json'
    config_param :username, :string, :default => nil
    config_param :password, :string, :default => nil
    config_param :ssl, :bool, :default => nil
    config_param :ca_file, :string, :default => nil
    config_param :key_file, :string, :default => nil
    config_param :cert_file, :string, :default => nil

    require 'mqtt'

    # Define `router` method of v0.12 to support v0.10 or earlier
    unless method_defined?(:router)
      define_method("router") { Fluent::Engine }
    end

    def configure(conf)
      super
      @bind ||= conf['bind']
      @topic ||= conf['topic']
      @port ||= conf['port']
      @username ||= conf['username']
      @password ||= conf['password']
      configure_parser(conf)
    end

    def configure_parser(conf)
      @parser = Plugin.new_parser(conf['format'])
      @parser.configure(conf)
    end

    def start
      $log.debug "start mqtt #{@bind}"
      opts = {
        host: @bind,
        port: @port,
        username: @username,
        password: @password
      }
      opts[:ssl] = @ssl if @ssl
      opts[:ca_file] = @ca_file if @ca_file
      opts[:cert_file] = @cert_file if @cert_file
      opts[:key_file] = @key_file if @key_file
      @connect = MQTT::Client.connect(opts)
      @connect.subscribe(@topic)

      @thread = Thread.new do
        @connect.get do |topic,message|
          emit topic, message
        end
      end
    end

    def emit topic, message
      begin
        topic.gsub!("/","\.")
        @parser.parse(message) {|time, record|
          if time.nil?
            $log.debug "Since time_key field is nil, Fluent::Engine.now is used."
            time = Fluent::Engine.now
          end
          $log.debug "#{topic}, #{time}, #{record}"
          router.emit(topic, time, record)
        }
      rescue Exception => e
        $log.error :error => e.to_s
        $log.debug_backtrace(e.backtrace)
      end
    end

    def shutdown
      @thread.kill
      @connect.disconnect
    end
  end
end
