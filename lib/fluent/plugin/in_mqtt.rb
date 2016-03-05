module Fluent
  class MqttInput < Input
    Plugin.register_input('mqtt', self)

    config_param :port, :integer, :default => 1883
    config_param :host, :string, :default => '127.0.0.1'
    config_param :topic, :string, :default => '#'
    config_param :format, :string, :default => 'json'
    config_param :bulk_trans, :bool, :default => true
    config_param :bulk_trans_sep, :string, :default => "\t"
    config_param :username, :string, :default => nil
    config_param :password, :string, :default => nil
    config_param :keep_alive, :integer, :default => 15
    config_param :ssl, :bool, :default => nil
    config_param :ca_file, :string, :default => nil
    config_param :key_file, :string, :default => nil
    config_param :cert_file, :string, :default => nil
    config_param :recv_time, :bool, :default => false
    config_param :recv_time_key, :string, :default => "recv_time"
    config_param :initial_interval, :integer, :default => 1
    config_param :retry_inc_ratio, :integer, :default => 2

    require 'mqtt'

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
        username: @username,
        password: @password,
        keep_alive: @keep_alive
      }
      opts[:ssl] = @ssl if @ssl
      opts[:ca_file] = @ca_file if @ca_file
      opts[:cert_file] = @cert_file if @cert_file
      opts[:key_file] = @key_file if @key_file

      # In order to handle Exception raised from reading Thread
      # in MQTT::Client caused by network disconnection (during read_byte),
      # @connect_thread generates connection.
      @client = MQTT::Client.new(opts)
      @connect_thread = Thread.new do
        while (true)
          begin
            @client.disconnect if @client.connected?
            @client.connect
            @client.subscribe(@topic)
            @get_thread.kill if !@get_thread.nil? && @get_thread.alive?
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
    end

    def add_recv_time(record)
      if @recv_time
        # recv_time is recorded in ms
        record.merge({@recv_time_key => Time.now.instance_eval { self.to_i * 1000 + (usec/1000) }})
      else
        record
      end
    end

    def parse(message)
      @parser.parse(message) do |time, record|
        if time.nil?
          $log.debug "Since time_key field is nil, Fluent::Engine.now is used."
          time = Fluent::Engine.now
        end
        $log.debug "#{topic}, #{time}, #{add_recv_time(record)}"
        return [time, add_recv_time(record)]
      end
    end

    def emit(topic, message)
      begin
        topic.gsub!("/","\.")
        if @bulk_trans
          message.split(@bulk_trans_sep).each do |m|
            router.emit(topic, *parse(m))
          end
        else
          router.emit(topic, *parse(message))
        end
      rescue Exception => e
        $log.error :error => e.to_s
        $log.debug_backtrace(e.backtrace)
      end
    end

    def shutdown
      @get_thread.kill
      @connect_thread.kill
      @client.disconnect
    end
  end
end
