module Fluent
  class MqttOutput < BufferedOutput
    require 'fluent/plugin/mqtt_output_mixin'
    include Fluent::MqttOutputMixin

    # First, register the plugin. NAME is the name of this plugin
    # and identifies the plugin in the configuration file.
    Fluent::Plugin.register_output('mqtt_buf', self)

    # This method is called when an event reaches to Fluentd.
    # Convert the event to a raw string.
    def format(tag, time, record)
      [tag, time, record].to_json + "\n"
    end

    # This method is called every flush interval. Write the buffer chunk
    # to files or databases here.
    # 'chunk' is a buffer chunk that includes multiple formatted
    # events. You can use 'data = chunk.read' to get all events and
    # 'chunk.open {|io| ... }' to get IO objects.
    #
    # NOTE! This method is called by internal thread, not Fluentd's main thread. So IO wait doesn't affect other plugins.
    def write(chunk)
      json = json_parse(chunk.read)
      $log.debug "#{json[0]}, #{format_time(json[1])}, #{json[2]}"
      @connect.publish(rewrite_tag(json[0]), (json[2].merge(@time_key => format_time(json[1]))).to_json)
      $log.flush
    end
  end
end
