module Fluent
  class MqttBufferedOutput < BufferedOutput
    require 'fluent/plugin/mqtt_output_mixin'
    include Fluent::MqttOutputMixin

    # First, register the plugin. NAME is the name of this plugin
    # and identifies the plugin in the configuration file.
    Fluent::Plugin.register_output('mqtt_buf', self)

    # This method is called when an event reaches to Fluentd.
    # Convert the event to a raw string.
    def format(tag, time, record)
      [tag, time, record].to_msgpack
    end

    # This method is called every flush interval. Write the buffer chunk
    # to files or databases here.
    # 'chunk' is a buffer chunk that includes multiple formatted
    # events. You can use 'data = chunk.read' to get all events and
    # 'chunk.open {|io| ... }' to get IO objects.
    #
    # NOTE! This method is called by internal thread, not Fluentd's main thread. So IO wait doesn't affect other plugins.
    def write(chunk)
      messages = {}
      chunk.msgpack_each do |tag, time, record|
        messages[tag] = [] if messages[tag].nil?
        messages[tag] << add_send_time(record).merge(timestamp_hash(time))
      end
      messages.keys.each do |tag|
        $log.debug "Thread ID: #{Thread.current.object_id}, topic: #{rewrite_tag(tag)}, message: #{messages[tag]}"
        @client.publish(rewrite_tag(tag), messages[tag].map {|m| m.to_json}.join(@bulk_trans_sep))
      end
      $log.flush
    end
  end
end
