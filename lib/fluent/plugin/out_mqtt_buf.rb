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
      #[tag, time, record].to_json + "\n"
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
        #$log.debug "Thread ID: #{Thread.current.object_id}, tag: #{tag}, time: #{format_time(time)}, record: #{record}"
        messages[tag] = [] if messages[tag].nil?
        messages[tag] << record.merge(timestamp_hash(time))
      end
      messages.keys.each do |tag|
        $log.debug "Thread ID: #{Thread.current.object_id}, topic: #{rewrite_tag(tag)}, message: #{messages[tag]}"
        @client.publish(rewrite_tag(tag), messages[tag].map {|m| m.to_json}.join(@bulk_trans_sep))
      end
      $log.flush
      #json = json_parse(chunk.open {|io| io.readline})
      #$log.debug "#{json[0]}, #{format_time(json[1])}, #{json[2]}"
      #@client.publish(rewrite_tag(json[0]), (json[2].merge(timestamp_hash(json[1]))).to_json)
      #$log.flush
    end
  end
end
