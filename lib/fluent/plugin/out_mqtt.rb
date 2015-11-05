module Fluent
  class MqttOutput < Output
    require 'fluent/plugin/mqtt_output_mixin'
    include Fluent::MqttOutputMixin

    # First, register the plugin. NAME is the name of this plugin
    # and identifies the plugin in the configuration file.
    Fluent::Plugin.register_output('mqtt', self)

    def emit(tag, es, chain)
      es.each {|time,record|
        $log.debug "#{tag}, #{format_time(time)}, #{record}"
        @connect.publish(rewrite_tag(tag), record.merge(@time_key => format_time(time)).to_json)
      }
      $log.flush

      chain.next
    end
  end
end
