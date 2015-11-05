module Fluent
  class MqttOutput < Output
    require 'fluent/plugin/mqtt_output_mixin'
    include Fluent::MqttOutputMixin

    # First, register the plugin. NAME is the name of this plugin
    # and identifies the plugin in the configuration file.
    Fluent::Plugin.register_output('mqtt', self)

    def emit(tag, es, chain)
      es.each {|time,record|
        @connect.publish(tag, record.to_json)
      }
      $log.flush

      chain.next
    end
  end
end
