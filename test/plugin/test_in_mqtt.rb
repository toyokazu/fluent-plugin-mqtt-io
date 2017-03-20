require_relative '../helper'
require 'fluent/test/driver/input'
require 'fluent/plugin/in_mqtt'

class MqttInputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end


  CONFIG = %[
  ]

  def create_driver(conf = CONFIG, opts = {}) 
    Fluent::Test::Driver::Input.new(Fluent::Plugin::MqttInput, opts: opts).configure(conf)
  end

  sub_test_case "configure" do
    test "host and port" do
      d = create_driver %[
          host 127.0.0.1
          port 1300
          <parse>
            @type json
            time_format %FT%T%:z
          </parse>
      ]
      assert_equal '127.0.0.1', d.instance.host
      assert_equal 1300, d.instance.port
    end
  end
end
