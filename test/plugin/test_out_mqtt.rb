require_relative '../helper'
require 'fluent/test/driver/output'
require 'fluent/plugin/out_mqtt'

class MqttOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end


  CONFIG = %[
  ]

  def create_driver(conf = CONFIG, opts = {}) 
    Fluent::Test::Driver::Output.new(Fluent::Plugin::MqttOutput, opts: opts).configure(conf)
  end

  sub_test_case 'non-buffered' do
    test 'configure' do
      d = create_driver %[
        host 127.0.0.1
        port 1300
        <format>
          @type json
        </format>
      ]
      assert_equal '127.0.0.1', d.instance.host
      assert_equal 1300, d.instance.port
    end
  end
end
