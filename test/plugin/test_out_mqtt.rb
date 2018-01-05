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
        client_id aa-bb-cc-dd
        retain true
        qos 2
        <monitor>
          send_time true
        </monitor>
        <security>
          use_tls true
          <tls>
            ca_file /cert/cacert.pem
            key_file /cert/private.key
            cert_file /cert/cert.pem
          </tls>
        </security>
      ]
      assert_equal '127.0.0.1', d.instance.host
      assert_equal 1300, d.instance.port
      assert_equal 'aa-bb-cc-dd', d.instance.client_id
      assert_equal true, d.instance.retain
      assert_equal 2, d.instance.qos

      assert_equal true, d.instance.monitor.send_time

      # cannot test formatter configuration (default: single_value)

      assert_equal true, d.instance.security.use_tls
      assert_equal '/cert/cacert.pem', d.instance.security.tls.ca_file
      assert_equal '/cert/private.key', d.instance.security.tls.key_file
      assert_equal '/cert/cert.pem', d.instance.security.tls.cert_file
   end
  end
end
