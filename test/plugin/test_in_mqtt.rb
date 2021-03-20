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
          client_id aa-bb-cc-dd
          <monitor>
            recv_time true
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
      
      assert_equal 'none', d.instance.parser_configs.first[:@type]

      assert_equal true, d.instance.monitor.recv_time

      assert_equal true, d.instance.security.use_tls
      assert_equal '/cert/cacert.pem', d.instance.security.tls.ca_file
      assert_equal '/cert/private.key', d.instance.security.tls.key_file
      assert_equal '/cert/cert.pem', d.instance.security.tls.cert_file

    end

    test "topic with qos" do
      d = create_driver %[
          topic_with_qos ["a/b",1]
      ]

      assert_equal ["a/b",1], d.instance.topic_with_qos

    end

    test "highly available hosts" do
      d = create_driver %[
          ha_hosts ["host1","host2"]
      ]

      assert_equal ["host1","host2"], d.instance.ha_hosts

    end
  end
end
