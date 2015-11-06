# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)

Gem::Specification.new do |spec|
  spec.name          = "fluent-plugin-mqtt-io"
  spec.version       = "0.0.3"
  spec.authors       = ["Toyokazu Akiyama"]
  spec.email         = ["toyokazu@gmail.com"]

  spec.summary       = %q{fluentd input/output plugin for mqtt broker}
  spec.description   = %q{fluentd input/output plugin for mqtt broker}
  spec.homepage      = "https://github.com/toyokazu/fluent-plugin-mqtt-io"
  spec.license       = "MIT"

  spec.files         = `git ls-files`.split($/)
  spec.bindir        = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.10"
  spec.add_development_dependency "rake", "~> 10.0"
  spec.add_development_dependency "mqtt"
end
