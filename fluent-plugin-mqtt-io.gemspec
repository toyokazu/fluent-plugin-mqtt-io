# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)

Gem::Specification.new do |spec|
  spec.name          = "fluent-plugin-mqtt-io"
  spec.version       = "0.6.0"
  spec.authors       = ["Toyokazu Akiyama"]
  spec.email         = ["toyokazu@gmail.com"]

  spec.summary       = %q{fluentd input/output plugin for mqtt broker}
  spec.description   = %q{fluentd input/output plugin for mqtt broker}
  spec.homepage      = "https://github.com/toyokazu/fluent-plugin-mqtt-io"
  spec.license       = "Apache License Version 2.0"

  spec.files         = `git ls-files`.gsub(/images\/[\w\.]+\n/, "").split($/)
  spec.bindir        = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.required_ruby_version = '>= 2.2.0'

  spec.add_dependency 'fluentd', [">= 0.14.0", "< 2"]
  spec.add_dependency "mqtt", "~> 0.5"

  spec.add_development_dependency "bundler", [">= 1.14", "< 2.3"]
  spec.add_development_dependency "rake", "~> 13.0"
  spec.add_development_dependency "test-unit"
end
