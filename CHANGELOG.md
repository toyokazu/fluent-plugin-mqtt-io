## 0.4.1 (2018-01-06)
Features:
  - change fluentd dependency from ~> 0.14 to >= 0.14

Deprecations:
  - reconnection without fluentd restart
  - reconnection interval configuration
    - Reason: zombie threads cannot be killed properly from plugin codes


## 0.3.0 (2017-03-20)

Features:
  - compatible with fluentd v0.14
  - change license from MIT License to Apache License Version 2.0

Deprecations:
  - deprecated support of fluentd v0.12
