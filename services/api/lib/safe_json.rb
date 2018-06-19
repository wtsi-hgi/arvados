# Copyright (C) The Arvados Authors. All rights reserved.
#
# SPDX-License-Identifier: AGPL-3.0

require 'new_relic/agent/method_tracer'

class SafeJSON
  def self.dump(o)
    return Oj.dump(o, mode: :compat)
  end
  def self.load(s)
    Oj.strict_load(s, symbol_keys: false)
  end

  class << self
    include ::NewRelic::Agent::MethodTracer

    add_method_tracer :dump, 'Custom/SafeJSON/dump'
    add_method_tracer :load, 'Custom/SafeJSON/load'
  end
end
