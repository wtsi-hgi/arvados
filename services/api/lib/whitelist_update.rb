# Copyright (C) The Arvados Authors. All rights reserved.
#
# SPDX-License-Identifier: AGPL-3.0

require 'new_relic/agent/method_tracer'

module WhitelistUpdate
  def check_update_whitelist permitted_fields
    attribute_names.each do |field|
      if !permitted_fields.include?(field.to_sym) && really_changed(field)
        errors.add field, "cannot be modified in this state (#{send(field+"_was").inspect}, #{send(field).inspect})"
      end
    end
  end

  def really_changed(attr)
    chg = nil
    self.class.trace_execution_scoped(['Custom/whitelist_update/really_changed']) do
      return false if !send(attr+"_changed?")
      old = send(attr+"_was")
      new = send(attr)
      if (old.nil? || old == [] || old == {}) && (new.nil? || new == [] || new == {})
        chg = false
      else
        chg = old != new
      end
    end
    chg
  end

  def validate_state_change
    self.class.trace_execution_scoped(['Custom/whitelist_update/validate_state_change']) do
      if self.state_changed?
        unless state_transitions[self.state_was].andand.include? self.state
          errors.add :state, "cannot change from #{self.state_was} to #{self.state}"
          return false
        end
      end
    end
  end
end
