# Copyright (C) The Arvados Authors. All rights reserved.
#
# SPDX-License-Identifier: AGPL-3.0

require 'diagnostics_test_helper'

class PipelineTest < DiagnosticsTest
  pipelines_to_test = Rails.configuration.pipelines_to_test.andand.keys

  setup do
    need_selenium 'to make websockets work'
  end

  pipelines_to_test.andand.each do |pipeline_to_test|
    test "run pipeline: #{pipeline_to_test}" do
      visit_page_with_token 'active'
      pipeline_config = Rails.configuration.pipelines_to_test[pipeline_to_test]

      # Search for tutorial template
      find '.navbar-fixed-top'
      within('.navbar-fixed-top') do
        page.find_field('search this site').set pipeline_config['template_uuid']
        page.find('.glyphicon-search').click
      end

      # Run the pipeline
      assert_triggers_dom_event 'shown.bs.modal' do
        find('a,button', text: 'Run').click
      end

      # Choose project
      within('.modal-dialog') do
        find('.selectable', text: 'Home').click
        find('button', text: 'Choose').click
      end

      page.assert_selector('a.disabled,button.disabled', text: 'Run') if pipeline_config['input_paths'].any?

      # Choose input for the pipeline
      pipeline_config['input_paths'].each do |look_for|
        select_input look_for
      end
      wait_for_ajax

      # All needed input are filled in. Run this pipeline now
      find('a,button', text: 'Components').click
      find('a,button', text: 'Run').click

      # Pipeline is running. We have a "Pause" button instead now.
      page.assert_selector 'a,button', text: 'Pause'

      # Wait for pipeline run to complete
      wait_until_page_has 'completed', pipeline_config['max_wait_seconds']
    end
  end
end
