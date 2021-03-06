# encoding: utf-8
require_relative '../spec_helper'
require "logstash/filters/foreach"
require "logstash/filters/mutate"
require "logstash/filters/drop"
require "rspec/wait"

java_import org.apache.logging.log4j.LogManager

describe LogStash::Filters::Foreach do

  before(:each) do
    # LogStash::Logging::Logger::configure_logging("DEBUG", "logstash.filters.foreach")
    LogStash::Filters::Foreach.class_variable_get(:@@configuration_data).clear()
  end

  after(:each) do
    # LogStash::Logging::Logger::configure_logging("ERROR", "logstash.filters.foreach")
  end

  context "In validation stage" do

    describe "should throw exception without end filter" do
      let(:config) do
        <<-CONFIG
        filter {
          foreach {
            task_id => "%{task_id}"
            array_field => "array"
            join_fields => ["join"]
          }
        }
        CONFIG
      end

      sample("task_id" => 1, "array" => ["big", "bird", "sesame street"], "unchanged" => "unchanged_value") do
        insist { subject }.raises(LogStash::ConfigurationError)
      end
    end

    describe "should throw exception without start filter" do
      let(:config) do
        <<-CONFIG
        filter {
          foreach {
            task_id => "%{task_id}"
            end => true
          }
        }
        CONFIG
      end

      sample("task_id" => 1, "array" => ["big", "bird", "sesame street"], "unchanged" => "unchanged_value") do
        insist { subject }.raises(LogStash::ConfigurationError)
      end
    end

    describe "should throw exception with two start filters" do
      let(:config) do
        <<-CONFIG
        filter {
          foreach {
            task_id => "%{task_id}"
            array_field => "array"
            join_fields => ["join"]
          }
        }
        filter {
          foreach {
            task_id => "%{task_id}"
            array_field => "array"
            join_fields => ["join"]
          }
        }
        filter {
          foreach {
            task_id => "%{task_id}"
            end => true
          }
        }
        CONFIG
      end

      sample("task_id" => 1, "array" => ["big", "bird", "sesame street"], "unchanged" => "unchanged_value") do
        insist { subject }.raises(LogStash::ConfigurationError)
      end
    end

    describe "should throw exception with two end filters" do
      let(:config) do
        <<-CONFIG
        filter {
          foreach {
            task_id => "%{task_id}"
            array_field => "array"
            join_fields => ["join"]
          }
        }
        filter {
          foreach {
            task_id => "%{task_id}"
            end => true
          }
        }
        filter {
          foreach {
            task_id => "%{task_id}"
            end => true
          }
        }
        CONFIG
      end

      sample("task_id" => 1, "array" => ["big", "bird", "sesame street"], "unchanged" => "unchanged_value") do
        insist { subject }.raises(LogStash::ConfigurationError)
      end
    end

  end

  context "Filtering" do

    describe "should split and join correctly" do
      let(:config) do
        <<-CONFIG
        filter {
          foreach {
            task_id => "%{task_id}"
            array_field => "array"
            join_fields => ["join"]
          }

          mutate {
            add_field => { "join" => "%{array}_changed" }
          }

          foreach {
            task_id => "%{task_id}"
            end => true
          }
        }
        CONFIG
      end

      sample("task_id" => 1, "array" => ["big", "bird", "sesame street"], "unchanged" => "unchanged_value") do
        insist { subject.is_a?(LogStash::Event) } == true
        insist { subject.get("array").is_a?(Array) } == true
        insist { subject.get("array") } == ["big", "bird", "sesame street"]
        insist { subject.get("join").is_a?(Array) } == true
        insist { subject.get("join") } == ["big_changed", "bird_changed", "sesame street_changed"]
        insist { subject.get("unchanged").is_a?(String) } == true
        insist { subject.get("unchanged") } == "unchanged_value"
      end
    end

    describe "should split and join correctly with nested field" do
      let(:config) do
        <<-CONFIG
        filter {
          foreach {
            task_id => "%{task_id}"
            array_field => "[array][nested]"
            join_fields => ["join"]
          }

          mutate {
            add_field => { "join" => "%{[array][nested]}_changed" }
          }

          foreach {
            task_id => "%{task_id}"
            end => true
          }
        }
        CONFIG
      end

      sample("task_id" => 1, "array" => {"nested" => ["big", "bird", "sesame street"]}, "unchanged" => "unchanged_value") do
        insist { subject.is_a?(LogStash::Event) } == true
        insist { subject.get("[array][nested]").is_a?(Array) } == true
        insist { subject.get("[array][nested]") } == ["big", "bird", "sesame street"]
        insist { subject.get("join").is_a?(Array) } == true
        insist { subject.get("join") } == ["big_changed", "bird_changed", "sesame street_changed"]
        insist { subject.get("unchanged").is_a?(String) } == true
        insist { subject.get("unchanged") } == "unchanged_value"
      end
    end

    describe "should split and join correctly with array joins" do
      let(:config) do
        <<-CONFIG
        filter {
          foreach {
            task_id => "%{task_id}"
            array_field => "array"
            join_fields => ["join"]
          }

          mutate {
            add_field => { "join" => ["%{array}_changed", "%{array}_changed2"] }
          }

          foreach {
            task_id => "%{task_id}"
            end => true
          }
        }
        CONFIG
      end

      sample("task_id" => 1, "array" => ["big", "bird", "sesame street"], "unchanged" => "unchanged_value") do
        insist { subject.is_a?(LogStash::Event) } == true
        insist { subject.get("array").is_a?(Array) } == true
        insist { subject.get("array") } == ["big", "bird", "sesame street"]
        insist { subject.get("join").is_a?(Array) } == true
        insist { subject.get("join") } == ["big_changed", "big_changed2", "bird_changed", "bird_changed2", "sesame street_changed", "sesame street_changed2"]
        insist { subject.get("unchanged").is_a?(String) } == true
        insist { subject.get("unchanged") } == "unchanged_value"
      end
    end

    describe "should passthrough event with incorrect task_id" do
      let(:config) do
        <<-CONFIG
        filter {
          foreach {
            task_id => "%{task_id}"
            array_field => "array"
            join_fields => ["join"]
          }

          mutate {
            add_field => { "join" => "%{array}_changed" }
          }

          foreach {
            task_id => "%{task_id}"
            end => true
          }
        }
        CONFIG
      end

      sample("unchanged" => "unchanged_value") do
        insist { subject.is_a?(LogStash::Event) } == true
        insist { subject.get("join").is_a?(String) } == true
        insist { subject.get("join") } == "%{array}_changed"
        insist { subject.get("unchanged").is_a?(String) } == true
        insist { subject.get("unchanged") } == "unchanged_value"
      end
    end

    describe "should passthrough event without array_field" do
      let(:config) do
        <<-CONFIG
        filter {
          foreach {
            task_id => "%{task_id}"
            array_field => "array"
            join_fields => ["join"]
          }

          mutate {
            add_field => { "join" => "%{array}_changed" }
          }

          foreach {
            task_id => "%{task_id}"
            end => true
          }
        }
        CONFIG
      end

      sample("task_id" => 1, "unchanged" => "unchanged_value") do
        insist { subject.is_a?(LogStash::Event) } == true
        insist { subject.get("join").is_a?(String) } == true
        insist { subject.get("join") } == "%{array}_changed"
        insist { subject.get("unchanged").is_a?(String) } == true
        insist { subject.get("unchanged") } == "unchanged_value"
      end
    end

    describe "should omit processing for event with array_field = []" do
      let(:config) do
        <<-CONFIG
        filter {
          foreach {
            task_id => "%{task_id}"
            array_field => "array"
            join_fields => ["join"]
          }

          mutate {
            add_field => { "join" => "%{array}_changed" }
          }

          foreach {
            task_id => "%{task_id}"
            end => true
          }
        }
        CONFIG
      end

      sample("task_id" => 1, "array" => [], "unchanged" => "unchanged_value") do
        insist { subject.is_a?(LogStash::Event) } == true
        insist { subject.get("array").is_a?(Array) } == true
        insist { subject.get("array") } == []
        insist { subject.get("join").nil? } == true
        insist { subject.get("unchanged").is_a?(String) } == true
        insist { subject.get("unchanged") } == "unchanged_value"
      end
    end

    describe "should split and join (partly) correctly" do
      let(:config) do
        <<-CONFIG
        filter {
          foreach {
            task_id => "%{task_id}"
            array_field => "array"
            join_fields => ["join"]
          }

          if [array] != "bird" {
            mutate {
              add_field => { "join" => "%{array}_changed" }
            }
          }

          foreach {
            task_id => "%{task_id}"
            end => true
          }
        }
        CONFIG
      end

      sample("task_id" => 1, "array" => ["big", "bird", "sesame street"], "unchanged" => "unchanged_value") do
        insist { subject.is_a?(LogStash::Event) } == true
        insist { subject.get("array").is_a?(Array) } == true
        insist { subject.get("array") } == ["big", "bird", "sesame street"]
        insist { subject.get("join").is_a?(Array) } == true
        insist { subject.get("join") } == ["big_changed", "sesame street_changed"]
        insist { subject.get("unchanged").is_a?(String) } == true
        insist { subject.get("unchanged") } == "unchanged_value"
      end
    end

    describe "should clear data on timeout" do
      let(:config) do
        <<-CONFIG
        filter {
          foreach {
            task_id => "%{task_id}"
            array_field => "array"
            join_fields => ["join"]
            timeout => 3
          }

          drop {}

          foreach {
            task_id => "%{task_id}"
            end => true
          }
        }
        CONFIG
      end

      ["1", "2", "3"].each do |task_id|
        sample("task_id" => task_id, "array" => ["big", "bird", "sesame street"], "unchanged" => "unchanged_value") do
          insist { subject.nil? } == true
          insist { LogStash::Filters::Foreach.class_variable_get(:@@event_data).has_key?(task_id) } == true
          sleep 3
          flushed_events = []
          pipeline.flush_filters(:final => false) { |flushed_event| flushed_events << flushed_event }
          insist { LogStash::Filters::Foreach.class_variable_get(:@@event_data).has_key?(task_id) } == false
          insist { flushed_events.length } == 0
        end
      end

    end

    describe "should send partial data on timeout" do
      let(:config) do
        <<-CONFIG
        filter {
          foreach {
            task_id => "%{task_id}"
            array_field => "array"
            join_fields => ["join"]
            timeout => 3
          }

          if [array] == "bird" {
            drop {}
          }

          mutate {
            add_field => { "join" => "%{array}_changed" }
          }

          foreach {
            task_id => "%{task_id}"
            end => true
          }
        }
        CONFIG
      end

      ["1", "2", "3"].each do |task_id|
        sample("task_id" => task_id, "array" => ["big", "bird", "sesame street"], "unchanged" => "unchanged_value") do
          insist { subject.nil? } == true
          insist { LogStash::Filters::Foreach.class_variable_get(:@@event_data).has_key?(task_id) } == true
          sleep 3
          flushed_events = []
          pipeline.flush_filters(:final => false) { |flushed_event| flushed_events << flushed_event }
          insist { LogStash::Filters::Foreach.class_variable_get(:@@event_data).has_key?(task_id) } == false
          insist { flushed_events.length } == 1
          insist { flushed_events[0].get('task_id') } == task_id
          insist { flushed_events[0].get("array").is_a?(Array) } == true
          insist { flushed_events[0].get("array") } == ["big", "bird", "sesame street"]
          insist { flushed_events[0].get("join").is_a?(Array) } == true
          insist { flushed_events[0].get("join") } == ["big_changed", "sesame street_changed"]
          insist { flushed_events[0].get("unchanged").is_a?(String) } == true
          insist { flushed_events[0].get("unchanged") } == "unchanged_value"
        end
      end

    end

    describe "should work with @metadata fields" do
      let(:config) do
        <<-CONFIG
        filter {
          foreach {
            task_id => "%{task_id}"
            array_field => "array"
            join_fields => ["join"]
          }

          mutate {
            add_field => { "join" => "%{array}_changed" }
          }

          foreach {
            task_id => "%{task_id}"
            end => true
          }
        }
        CONFIG
      end

      sample("task_id" => 1, "array" => ["big", "bird", "sesame street"], "@metadata" => {"unchanged" => "unchanged_value"}) do
        insist { subject.is_a?(LogStash::Event) } == true
        insist { subject.get("array").is_a?(Array) } == true
        insist { subject.get("array") } == ["big", "bird", "sesame street"]
        insist { subject.get("join").is_a?(Array) } == true
        insist { subject.get("join") } == ["big_changed", "bird_changed", "sesame street_changed"]
        insist { subject.get("@metadata").is_a?(Object) } == true
        insist { subject.get("[@metadata][unchanged]") } == "unchanged_value"
      end
    end

    describe "should pass @metadata fields" do
      let(:config) do
        <<-CONFIG
        filter {
          foreach {
            task_id => "%{task_id}"
            array_field => "array"
            join_fields => ["join"]
          }

          mutate {
            add_field => { "join" => "%{array}_changed" }
          }

          foreach {
            task_id => "%{task_id}"
            end => true
          }
        }
        CONFIG
      end

      sample("task_id" => 1, "@metadata" => {"unchanged" => "unchanged_value"}) do
        insist { subject.is_a?(LogStash::Event) } == true
        insist { subject.get("@metadata").is_a?(Object) } == true
        insist { subject.get("[@metadata][unchanged]") } == "unchanged_value"
      end
    end

    describe "should not set empty arrays" do
      let(:config) do
        <<-CONFIG
        filter {
          foreach {
            task_id => "%{task_id}"
            array_field => "array"
            join_fields => ["join", "join2"]
          }

          mutate {
            add_field => { "join" => "%{array}_changed" }
          }

          foreach {
            task_id => "%{task_id}"
            end => true
          }
        }
        CONFIG
      end

      sample("task_id" => 1, "array" => ["big", "bird", "sesame street"], "unchanged" => "unchanged_value") do
        insist { subject.is_a?(LogStash::Event) } == true
        insist { subject.get("array").is_a?(Array) } == true
        insist { subject.get("array") } == ["big", "bird", "sesame street"]
        insist { subject.get("join").is_a?(Array) } == true
        insist { subject.get("join") } == ["big_changed", "bird_changed", "sesame street_changed"]
        insist { subject.get("join2").nil? } == true
      end
    end

    describe "should work with nested loops" do
      let(:config) do
        <<-CONFIG
        filter {
          foreach {
            task_id => "%{task_id}"
            array_field => "array"
            join_fields => ["join", "join2"]
          }

          mutate {
            add_field => { "join" => "%{[array][str]}_changed" }
          }

          foreach {
            task_id => "%{task_id}_%{[array][str]}"
            array_field => "[array][nested]"
            join_fields => ["join2"]
          }
          
          mutate {
            add_field => { "join2" => [ "%{[array][nested]}_changed", "%{[array][nested]}_changed2" ] }
          }

          foreach {
            task_id => "%{task_id}_%{[array][str]}"
            end => true
          }

          foreach {
            task_id => "%{task_id}"
            end => true
          }
        }
        CONFIG
      end

      sample("task_id" => 1, "array" => [{"str" => "big", "nested" => ["nested_big1", "nested_big2"]}, {"str" => "bird", "nested" => ["nested_bird1", "nested_bird2"]}, {"str" => "sesame street", "nested" => ["nested_sesame street1", "nested_sesame street2"]}], "unchanged" => "unchanged_value") do
        insist { subject.is_a?(LogStash::Event) } == true
        insist { subject.get("join").is_a?(Array) } == true
        insist { subject.get("join") } == ["big_changed", "bird_changed", "sesame street_changed"]
        insist { subject.get("join2").is_a?(Array) } == true
        insist { subject.get("join2") } == [
            "nested_big1_changed", "nested_big1_changed2", "nested_big2_changed", "nested_big2_changed2",
            "nested_bird1_changed", "nested_bird1_changed2", "nested_bird2_changed", "nested_bird2_changed2",
            "nested_sesame street1_changed", "nested_sesame street1_changed2", "nested_sesame street2_changed", "nested_sesame street2_changed2"
        ]
        insist { subject.get("unchanged").is_a?(String) } == true
        insist { subject.get("unchanged") } == "unchanged_value"
      end
    end
  end
end