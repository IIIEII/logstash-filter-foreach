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
          pipeline.flush_filters(:final => true)
          insist { LogStash::Filters::Foreach.class_variable_get(:@@event_data).has_key?(task_id) } == false
        end
      end

    end
  end
end