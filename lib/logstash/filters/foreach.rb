# encoding: utf-8
require "logstash/filters/base"
require "logstash/namespace"

# This  filter will split event by array field, and later join back
class LogStash::Filters::Foreach < LogStash::Filters::Base

  FAILURE_TAG = '_foreach_failure'.freeze

  #
  # filter {
  #   foreach {
  #     task_id => "%{task_id}"
  #     array_field => "field_name"
  #     join_fields => ["join_field_name", "join_field_name2"]
  #   }
  # }
  #
  # ... Process records
  #
  # filter {
  #   foreach {
  #     task_id => "%{task_id}"
  #     end => true
  #   }
  # }
  #
  config_name "foreach"

  config :task_id, :validate => :string, :required => true
  config :array_field, :validate => :string
  config :end, :validate => :boolean, :default => false
  config :join_fields, :validate => :array
  config :timeout, :validate => :number, :default => 60

  @@configuration_data = {}
  @@event_data = {}

  @@mutex = Mutex.new


  public
  def register

    # validate task_id option
    if !@task_id.match(/%\{.+\}/)
      raise LogStash::ConfigurationError, "Foreach plugin: task_id pattern '#{@task_id}' must contain a dynamic expression like '%{field}'"
    end

    if !@end
      if @@configuration_data.has_key?(@task_id)
        raise LogStash::ConfigurationError, "Foreach plugin: For task_id pattern '#{@task_id}', there are more than one filters defined. There should be only one `start` and one `end` filter with the same task_id."
      end
      if !@array_field.is_a?(String)
        raise LogStash::ConfigurationError, "Foreach plugin: For task_id pattern '#{@task_id}': array_field should be a field name, but it is of type = #{@array_field.class}"
      end
      if !@join_fields.is_a?(Array)
        raise LogStash::ConfigurationError, "Foreach plugin: For task_id pattern '#{@task_id}': join_fields should be an Array of fields, but it is of type = #{@join_fields.class}"
      end
      @@configuration_data[@task_id] = LogStash::Filters::Foreach::Configuration.new(@array_field, @join_fields, @timeout)
    else
      if !@@configuration_data.has_key?(@task_id)
        raise LogStash::ConfigurationError, "Foreach plugin: For task_id pattern '#{@task_id}', there are no `start` filter. You should declare `start` filter before `end` filter."
      elsif @@configuration_data[@task_id].end_filter_configured
        raise LogStash::ConfigurationError, "Foreach plugin: For task_id pattern '#{@task_id}', there are more than one filters defined. There should be only one `start` and one `end` filter with the same task_id."
      end
      @@configuration_data[@task_id].end_filter_configured = true
    end


  end

  # def register

  public
  def filter(event)

    @logger.debug("Foreach plugin:", :task_id => @task_id, :array_field => @array_field, :join_fields => @join_fields, :end => @end, :timeout => @timeout, :event => event.to_hash)

    passthrough = false

    task_id = event.sprintf(@task_id)
    if task_id.nil? || task_id == @task_id

      @logger.trace("Foreach plugin: if task_id.nil? || task_id == @task_id");

      @logger.warn("Foreach plugin: #{@task_id} should be calculated into value (not '#{task_id}'). Passing through")
      event.tag(FAILURE_TAG)
      passthrough = true

    else

      @logger.trace("Foreach plugin: else task_id.nil? || task_id == @task_id");

      @@mutex.synchronize do

        if !@@configuration_data.has_key?(@task_id) or !@@configuration_data[@task_id].end_filter_configured

          @logger.trace("Foreach plugin: if !@@configuration_data.has_key?(@task_id) or !@@configuration_data[@task_id].end_filter_configured");

          raise LogStash::ConfigurationError, "Foreach plugin: For task_id pattern '#{@task_id}', there should be one `start` and one `end` filter."
        end

        configuration = @@configuration_data[@task_id]

        if !@end

          @logger.trace("Foreach plugin: if !@end");

          array_field = event.get(@array_field)

          if !array_field.is_a?(Array)

            @logger.trace("Foreach plugin: if !array_field.is_a?(Array)");

            @logger.warn("Foreach plugin: Field should be of Array type. field:#{@array_field} is of type = #{array_field.class}. Passing through")
            event.tag(FAILURE_TAG)
            passthrough = true

          elsif @@event_data.has_key?(task_id)

            @logger.trace("Foreach plugin: elsif @@event_data.has_key?(task_id)");

            @logger.warn("Foreach plugin: task_id whould be unique. Duplicate value found: '#{task_id}'. Passing through")
            event.tag(FAILURE_TAG)
            passthrough = true

          else

            @logger.trace("Foreach plugin: else !array_field.is_a?(Array)");

            @@event_data[task_id] = LogStash::Filters::Foreach::Element.new(configuration, Time.now(), event.clone, configuration.join_fields)
            event_data = @@event_data[task_id]

            array_field.each do |value|

              @logger.trace("Foreach plugin: array_field.each do |value|", :value => value);

              next if value.nil? or value.empty?

              event_split = event.clone
              @logger.debug("Foreach plugin: Split event", :field => @array_field, :value => value)
              event_split.set(@array_field, value)
              event_data.counter += 1

              filter_matched(event_split)
              yield event_split
            end

            event.cancel

          end

        else

          @logger.trace("Foreach plugin: else !@end");

          if !@@event_data.has_key?(task_id)

            @logger.trace("Foreach plugin: if !@@event_data.has_key?(task_id)");

            @logger.warn("Foreach plugin: found `end` event fot task_id = '#{task_id}' without `start` event. Passing through")
            event.tag(FAILURE_TAG)
            passthrough = true

          else

            @logger.trace("Foreach plugin: else !@@event_data.has_key?(task_id)");

            @logger.debug("Foreach plugin: Join event back", :field => configuration.array_field, :value => event.get(configuration.array_field))

            event_data = @@event_data[task_id]
            event_data.lastevent_timestamp = Time.now()

            configuration.join_fields.each do |join_field|
              event_data.join_fields[join_field] += [*event.get(join_field)]
            end
            event_data.counter -= 1

            if event_data.counter == 0

              @logger.trace("Foreach plugin: if event_data.counter == 0");

              configuration.join_fields.each do |join_field|
                event_data.initial_event.set(join_field, event_data.join_fields[join_field])
              end
              filter_matched(event_data.initial_event)
              yield event_data.initial_event
              @@event_data.delete(task_id)
            end

            event.cancel

          end

        end

      end # @@mutex.synchronize

    end # task_id.nil? || task_id == @task_id

    if passthrough

      @logger.trace("Foreach plugin: if passthrough");

      filter_matched(event)
    end

  end

  # def filter

  def flush(options = {})
    events_to_flush = []
    if @end
      @@mutex.synchronize do
        @@event_data.each do |task_id, obj|
          if obj.lastevent_timestamp < Time.now() - obj.configuration.timeout
            if obj.counter < obj.sub_events_count
              @logger.warn("Foreach plugin: Flushing partly processed event with task_id = '#{obj.initial_event.sprintf(@task_id)}' after timeout = '#{obj.configuration.timeout.to_s}'")
              obj.configuration.join_fields.each do |join_field|
                obj.initial_event.set(join_field, obj.join_fields[join_field])
              end
              events_to_flush << obj.initial_event
            else
              @logger.warn("Foreach plugin: Removing unprocessed event with task_id = '#{obj.initial_event.sprintf(@task_id)}' after timeout = '#{obj.configuration.timeout.to_s}'")
            end
            @@event_data.delete(task_id)
          end
        end
      end # @@mutex.synchronize
    end
    return events_to_flush

  end

  # def flush

  def periodic_flush
    true
  end

end # class LogStash::Filters::Foreach

# Element of "event_data"
class LogStash::Filters::Foreach::Configuration

  attr_accessor :end_filter_configured, :array_field, :join_fields, :timeout

  def initialize(array_field, join_fields, timeout)
    @end_filter_configured = false
    @array_field = array_field
    @join_fields = join_fields
    @timeout = timeout
  end
end

class LogStash::Filters::Foreach::Element

  attr_accessor :initial_event, :counter, :sub_events_count, :join_fields, :lastevent_timestamp, :configuration

  def initialize(configuration, creation_timestamp, event, join_fields)
    # @creation_timestamp = creation_timestamp
    @configuration = configuration
    @lastevent_timestamp = creation_timestamp
    @initial_event = event
    @counter = 0
    @sub_events_count = event.get(configuration.array_field).length
    @join_fields = {}
    join_fields.each do |join_field|
      @join_fields[join_field] = []
    end
  end
end
