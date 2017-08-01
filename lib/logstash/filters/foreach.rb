# encoding: utf-8
require "logstash/filters/base"
require "logstash/namespace"

# This  filter will split event by array field, and later join back
class LogStash::Filters::Foreach < LogStash::Filters::Base

  FAILURE_TAG = '_foreach_type_failure'.freeze

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


  end # def register

  public
  def filter(event)


    @logger.debug("For task_id pattern '#{@task_id}' @@event_data = #{@@event_data.inspect()}")

    task_id = event.sprintf(@task_id)

    @@mutex.synchronize do

      if !@@configuration_data.has_key?(@task_id) or !@@configuration_data[@task_id].end_filter_configured
        raise LogStash::ConfigurationError, "Foreach plugin: For task_id pattern '#{@task_id}', there should be one `start` and one `end` filter."
      end

      configuration = @@configuration_data[@task_id]

      if !@@event_data.has_key?(task_id)
        @@event_data[task_id] = LogStash::Filters::Foreach::Element.new(configuration, Time.now(),event.clone, configuration.join_fields)
      end

      event_data = @@event_data[task_id]

      if !@end

        splits = event.remove(@array_field)

        if !splits.is_a?(Array)
          @logger.warn("Foreach plugin: Field should by of Array type. field:#{@array_field} is of type = #{splits.class}")
          event.tag(FAILURE_TAG)
          return
        end

        splits.each do |value|
          next if value.nil? or value.empty?

          event_split = event.clone
          @logger.debug("Foreach plugin: Split event", :field => @array_field, :value => value)
          event_split.set(@array_field, value)
          event_data.counter += 1
          filter_matched(event_split)

          # Push this new event onto the stack at the LogStash::FilterWorker
          yield event_split
        end

        # Cancel this event, we'll use the newly generated ones above.
        event.cancel

      else

        @logger.debug("Foreach plugin: Join event back", :field => configuration.array_field, :value => event.get(configuration.array_field))

        event_data.lastevent_timestamp = Time.now()

        configuration.join_fields.each do |join_field|
          event_data.join_fields[join_field] += [*event.get(join_field)]
        end
        event_data.counter -= 1

        if event_data.counter == 0
          configuration.join_fields.each do |join_field|
            event_data.initial_event.set(join_field, event_data.join_fields[join_field])
          end
          filter_matched(event_data.initial_event)
          yield event_data.initial_event
          @@event_data.delete(task_id)
        end

        event.cancel

      end # if !@end

    end # @@mutex.synchronize

  end # def filter

  def flush(options = {})
    @@mutex.synchronize do
      @@event_data.each do |task_id, obj|
        if obj.lastevent_timestamp < Time.now() - obj.configuration.timeout
          @@event_data.delete(task_id)
        end
      end
    end # @@mutex.synchronize
    return []

  end # def flush

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

  attr_accessor :initial_event, :counter, :join_fields, :lastevent_timestamp, :configuration

  def initialize(configuration, creation_timestamp, event, join_fields)
    # @creation_timestamp = creation_timestamp
    @configuration = configuration
    @lastevent_timestamp = creation_timestamp
    @initial_event = event
    @counter = 0
    @join_fields = {}
    join_fields.each do |join_field|
      @join_fields[join_field] = []
    end
  end
end
