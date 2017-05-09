require 'fluent/plugin/output'

module Fluent::Plugin
  class DogstatsdOutput < Output
    Fluent::Plugin.register_output('dogstatsd', self)

    helpers :compat_parameters

    DEFAULT_BUFFER_TYPE = "memory"

    config_param :host, :string, :default => nil
    config_param :port, :integer, :default => nil
    config_param :use_tag_as_key, :bool, :default => false
    config_param :use_tag_as_key_if_missing, :bool, :default => false
    config_param :flat_tags, :bool, :default => false
    config_param :flat_tag, :bool, :default => false # obsolete
    config_param :metric_type, :string, :default => nil
    config_param :value_key, :string, :default => nil
    config_param :sample_rate, :float, :default => nil

    config_section :buffer do
      config_set_default :@type, DEFAULT_BUFFER_TYPE
      config_set_default :chunk_keys, ['tag']
    end

    unless method_defined?(:log)
      define_method(:log) { $log }
    end

    attr_accessor :statsd

    def initialize
      super

      require 'statsd' # dogstatsd-ruby
    end

    def configure(conf)
      compat_parameters_convert(conf, :buffer)
      super
      raise Fluent::ConfigError, "'tag' in chunk_keys is required." if not @chunk_key_tag
    end

    def start
      super

      host = @host || Statsd::DEFAULT_HOST
      port = @port || Statsd::DEFAULT_PORT

      @statsd ||= Statsd.new(host, port)
    end

    def format(tag, time, record)
      [time, record].to_msgpack
    end

    def formatted_to_msgpack_binary
      true
    end

    def write(chunk)
      tag = chunk.metadata.tag
      @statsd.batch do |s|
        chunk.msgpack_each do |time, record|
          key = if @use_tag_as_key
                  tag
                else
                  record.delete('key')
                end

          if !key && @use_tag_as_key_if_missing
            key = tag
          end

          unless key
            log.warn "'key' is not specified. skip this record:", tag: tag
            next
          end

          value = record.delete(@value_key || 'value')

          options = {}
          title = record.delete('title')
          text  = record.delete('text')
          type  = @metric_type || record.delete('type')
          sample_rate = @sample_rate || record.delete('sample_rate')

          if sample_rate
            options[:sample_rate] = sample_rate
          end

          tags = if @flat_tags || @flat_tag
                   record
                 else
                   record['tags']
                 end
          if tags
            options[:tags] = tags.map do |k, v|
              "#{k}:#{v}"
            end
          end

          case type
          when 'increment'
            s.increment(key, options)
          when 'decrement'
            s.decrement(key, options)
          when 'count'
            s.count(key, value, options)
          when 'gauge'
            s.gauge(key, value, options)
          when 'histogram'
            s.histogram(key, value, options)
          when 'timing'
            s.timing(key, value, options)
          when 'set'
            s.set(key, value, options)
          when 'event'
            options[:alert_type] = record['alert_type']
            s.event(title, text, options)
          when nil
            log.warn "type is not provided (You can provide type via `metric_type` in config or `type` field in a record."
          else
            log.warn "Type '#{type}' is unknown."
          end
        end
      end
    end
  end
end
