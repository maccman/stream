require 'rubygems'
require 'bundler'

Bundler.require

require 'json'

ENV['REDIS_URL'] ||= ENV['REDISCLOUD_URL']

Thread.abort_on_exception = true

class Stream
  def self.streams
    @streams ||= Hash.new { |hash, key| hash[key] = [] }
  end

  def self.enabled?
    unless defined? @enabled
      @enabled = true
    end
    @enabled
  end

  def self.disable!
    @enabled = false
    streams.each(&:close!)
  end

  def self.publish(channel, message, options = {})
    streams = self.streams[channel]

    if except_id = options[:except]
      streams = streams.reject {|s| s.id == except_id }
    end

    streams.each {|c| c.publish(:message, message) }
  end

  attr_reader :out, :id

  def initialize(out)
    @id  = SecureRandom.uuid
    @out = out
    @channels = []

    subscribe :all

    @timer = EventMachine::PeriodicTimer.new(
      20, method(:keepalive)
    )

    setup
  end

  def subscribe(channel)
    @channels << channel
    self.class.streams[channel] << self
  end

  def close!
    @timer.cancel

    @channels.each do |name|
      self.class.channels[name].delete(self)
    end
  end

  def publish(event, message)
    self << "event: #{event}\n"
    self << "data: #{message}\n\n"
  end

  protected

  def <<(data)
    return close! if out.closed?
    out << data
  end

  # Only keep streams alive for so long
  def keepalive_timeout?
    @keepalive_count ||= 0
    @keepalive_count += 1
    @keepalive_count > 10
  end

  def keepalive
    return close! if keepalive_timeout?
    publish(:keepalive, '')
  end

  def setup
    publish(:setup, id)
    self << "retry: 5000\n"
  end
end

get '/subscribe/?:channel?', :provides => 'text/event-stream' do
  error 402 unless Stream.enabled?

  stream :keep_open do |out|
    stream = Stream.new(out)

    if params[:channel]
      stream.subscribe(params[:channel])
    end

    out.callback { stream.close! }
    out.errback {  stream.close! }
  end
end

Thread.new do
  redis = Redis.connect

  redis.psubscribe('stream', 'stream.*') do |on|
    on.pmessage do |match, channel, message|
      channel = channel.sub('stream.', '')
      Stream.publish(channel, message)
    end
  end
end

# Make sure we shut down all long-lived streams
# when we receive a TERM from Heroku
trap('TERM') do
  Stream.disable!
  Process.kill('INT', $$)
end