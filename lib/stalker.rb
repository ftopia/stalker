require 'beanstalk-client'
require 'json'
require 'uri'
require 'timeout'

module Stalker
  extend self
  
  MAX_WAITING_TIME_BEFORE_CHANGING_QUEUE = 30

  def connect(url)
    @@url = url
    beanstalk
  end

  def enqueue(job, args={}, opts={})
    pri   = opts[:pri]   || 65536
    delay = [0, opts[:delay].to_i].max  
    ttr   = opts[:ttr]   || 120
    beanstalk.use job
    beanstalk.put [ job, args ].to_json, pri, delay, ttr
  rescue Beanstalk::NotConnected => e
    failed_connection(e)
  end

  def job(j, &block)
    @@handlers ||= {}
    @@handlers[j] = block
  end

  def before(&block)
    @@before_handlers ||= []
    @@before_handlers << block
  end

  def error(&blk)
    @@error_handler = blk
  end

  class NoJobsDefined < RuntimeError; end
  class NoSuchJob < RuntimeError; end

  def prep(jobs=nil)
    raise NoJobsDefined unless defined?(@@handlers)
    @@error_handler = nil unless defined?(@@error_handler)

    jobs ||= all_jobs

    jobs.each do |job|
      raise(NoSuchJob, job) unless @@handlers[job]
    end

    log "Working #{jobs.size} jobs: [ #{jobs.join(' ')} ]"

    jobs.each { |job| beanstalk.watch(job) }

    beanstalk.list_tubes_watched.each do |server, tubes|
      tubes.each { |tube| beanstalk.ignore(tube) unless jobs.include?(tube) }
    end
  rescue Beanstalk::NotConnected => e
    failed_connection(e)
  end

  def work(jobs=nil)
    prep(jobs)
    trap('TERM') { @stop = true; log "SIGTERM trapped, will stop ASAP" }
    $0 = "waiting for a job"
    loop { work_one_job; break if @stop }
  end

  class JobTimeout < RuntimeError; end
  
  # Return either the latest queue used or the global one
  def q_hint
    @q_hint || beanstalk
  end
  
  # This heuristic is to help prevent one queue from starving. The idea is that
  # if the connection returns a job right away, it probably has more available.
  # But if it takes time, then it's probably empty. So reuse the same
  # connection as long as it stays fast. Otherwise, have no preference.
  # -- originally extracted from AsyncObserver and changed since
  def reserve_and_set_hint()
    t1 = Time.now.utc
    # OPTIMIZE: use a timeout only if we have more than one queue to reserve from
    return job = q_hint.reserve(MAX_WAITING_TIME_BEFORE_CHANGING_QUEUE)
  rescue Beanstalk::TimedOut
    log "did not get any job from this queue, retrying on another one"
    @q_hint = nil
    # OPTIMIZE: ensure we use another queue instead of relying on the random queue selection
    # OPTIMIZE: we should exit at this point, if we received the right signal
    retry
  ensure
    @q_hint = if brief?(t1, Time.now.utc) and job then job.conn else nil end
  end

  def brief?(t1, t2)
    ((t2 - t1) * 100).to_i.abs < 10
  end

  def work_one_job
    job = reserve_and_set_hint
    name, args = JSON.parse job.body
    log_job_begin(name, args)
    handler = @@handlers[name]
    raise(NoSuchJob, name) unless handler
    begin
      Timeout::timeout(job.ttr - 1) do
        if defined? @@before_handlers and @@before_handlers.respond_to? :each
          @@before_handlers.each do |block|
            block.call(name)
          end
        end
        handler.call(args)
      end
    rescue Timeout::Error
      raise JobTimeout, "#{name} hit #{job.ttr-1}s timeout"
    end

    job.delete
    log_job_end(name)
  rescue Beanstalk::NotConnected => e
    failed_connection(e)
  rescue SystemExit
    raise
  rescue => e
    log_error exception_message(e)
    job.bury rescue nil
		log_job_end(name, 'failed') if @job_begun
    if error_handler
      if error_handler.arity == 1
        error_handler.call(e)
      else
        error_handler.call(e, name, args)
      end
    end
  end

  def failed_connection(e)
    log_error exception_message(e)
    log_error "*** Failed connection to #{beanstalk_url}"
    log_error "*** Check that beanstalkd is running (or set a different BEANSTALK_URL)"
    exit 1
  end

  def log_job_begin(name, args)
    @original_procname = $0
    args_flat = unless args.empty?
      '(' + args.inject([]) do |accum, (key,value)|
        accum << "#{key}=#{value}"
      end.join(' ') + ')'
    else
      ''
    end

    log [ "Working", name, args_flat ].join(' ')
    @job_begun = Time.now
    $0 = "since #{@job_begun.to_i} working #{name}#{args_flat}"
  end

  def log_job_end(name, failed=false)
    $0 = @original_procname
    ellapsed = Time.now - @job_begun
    ms = (ellapsed.to_f * 1000).to_i
    log "Finished #{name} in #{ms}ms #{failed ? ' (failed)' : ''}"
  end

  def log(msg)
    puts msg
  end

  def log_error(msg)
    STDERR.puts msg
  end

  def beanstalk
    @@beanstalk ||= Beanstalk::Pool.new(beanstalk_addresses)
  end

  def beanstalk_url
    return @@url if defined?(@@url) and @@url
    ENV['BEANSTALK_URL'] || 'beanstalk://localhost/'
  end

  class BadURL < RuntimeError; end

  def beanstalk_addresses
    uris = beanstalk_url.split(/[\s,]+/)
    uris.map {|uri| beanstalk_host_and_port(uri)}
  end

  def beanstalk_host_and_port(uri_string)
    uri = URI.parse(uri_string)
    raise(BadURL, uri_string) if uri.scheme != 'beanstalk'
    "#{uri.host}:#{uri.port || 11300}"
  end

  def exception_message(e)
    msg = [ "Exception #{e.class} -> #{e.message}" ]

    base = File.expand_path(Dir.pwd) + '/'
    e.backtrace.each do |t|
      msg << "   #{File.expand_path(t).gsub(/#{base}/, '')}"
    end

    msg.join("\n")
  end

  def all_jobs
    @@handlers.keys
  end

  def error_handler
    @@error_handler
  end

  def clear!
    @@handlers = nil
    @@before_handlers = nil
    @@error_handler = nil
  end
end
