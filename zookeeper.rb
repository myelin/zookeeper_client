# Ruby wrapper for the Zookeeper C API
# Phillip Pearson <pp@myelin.co.nz>

require 'c_zookeeper'

class ZkStat
  attr_reader :version
  def initialize(ary)
    @czxid, @mzxid, @ctime, @mtime, @version, @cversion, @aversion, @ephemeralOwner = ary
  end
end

class Zookeeper < CZookeeper
  def initialize(host)
    super(host)
    @watchers = {} # path => [ block, block, ... ]
  end

  def exists(path, &blk)
    (@watchers[path] ||= []) << blk if blk
    ZkStat.new(super(path, !!blk))
  end

  def stat(path, &blk)
    exists(path, &blk)
  rescue Zookeeper::NoNodeError
    nil
  end

  def get(path)
    value, stat = super
    [value, ZkStat.new(stat)]
  end

  def try_acquire(path, value)
    # create the parent node if it doesn't exist already
    create(path, "lock node", 0) unless stat(path)

    # attempt to obtain the lock
    realpath = create("#{path}/lock-", value, Zookeeper::EPHEMERAL | Zookeeper::SEQUENCE)
    #puts "created lock node #{realpath}"

    # see if we got it
    serial = /lock-(\d+)$/.match(realpath).captures[0].to_i
    have_lock = true
    ls(path).each do |child|
      if m = /lock-(\d+)$/.match(child)
        if m.captures[0].to_i < serial
          have_lock = false
          break
        end
      end
    end

    # call block
    yield(have_lock)

    # release the lock
    #puts "deleting #{realpath}"
    delete(realpath, stat(realpath).version)
  end

  def watcher(type, state, path)
    raise Exception("watchers don't work in ruby yet") # ... until I figure out how to synchronize access to the Ruby interpreter

    return unless type == SESSION_EVENT

    case state
    when CONNECTED_STATE
      puts "ruby watcher; got an event for #{path}"

    when AUTH_FAILED_STATE
      raise Exception, "auth failure"
    when EXPIRED_SESSION_STATE
      raise Exception, "session expired"
    end
  end
end
