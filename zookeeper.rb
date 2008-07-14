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
  def exists(path)
    ZkStat.new(super)
  end

  def stat(path)
    exists(path)
  rescue Zookeeper::NoNodeError
    nil
  end

  def get(path)
    value, stat = super
    [value, ZkStat.new(stat)]
  end

  def try_acquire(path, value)
    create(path, "lock node", 0) unless stat(path)

    # attempt to obtain the lock
    realpath = create("#{path}/lock-", value, Zookeeper::EPHEMERAL | Zookeeper::SEQUENCE)
    puts "created lock node #{realpath}"

    # see if we got it
    serial = /lock-(\d+)$/.match(realpath).captures[0].to_i
    child_serials = ls(path).map { |child|
      if m = /lock-(\d+)$/.match(child)
        m.captures[0].to_i
      else
        nil
      end
    } .reject { |n| n.nil? }
    puts "  my serial is #{serial}; child serials are #{child_serials.inspect}"
    have_lock = (serial == child_serials.min)

    # call block
    yield(have_lock)

    # release the lock
    delete(realpath, stat(realpath).version)
  end
end
