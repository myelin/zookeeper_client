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

  def get(path)
    value, stat = super
    [value, ZkStat.new(stat)]
  end
end
