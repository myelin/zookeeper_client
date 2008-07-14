require 'zookeeper'

z = Zookeeper.new("localhost:2181")

puts "root: #{z.ls("/").inspect}"

path = "/testing_node"

puts "working with path #{path}"

begin
  puts "exists: #{z.exists(path).inspect}"
rescue Zookeeper::NoNodeError
  puts "doesn't exist"
  stat = nil
end

puts "create: #{z.create(path, "initial value", 0).inspect}" unless stat

value, stat = z.get(path)
puts "current value #{value}, stat #{stat.inspect}"

puts "set: #{z.set(path, "this is a test", stat.version).inspect}"

value, stat = z.get(path)
puts "new value: #{value.inspect} #{stat.inspect}"

puts "delete: #{z.delete(path, stat.version).inspect}"

begin
  puts "exists? #{z.exists(path)}"
  raise Exception, "it shouldn't exist"
rescue Zookeeper::NoNodeError
  puts "doesn't exist - good, because we just deleted it!"
end

