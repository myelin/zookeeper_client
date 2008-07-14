require 'zookeeper'

z = Zookeeper.new("localhost:2181")

puts z.ls("/").inspect

path = "/testing_node"

begin
  puts "value of foo: #{z.get(path).inspect}"
rescue Zookeeper::NoNodeError
  puts "path #{path} doesn't exist"
  z.create(path, "initial value", 0)
  puts "created; new value is #{z.get(path).inspect}"
end
value, version = z.get(path)

z.set(path, "this is a test", version)

puts "new value of foo: #{z.get(path).inspect}"
