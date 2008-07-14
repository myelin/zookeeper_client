require 'zookeeper'

z = Zookeeper.new("localhost:2181")

puts z.ls("/").inspect

puts "value of foo: #{z.get("/foo").inspect}"

z.set("/foo", "this is a test", 42)

puts "new value of foo: #{z.get("/foo")}"
