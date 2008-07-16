require 'mkmf'
extension_name = 'zookeeper_c'
dir_config(extension_name)

if have_library("zookeeper_mt", "zoo_set_debug_level") then
  create_makefile(extension_name)
else
  puts "No ZooKeeper C client library available"
end
