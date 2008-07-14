require 'mkmf'
extension_name = 'c_zookeeper'
dir_config(extension_name)

if have_library("zookeeper_mt", "zoo_set_debug_level") then
  create_makefile(extension_name)
else
  puts "No Zookeeper C client library available"
end
