# Loads mkmf which is used to make makefiles for Ruby extensions
require 'mkmf'

# Give it a name
extension_name = 'c_zookeeper'

# The destination
dir_config(extension_name)

# Do the work
if have_library("zookeeper_mt", "zoo_set_debug_level") then
  create_makefile(extension_name)
else
  puts "No Zookeeper C client library available"
end

