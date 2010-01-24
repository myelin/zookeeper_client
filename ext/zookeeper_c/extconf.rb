require 'mkmf'

dir_config("zookeeper")

have_header "sys/errno.h" 
have_header "stdio.h"
find_header("zookeeper/zookeeper.h") or
  raise "zookeeper.h not found."
find_library("zookeeper_mt", "zoo_set_debug_level") or 
  raise "libzookeeper_mt not found."
  
dir_config('zookeeper_c')
create_makefile( 'zookeeper_c')

