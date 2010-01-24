require 'mkmf'

have_header "sys/errno.h" 
have_header "stdio.h"
find_header("zookeeper/zookeeper.h", dir_config("zookeeper").first || ENV['INCLUDE']) or
  raise "zookeeper.h not found. Try 'gem install zookeeper -- --with-zookeeper-dir=/SOMEWHERE'"
  
dir_config('zookeeper_c')
create_makefile( 'zookeeper_c')

