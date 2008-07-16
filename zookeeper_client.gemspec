Gem::Specification.new do |s|
  s.name = "zookeeper_client"
  s.version = "0.0.2"
  s.date = "2008-07-16"
  s.summary = "Ruby wrapper for the ZooKeeper C client library"
  s.email = "pp@myelin.co.nz"
  s.homepage = "http://github.com/myelin/zookeeper_client"
  s.description = "zookeeper_client is a Ruby library to interface with the ZooKeeper replicated object store / lock server."
  s.has_rdoc = false
  s.authors = ["Phillip Pearson"]
  s.extensions = ["ext/zookeeper_c/extconf.rb"]
  s.files = ["lib/zookeeper_client.rb", "ext/zookeeper_c/zookeeper_ruby.c"]
  s.test_files = ["test/test_basic.rb"]
  #s.rdoc_options = ["--main", "README.txt"]
  #s.extra_rdoc_files = ["History.txt", "Manifest.txt", "README.txt"]
  #s.add_dependency("mime-types", ["> 0.0.0"])
end
