Gem::Specification.new do |s|
  s.name = 'hadoop-find'
  s.rubyforge_project = 'hadoop-find'
  s.platform = 'java'
  s.version = '0.0.1'
  s.date = '2011-07-02'
  s.authors = ["Frank Fejes"]
  s.email = 'frank@fejes.net'
  s.summary =
    'jruby file listing utility for HDFS filesystems similar to unix find(1).'
  s.homepage = 'https://github.com/fsfiii/hadoop-find'
  s.description =
    "A file listing utility for HDFS filesystems similar to unix find(1).\n" +
    'Requires jruby 1.6+.'
  s.files = [
    "README",
    "CHANGELOG",
    "bin/hfind",
    "bin/hfind.rb",
  ]
  s.executables = [
    "hfind",
    "hfind.rb",
  ]
  s.post_install_message = <<EOF
===
  Please be sure to install with:

  jgem install --no-wrapper hadoop-find
===
EOF
end
