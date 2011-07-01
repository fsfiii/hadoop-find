#!/usr/bin/env jruby

require 'java'
require 'getoptlong'

class HDFSFinder
  def initialize uri, opts = {}
    @opts = opts
    @conf = org.apache.hadoop.conf.Configuration.new
    @uri  = java.net.URI.create uri
    @path = org.apache.hadoop.fs.Path.new @uri
    @fs   = org.apache.hadoop.fs.FileSystem.get @uri, @conf
  end

  # filter by size using unix find -size numbering scheme
  def filter_size size
    s = @opts[:size]
    cmp = :== 
    case s[0].chr
    when '-'
      cmp = :<
    when '+'
      cmp = :>
    end

    multi = 1
    case s[-1].chr.upcase
    when 'K'
      multi = 1024
    when 'M'
      multi = 1024 * 1024
    when 'G'
      multi = 1024 * 1024 * 1024
    when 'T'
      multi = 1024 * 1024 * 1024 * 1024
    when 'P'
      multi = 1024 * 1024 * 1024 * 1024 * 1024
    end
    filter_size = s.to_i.abs * multi

    return size.send(cmp, filter_size)
  end

  # print out one line of info for a filestatus object
  def display f
    size = f.len
    return if @opts[:size] and not filter_size size

    if @opts[:uri]
      path = f.path.to_s
    else
      path = f.path.to_uri.path
    end

    if not @opts[:ls]
      puts path
      return
    end

    type = f.dir? ? 'd' : '-'
    repl = f.replication > 0 ? f.replication : '-'
    mtime = Time.at(f.modification_time / 1000).strftime '%Y-%m-%d %H:%M:%S'
    perm = f.permission.to_s.strip
    puts '%s%s %s %-8s %-16s %12s %s %s' %
      [type, perm, repl, f.owner, f.group, size, mtime, path]
  end

  def find
    @fs.glob_status(@path).each {|s| walk(s) {|f| display f}}
  end

  def walk fstatus
    yield fstatus

    return if not fstatus.dir?

    @fs.list_status(fstatus.path).each {|s| walk(s) {|f| display f}}
  end
end

def usage
  puts <<-EOF
usage: #$0 [options] path
  -h, --help
  -a, --after       # display files modified after ISO date
  -b, --before      # display files modified before ISO date
  -s, --size        # display files greater (+val) or less than (-val) size
  -l, --ls          # display full listing detail
  -u, --uri         # display full uri for path
EOF
end

# main

opts = {}

gopts = GetoptLong.new(
  [ '--size',   '-s', GetoptLong::REQUIRED_ARGUMENT ],
  [ '--after',  '-a', GetoptLong::REQUIRED_ARGUMENT ],
  [ '--before', '-b', GetoptLong::REQUIRED_ARGUMENT ],
  [ '--ls',     '-l', GetoptLong::NO_ARGUMENT ],
  [ '--uri',    '-u', GetoptLong::NO_ARGUMENT ],
  [ '--help',   '-h', GetoptLong::NO_ARGUMENT ]
)

gopts.each do |opt, arg|
  case opt
  when '--after'
    opts[:after] = arg
  when '--before'
    opts[:before] = arg
  when '--size'
    opts[:size] = arg    
  when '--ls'
    opts[:ls] = true    
  when '--uri'
    opts[:uri] = true    
  else
    usage
  end
end

uri = ARGV[0] or (usage ; exit 1)

hf = HDFSFinder.new uri, opts
hf.find
