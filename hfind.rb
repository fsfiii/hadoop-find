#!/usr/bin/env jruby

require 'java'
require 'getoptlong'

class HadoopFSFinder
  def initialize uri, opts = {}
    @opts = opts

    @conf = org.apache.hadoop.conf.Configuration.new
    core_site = ENV['HADOOP_HOME'].to_s + '/conf/core-site.xml'
    core_path = org.apache.hadoop.fs.Path.new core_site
    @conf.add_resource core_path
    hdfs_site = ENV['HADOOP_HOME'].to_s + '/conf/hdfs-site.xml'
    hdfs_path = org.apache.hadoop.fs.Path.new hdfs_site
    @conf.add_resource hdfs_path
    # convert . to the user's home directory
    uri.sub! /\A\./, "/user/#{ENV['USER']}"

    @uri  = java.net.URI.create uri
    @path = org.apache.hadoop.fs.Path.new @uri
    @fs   = org.apache.hadoop.fs.FileSystem.get @uri, @conf
  end

  # filter by size using unix find -size numbering scheme
  def filter_size size
    return true if not @opts[:size]

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

  # filter by replication count using unix find -size numbering scheme
  def filter_repl repl
    return true if not @opts[:repl]

    r = @opts[:repl]
    cmp = :== 
    case r[0].chr
    when '-'
      cmp = :<
    when '+'
      cmp = :>
    end

    filter_repl = r.to_i.abs

    return repl.send(cmp, filter_repl)
  end


  def filter_mtime mtime
    mtime_filters = [:before, :after, :mmin, :mtime]
    return true if (mtime_filters & @opts.keys).empty?
    
    dt_regexp = /\A(\d{4})-(\d{2})-(\d{2})/

    if @opts[:before]
      match = dt_regexp.match @opts[:before]
      if match
        m = Time.new(match[1], match[2], match[3]).to_i
      else
        raise 'Invalid Date Representation'
      end
      #puts "#{mtime} vs #{m}"
      if mtime < m
        return true
      else
        return false
      end
    elsif @opts[:after]
      match = dt_regexp.match @opts[:after]
      if match
        m = Time.new(match[1], match[2], match[3]).to_i
      else
        raise 'Invalid Date Representation'
      end
      #puts "#{mtime} vs #{m}"
      if mtime > m
        return true
      else
        return false
      end
    end

    m = 0
    if @opts[:mmin]
      m = @opts[:mmin].to_i * 60
    elsif @opts[:mtime]
      m = @opts[:mtime].to_i * 86400
    end

    cmp = :== 
    if m < 0
      cmp = :>
    elsif m > 0
      cmp = :<
    end

    filter_mtime = Time.now.to_i - m.abs.to_i

    #puts "#{mtime} vs #{filter_mtime} #{m}"
    return mtime.send(cmp, filter_mtime)
  end


  # print out one line of info for a filestatus object
  def display f
    type = f.dir? ? 'd' : 'f'
    return if @opts[:type] and @opts[:type] != type

    size = f.len
    return if not filter_size size

    repl = f.replication
    return if not filter_repl repl

    mtime = Time.at(f.modification_time / 1000).to_i
    return if not filter_mtime mtime

    if @opts[:uri]
      path = f.path.to_s
    else
      path = f.path.to_uri.path
    end

    if not @opts[:ls]
      puts path
      return
    end

    if @opts[:human]
      if size > 1125899906842624
        size = "#{size / 1125899906842624}P"
      elsif size > 1099511627776
        size = "#{size / 1099511627776}T"
      elsif size > 1073741824
        size = "#{size / 1073741824}G"
      elsif size > 1048576
        size = "#{size / 1048576}M"
      elsif size > 1024
        size = "#{size / 1024}K"
      else
        size = "#{size}B"
      end
      size = '%4s' % size
    else
      size = '%12s' % size
    end

    type = f.dir? ? 'd' : '-'
    repl = f.replication > 0 ? f.replication : '-'
    mtime = Time.at(f.modification_time / 1000).strftime '%Y-%m-%d %H:%M:%S'
    perm = f.permission.to_s.strip
    puts '%s%s %s %-8s %-16s %s %s %s' %
      [type, perm, repl, f.owner, f.group, size, mtime, path]
  end

  def find
    @fs.glob_status(@path).each {|s| walk(s) {|f| display f}}
  end

  def walk fstat
    yield fstat

    return if not fstat.dir?

    @fs.list_status(fstat.path).each {|s| walk(s) {|f| display f}}
  end
end

def usage
  puts <<-EOF
usage: hfind [options] path
  -H, --help
  -a, --after       # files modified after ISO date
  -b, --before      # files modified before ISO date
  -m, --mmin        # files modified before (-x) or after (+x) minutes ago
  -M, --mtime       # files modified before (-x) or after (+x) days ago
  -s, --size        # file size > (+x), < (-x), or == (x)
  -r, --repl        # replication factor > (+x), < (-x), or == (x)
  -t, --type        # show type (f)ile or (d)irectory
  -l, --ls          # show full listing detail
  -h, --human       # show human readable file sizes
  -u, --uri         # show full uri for path
EOF
end

# main

opts = {}

gopts = GetoptLong.new(
  [ '--size',   '-s', GetoptLong::REQUIRED_ARGUMENT ],
  [ '--repl',   '-r', GetoptLong::REQUIRED_ARGUMENT ],
  [ '--after',  '-a', GetoptLong::REQUIRED_ARGUMENT ],
  [ '--before', '-b', GetoptLong::REQUIRED_ARGUMENT ],
  [ '--mmin',   '-m', GetoptLong::REQUIRED_ARGUMENT ],
  [ '--mtime',  '-M', GetoptLong::REQUIRED_ARGUMENT ],
  [ '--type',   '-t', GetoptLong::REQUIRED_ARGUMENT ],
  [ '--ls',     '-l', GetoptLong::NO_ARGUMENT ],
  [ '--uri',    '-u', GetoptLong::NO_ARGUMENT ],
  [ '--human',  '-h', GetoptLong::NO_ARGUMENT ],
  [ '--help',   '-H', GetoptLong::NO_ARGUMENT ],
)

gopts.each do |opt, arg|
  case opt
  when '--after'
    opts[:after] = arg
  when '--before'
    opts[:before] = arg
  when '--mmin'
    opts[:mmin] = arg
  when '--mtime'
    opts[:mtime] = arg
  when '--size'
    opts[:size] = arg
  when '--repl'
    opts[:repl] = arg
  when '--type'
    opts[:type] = arg
  when '--human'
    opts[:human] = true
  when '--ls'
    opts[:ls] = true
  when '--uri'
    opts[:uri] = true
  else
    usage
    exit 1
  end
end

uri = ARGV[0] or (usage ; exit 1)

hf = HadoopFSFinder.new uri, opts
hf.find rescue STDERR.puts "error: could not process #{uri}"
