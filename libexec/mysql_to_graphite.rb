#!/usr/bin/env ruby

# Usage: novac cloud-instance-metrics-collectd
# Summary: Adds instance usage to collectd for the past hour
# Help: Adds instance usage to collectd for the past hour

$:.unshift File.expand_path("../../share/novac/lib/rb", __FILE__)
require 'rubygems'
require 'mysql2'
require 'novadb2'
require 'pp'

location = %x{ facter location }.chomp
novadb = NovaDB2.instance
db = novadb.clouds[location]['mysql']['nova']
itop = {}

now = Time.now
now = Time.new(now.year, now.month, now.day, now.hour, 0, 0).getgm.to_i
past_hour = now - (3600 * 1)

queries = {
  'cpu'       => "select * from cpu limit 10", #where timestamp #< #{now} and timestamp > #{past_hour}",
  'memory'    => "select * from memory limit 10", #where timestamp < #{now} and timestamp > #{past_hour}",
  'disk'      => "select * from disk limit 10", #where timestamp < #{now} and timestamp > #{past_hour}",
  'interface' => "select * from interface limit 10", #where timestamp < #{now} and timestamp > #{past_hour}",
}

instance_metrics = Mysql2::Client.new( :host => db[:host], :username => db[:user], :password => db[:password], :database => 'instance_metrics' )

queries.each do |metric, query|
  query_rs = instance_metrics.query query
  query_rs.each do |row|
    if itop.key?(row['uuid'])
      project_id = itop[row['uuid']]
    else
      project_query = "select project_id from nova.instances where uuid = '#{row['uuid']}'"
      project_query_rs = instance_metrics.query project_query
      if project_query_rs.count == 1
        project_id = project_query_rs.first['project_id']
      else
        next
      end
      itop[row['uuid']] = project_id
    end

    timestamp = row.delete('timestamp')
    uuid = row.delete('uuid')
    row_id = row.delete('id')

    submetric = ""
    if metric == 'interface' or metric == 'disk'
      submetric = ".#{row[metric]}"
      row.delete(metric)
      if metric == 'interface'
        row['rx_total_bytes'] = row['rx_bytes']
        row['rx_total_packets'] = row['rx_packets']
        row['tx_total_bytes'] = row['tx_bytes']
        row['tx_total_packets'] = row['tx_packets']
      end
    end

    values = []
    row.keys.sort.each do |k|
      values << row[k]
    end

    row.each do |k, v|
      puts "prod.projects.#{project_id}.instances.#{uuid}.#{metric}#{submetric}.#{k} #{v} #{timestamp}"
    end
  end
end