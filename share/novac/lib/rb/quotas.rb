require 'novadb'
require 'projects'

# required libs
required_libs = ['mysql']
begin
  required_libs.each { |l| require l}
rescue LoadError
  puts "This script needs the following external libraries: "
  required_libs.each { |l| puts " * #{l}" }
end

class Quotas

  attr_accessor :defaults

  def initialize
    @novadb = NovaDB.new
    @defaults = {
      'instances'                   => 10,
      'cores'                       => 20,
      'ram'                         => 50 * 1024,
      'volumes'                     => 10,
      'gigabytes'                   => 1000,
      'floating_ips'                => 10,
      'metadata_items'              => 128,
      'injected_files'              => 5,
      'injected_file_content_bytes' => 10 * 1024,
      'injected_file_path_bytes'    => 255,
      'security_groups'             => 10,
      'security_group_rules'        => 20,
      'key_pairs'                   => 100,
      'reservation_expire'          => 86400,
    }
  end

  def project_quota(project_id)
    begin
      # Get the master cloud
      master = @novadb.master_cloud

      # Connect to the nova database on the master cloud's db
      nova = Mysql.new master[:server], master[:username], master[:password], 'nova'
      quota = @defaults.clone

      # Query for the quota for the certain project
      quota_rs = nova.query "select resource, hard_limit from quotas where project_id = '#{project_id}'"

      # Build a quota that is a combination of default + project quota
      quota_rs.each_hash do |row|
        quota[row['resource']] = row['hard_limit']
      end

      # Return the quota
      quota

    ensure
      nova.close if nova
    end
  end

  def project_quota_limits(project_id)
    quota = {}
    begin
      # Get the master cloud
      master = @novadb.master_cloud

      # Connect to the nova database on the master cloud's db
      nova = Mysql.new master[:server], master[:username], master[:password], 'nova'
      
      # Query for the non-default quota items in the project
      quota_rs = nova.query "select resource, hard_limit from quotas where project_id = '#{project_id}'"

      quota_rs.each_hash do |row|
        quota[row['resource']] = row['hard_limit']
      end

      quota
    ensure
      nova.close if nova
    end
  end

  def used(project_id)
    resources = {}
    # Loop through all clouds
    @novadb.clouds.each do |region, creds|
      begin
        # Connect to nova database of the current cloud
        nova = Mysql.new creds[:server], creds[:username], creds[:password], 'nova'
        cinder = Mysql.new creds[:server], creds[:username], creds[:password], 'cinder'

        # Piece together used stuff
        queries = {
          :instance_count => {
            :query => "select count(*) as instances from instances 
              where project_id = '#{project_id}' and deleted = 0",
            :database => nova
          },
          :instance_usage_info => {
            :query => "select sum(memory_mb) ram, sum(vcpus) as cores from instances 
              where project_id = '#{project_id}' and deleted = 0",
            :database => nova,
          },
          :floating_ip_count => {
            :query => "select count(*) as floating_ips from floating_ips 
              where project_id = '#{project_id}'",
            :database => nova,
          },
          :volume_count => {
            :query => "select count(*) as volumes from volumes 
              where project_id = '#{project_id}' and deleted = 0",
            :database => cinder,
          },
          :volume_usage_info => {
            :query => "select sum(size) as gigabytes from volumes 
              where project_id = '#{project_id}' and deleted = 0",
            :database => cinder,
          }
        }
        
        # Perform all queries to do a manual inventory
        queries.each do |query, query_info|
          q = query_info[:query]
          database = query_info[:database]
          rs = database.query q
          rs.fetch_hash.each do |column, value|
            next if value.to_i < 0
            if resources.has_key?(column)
              resources[column] += value.to_i
            else
              resources[column] = value.to_i
            end
          end
        end
      ensure
        nova.close if nova
      end
    end
    resources
  end

  def balance_quotas(project_id)
    clouds = @novadb.clouds
    used = used(project_id)
    clouds.each do |region, creds|
      used.each do |resource, in_use|
        set_used(project_id, region, resource, in_use)
      end
    end
  end

  def balance_all_quotas
    projects = Projects.new
    projects.project_ids.each do |project_id|
      balance_quotas(project_id)
    end
  end

  def sync_limits(project_id)
    clouds = @novadb.clouds
    limits = project_quota_limits(project_id)
    clouds.each do |region, creds|
      limits.each do |resource, limit|
        set_project_quota_limits(project_id, region, resource, limit)
      end
    end
  end

  def sync_all_limits
    projects = Projects.new
    projects.project_ids.each do |project_id|
      sync_limits(project_id)
    end
  end

  def set_project_quota_limits(project_id, region, resource, limit)
    cloud = @novadb.clouds[region]
    begin
      nova = Mysql.new cloud[:server], cloud[:username], cloud[:password], 'nova'

      # Update statement
      update_query = "update quotas set hard_limit = ? where resource = ? and project_id = ?"
      update = nova.prepare update_query

      # Insert statement
      insert_query = "insert into quotas (created_at, updated_at, deleted, project_id, resource, hard_limit) VALUES (now(), now(), 0, ?, ?, ?)"
      insert = nova.prepare insert_query

      quota_rs = nova.query "select count(*) as c from quotas where project_id = '#{project_id}' and resource = '#{resource}'"
      count = quota_rs.fetch_hash
      if count['c'].to_i == 1
        update.execute limit, resource, project_id
      elsif count['c'].to_i == 0
        insert.execute project_id, resource, limit
      else
        throw "Unable to update default #{resource} to #{limit}. #{project_id} has #{count['c']} entries for #{resource}"
      end
    ensure
      update.close if update
      insert.close if insert
      nova.close if nova
    end
  end
  
  def set_used(project_id, region, resource, in_use)
    cloud = @novadb.clouds[region]
    begin
      nova = Mysql.new cloud[:server], cloud[:username], cloud[:password], 'nova'

      # Update statement
      update_query = "update quota_usages set in_use = ? where resource = ? and project_id = ?"
      update = nova.prepare update_query

      # Insert statement
      insert_query = "insert into quota_usages (created_at, updated_at, project_id, resource, in_use, deleted, reserved) VALUES (now(),now(),?,?,?,0,0)"
      insert = nova.prepare insert_query

      quota_rs = nova.query "select count(*) as c from quota_usages where project_id = '#{project_id}' and resource = '#{resource}'"
      count = quota_rs.fetch_hash
      if count['c'].to_i == 1
        update.execute in_use, resource, project_id
      elsif count['c'].to_i == 0
        unless in_use == 0
          insert.execute project_id, resource, in_use
        end
      else
        throw "Unable to update #{resource} to #{in_use}. #{project_id} has #{count['c']} entries for #{resource}"
      end
    ensure
      update.close if update
      insert.close if insert
      nova.close if nova
    end
  end
end
