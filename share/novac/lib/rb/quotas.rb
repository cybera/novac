require 'novadb'
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
      'volume_gigabytes'            => 1000
    }
  end

  def project_quota(project_id)
    begin
      # Get the master cloud
      master = @novadb.master_cloud

      # Connect to the nova table on the master cloud's db
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

  def _count_usages(resources, query)
    # Loop through used resources
    query.each_hash do |row|
      sum = row['sum'].to_i
      # Combine resources used across all clouds
      if sum > 0
        if resources.has_key?(row['resource'])
          resources[row['resource']] += sum
        else
          resources[row['resource']] = sum
        end
      end
    end
    resources
  end


  def used(project_id)
    resources = {}
    # Loop through all clouds
    @novadb.clouds.each do |region, creds|
      begin
        # Connect to nova database of the current cloud
        nova = Mysql.new creds[:server], creds[:username], creds[:password], 'nova'
        cinder = Mysql.new creds[:server], creds[:username], creds[:password], 'cinder'

        # Query the quota_usages table for all resources used by the project
        query = nova.query "select sum(in_use) as sum, resource from quota_usages where project_id = '#{project_id}' group by resource"
        resources = _count_usages(resources, query)

        query = cinder.query "select sum(in_use) as sum, resource from quota_usages where project_id = '#{project_id}' group by resource"
        resources = _count_usages(resources, query)

      ensure
        nova.close if nova
      end
    end
    resources
  end

  def set_used(project_id, region, resource, in_use)
    cloud = @novadb.clouds[region]
    begin
      nova = Mysql.new cloud[:server], cloud[:username], cloud[:password], 'nova'

      # Update statement
      update = nova.prepare "update quota_usages set in_use = ? where resource = ?"
      insert = nova.prepare "insert into quota_usages (created_at, updated_at, project_id, resource, in_use, deleted, reserved) VALUES (now(),now(),?,?,?,0,0)"

      quota_rs = nova.query "select count(*) as c from quotas_usages where project_id = '#{project_id}'"
      count = quota_rs.fetch_hash
      if count['c'].to_i == 1
        update.execute in_use, resource
      elsif count['c'] == 0
        insert.execute project_id, resource, in_use
      else
        throw "Unable to update #{resource} to #{in_use}. #{project_id} has #{count['c']} entries for #{resource}"
      end
    rescue Mysql::Error => e
      puts e
    ensure
      update.close if update
      insert.close if insert
      nova.close if nova
    end
  end
end
