require 'novadb'
require 'projects'

# required libs
required_libs = ['mysql2']
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
      'instances'                   => 8,
      'cores'                       => 8,
      'ram'                         => 8 * 1024,
      'volumes'                     => 10,
      'gigabytes'                   => 500,
      'floating_ips'                => 1,
      'fixed_ips'                   => -1,
      'metadata_items'              => 128,
      'injected_files'              => 5,
      'injected_file_content_bytes' => 10 * 1024,
      'injected_file_path_bytes'    => 255,
      'security_groups'             => 10,
      'security_group_rules'        => 20,
      'key_pairs'                   => 100,
      'reservation_expire'          => 86400,
      'images'                      => 5,
      'object_mb'                   => 8192,
    }

    @cloud = @novadb.cloud
    @regions = {}
    @master_region = @novadb.master_region
    @novadb.regions.each do |region|
      @regions[region] = {}
    end

  end

  def _connect_to_dbs
    @regions.keys.each do |region|
      unless @regions[region].has_key?('nova')
        @regions[region]['nova'] = Mysql2::Client.new(:host => @cloud[:server], :username => @cloud[:username], :password => @cloud[:password], :database => "nova_#{region}", :reconnect => true)
      end
      unless @regions[region].has_key?('cinder')
        @regions[region]['cinder'] = Mysql2::Client.new(:host => @cloud[:server], :username => @cloud[:username], :password => @cloud[:password], :database => "cinder_#{region}", :reconnect => true)
      end
      unless @regions[region].has_key?('glance')
        @regions[region]['glance'] = Mysql2::Client.new(:host => @cloud[:server], :username => @cloud[:username], :password => @cloud[:password], :database => 'glance', :reconnect => true)
      end
    end
  end

  def _disconnect_from_dbs
    @regions.keys.each do |region|
      @regions[region]['nova'].close
      @regions[region].delete('nova')
      @regions[region]['cinder'].close
      @regions[region].delete('cinder')
      @regions[region]['glance'].close
      @regions[region].delete('glance')
    end
  end

  # Returns a hash of the project's quota limits
  # Including defaults
  def project_quota(project_id)
    # Connect to the master nova database
    nova = @regions[@master_region]['nova']
    cinder = @regions[@master_region]['cinder']
    quota = @defaults.clone

    # Query for the quota for the certain project
    # All but Volumes / Block Storage
    quota_rs = nova.query "select resource, hard_limit from quotas where project_id = '#{project_id}'"

    # Build a quota that is a combination of default + project quota
    quota_rs.each do |row|
      quota[row['resource']] = row['hard_limit']
    end

    # Query for Volume / Block Storage quotas
    quota_rs = cinder.query "select resource, hard_limit from quotas where project_id = '#{project_id}'"

    quota_rs.each do |row|
      quota[row['resource']] = row['hard_limit']
    end

    _disconnect_from_dbs

    # Return the quota
    quota

  end

  # Returns a hash of the project's quota limits
  # But no defaults
  def project_quota_limits(project_id)
    quota = {}
    # Connect to the master nova database
    nova = @regions[@master_region]['nova']
    cinder = @regions[@master_region]['cinder']

    # Query for the non-default quota items in the project
    # All but Volumes / Block Storage
    quota_rs = nova.query "select resource, hard_limit from quotas where project_id = '#{project_id}'"

    quota_rs.each do |row|
      quota[row['resource']] = row['hard_limit']
    end

    # Query for Volume / Block Storage quotas
    quota_rs = cinder.query "select resource, hard_limit from quotas where project_id = '#{project_id}'"

    quota_rs.each do |row|
      quota[row['resource']] = row['hard_limit']
    end

    quota
  end

  # Return the resources used. No manual calculations -- might be out of sync until next balance
  # This is for Horizon because Grizzly manually calculates the usages instead of looking at quota_usages
  # and so it doesn't account for multiple regions.
  def get_used_resources(project_id)
    _connect_to_dbs
    resources = {}

    # Connect to the nova database on the master cloud's db
    nova = @regions[@master_region]['nova']
    cinder = @regions[@master_region]['cinder']
    quota = @defaults.clone

    # Query for the quota for the certain project
    # For all but Block Storage
    quota_rs = nova.query "select resource, in_use from quota_usages where project_id = '#{project_id}'"

    # Build a quota that is a combination of default + project quota
    quota_rs.each do |row|
      resources[row['resource']] = row['in_use']
    end

    # Query for Volume / Block Storage quotas
    quota_rs = cinder.query "select resource, in_use from quota_usages where project_id = '#{project_id}'"

    quota_rs.each do |row|
      resources[row['resource']] = row['in_use']
    end

    _disconnect_from_dbs

    # Return the quota
    return resources
  end

  # Manually calculates the resources that a user has used in a project
  def user_project_used(user_id, project_id)
    resources = {}
    @regions.each do |region, dbs|
      # These queries are used to manually calculate the resources
      # These resources have a user_id attached to the resource
      # and so must be handled differently than other resources
      queries = {
        :instance_count => {
          :query => "select count(*) as instances from instances
            where project_id = '#{project_id}' and user_id = '#{user_id}' and deleted = 0",
          :database => dbs['nova'],
        },
        :instance_usage_info => {
          :query => "select sum(memory_mb) ram, sum(vcpus) as cores from instances
            where project_id = '#{project_id}' and user_id = '#{user_id}' and deleted = 0",
          :database => dbs['nova'],
        },
      }

      # Perform all queries to do a manual inventory
      queries.each do |query, query_info|
        q = query_info[:query]
        database = query_info[:database]
        rs = database.query q
        if rs
          rs.each do |row|
            row.each do |column, value|
              next if value.to_i < 0
              if resources.has_key?(column)
                resources[column] += value.to_i
              else
                resources[column] = value.to_i
              end
            end
          end
        end
      end
    end
    resources
  end

  # Manually calculates the resources that a project has used
  def project_used(project_id)
    resources = {}

    # Loop through all clouds
    @regions.each do |region, dbs|
      # These queries are used to manually calculate the resources
      queries = {
        :floating_ip_count => {
          :query => "select count(*) as floating_ips from floating_ips
            where project_id = '#{project_id}'",
          :database => dbs['nova'],
        },
        :volume_count => {
          :query => "select count(*) as volumes from volumes
            where project_id = '#{project_id}' and deleted = 0",
          :database => dbs['cinder'],
        },
        :volume_usage_info => {
          :query => "select sum(size) as gigabytes from volumes
            where project_id = '#{project_id}' and deleted = 0",
          :database => dbs['cinder'],
        },
        :image_count => {
          :query => "select count(*) as images from images
            where owner = '#{project_id}' and (status != 'deleted' and status != 'killed')",
          :database => dbs['glance'],
        },
        :object_mb_usage => {
          :query => "select in_use as object_mb from quota_usages
            where project_id = '#{project_id}' and resource = 'swift_regional_object_usage'",
          :database => dbs['nova'],
        }
      }

      # Perform all queries to do a manual inventory
      queries.each do |query, query_info|
        q = query_info[:query]
        database = query_info[:database]
        rs = database.query q
        if rs
          rs.each do |row|
            row.each do |column, value|
              next if value.to_i < 0
              if resources.has_key?(column)
                resources[column] += value.to_i
              else
                resources[column] = value.to_i
              end
            end
          end
        end
      end
    end
    resources
  end

  def get_project_usage(project_id)
    project_usage = {}
    project_usage[project_id] = {}
    project_usage[project_id]['global'] = {}
    project_usage[project_id]['users'] = {}

    project_used(project_id).each do |resource, value|
      if project_usage[project_id]['global'].has_key?(resource)
        project_usage[project_id]['global'][resource] += value
      else
        project_usage[project_id]['global'][resource] = value
      end
    end

    projects = Projects.new
    projects.users(project_id).each do |user_id, user_name|
      project_usage[project_id]['users'][user_id] = {}
      user_project_used(user_id, project_id).each do |resource, value|
        if project_usage[project_id]['users'][user_id].has_key?(resource)
          project_usage[project_id]['users'][user_id][resource] += value
        else
          project_usage[project_id]['users'][user_id][resource] = value
        end
      end
    end

    project_usage[project_id]

  end

  # Manually calculates all resources used by all projects
  def get_all_projects_usage
    total_usage = {}
    projects = Projects.new

    # Loop through each project
    projects.project_ids.each do |project_id|
      total_usage[project_id] = get_project_usage(project_id)
    end

    total_usage
  end

  # Balance quotas in each region for a single project
  def balance_quotas(project_id)
    _connect_to_dbs
    total_usage = {}
    total_usage[project_id] = get_project_usage(project_id)
    set_used_resources(total_usage)
    _disconnect_from_dbs
  end

  # Runs balance_quotas for each project
  def balance_all_quotas
    _connect_to_dbs
    total_usage = get_all_projects_usage()
    set_used_resources(total_usage)
    _disconnect_from_dbs
  end

  # Sets limits for ALL resources for a project in all regions
  def sync_project_limits(project_id)
    regions = @novadb.regions
    limits = project_quota_limits(project_id)
    regions.each do |region|
      limits.each do |resource, limit|
        set_project_limit(project_id, region, resource, limit)
      end
    end
  end

  # Sets a limit for ONE resource in a project for all regions
  def set_project_limit_in_all_regions(project_id, resource, limit)
    @novadb.regions.each do |region|
      set_project_limit(project_id, region, resource, limit)
    end
  end

  # Sets a non-default limit for a certain resource for a single project in a single region
  def set_project_limit(project_id, region, resource, limit)
    nova = @regions[region]['nova']
    cinder = @regions[region]['cinder']

    quota_rs = "select count(*) as c from quotas where project_id = '#{project_id}' and resource = '#{resource}'"
    update = "update quotas set hard_limit = '#{limit}' where resource = '#{resource}' and project_id = '#{project_id}'"
    insert = "insert into quotas (created_at, updated_at, deleted, project_id, resource, hard_limit) VALUES (now(), now(), 0, '#{project_id}', '#{resource}', '#{limit}')"

    if resource == 'volumes' or resource == 'gigabytes' or resource == 'snapshots'
      db = cinder
    else
      db = nova
    end

    db.query(quota_rs).each do |row|
      count = row['c']
      if count == 1
        db.query(update)
      elsif count == 0
        db.query(insert)
      else
        throw "Unable to update default #{resource} to #{limit}. #{project_id} has #{count} entries for #{resource}"
      end
    end
  end

  # Sets non-default limits for all projects in all regions
  def sync_all_projects_limits
    _connect_to_dbs
    @regions.each do |region, dbs|
      projects = Projects.new
      projects.project_ids.each do |project_id|
        limits = project_quota_limits(project_id)
        limits.each do |resource, limit|
          set_project_limit(project_id, region, resource, limit)
        end
      end
    end
    _disconnect_from_dbs
  end

  def set_used_resources(total_usage)
    @regions.each do |region, dbs|
      nova = dbs['nova']
      cinder = dbs['cinder']

      total_usage.each do |project_id, usage_types|
        usage_types.keys.each do |type|
          if type == 'global'
            usage_types[type].each do |resource, in_use|
              if resource == 'volumes' or resource == 'gigabytes' or resource == 'snapshots'
                db = cinder
              else
                db = nova
              end
              quota = "select count(*) as c from quota_usages where project_id = '#{project_id}' and resource = '#{resource}'"
              update = "update quota_usages set in_use = '#{in_use}' where resource = '#{resource}' and project_id = '#{project_id}'"
              insert = "insert into quota_usages (created_at, updated_at, project_id, resource, in_use, deleted, reserved) VALUES (now(),now(),'#{project_id}','#{resource}','#{in_use}',0,0)"
              set_used_resource(db, quota, update, insert, resource, in_use, project_id)
            end
          elsif type == 'users'
            usage_types[type].each do |user_id, used|
              used.each do |resource, in_use|
                db = nova
                quota = "select count(*) as c from quota_usages where project_id = '#{project_id}' and user_id = '#{user_id}' and resource = '#{resource}'"
                update = "update quota_usages set in_use = '#{in_use}' where resource = '#{resource}' and project_id = '#{project_id}' and user_id = '#{user_id}'"
                insert = "insert into quota_usages (created_at, updated_at, user_id, project_id, resource, in_use, deleted, reserved) VALUES (now(),now(),'#{user_id}','#{project_id}','#{resource}','#{in_use}',0,0)"
                set_used_resource(db, quota, update, insert, resource, in_use, project_id)
              end
            end
          end
        end
      end
    end
  end

  def set_used_resource(db, quota, update, insert, resource, in_use, project_id)
    quota_rs = db.query quota
    quota_rs.each do |row|
      count = row['c']
      if count == 1
      # Update
      #puts update
      db.query update
      elsif count == 0
        unless in_use == 0
          # Insert
          #puts insert
          db.query insert
        end
      else
        throw "Unable to update #{resource} to #{in_use}. #{project_id} has #{count} entries for #{resource}"
      end
    end
  end

  def set_used(project_id, region, resource, in_use)
    nova = @regions[region]['nova']
    cinder = @regions[region]['cinder']

    if resource == 'volumes' or resource == 'gigabytes' or resource == 'snapshots'
      db = cinder
    else
      db = nova
    end

    # Query templates
    quota_rs = "select count(*) as c from quota_usages where project_id = '#{project_id}' and resource = '#{resource}'"
    update = "update quota_usages set in_use = '#{in_use}' where resource = '#{resource}' and project_id = '#{project_id}'"
    insert = "insert into quota_usages (created_at, updated_at, project_id, resource, in_use, deleted, reserved) VALUES (now(),now(),'#{project_id}','#{resource}','#{in_use}',0,0)"

    if resource == 'volumes' or resource == 'gigabytes' or resource == 'snapshots'
      db = cinder
    else
      db = nova
    end

    db.query(quota_rs).each do |row|
      count = row['c']
      if count == 1
        # Update
        update.execute in_use, resource, project_id
      elsif count == 0
        unless in_use == 0
          # Insert
          insert.execute project_id, resource, in_use
        end
      else
        throw "Unable to update #{resource} to #{in_use}. #{project_id} has #{count} entries for #{resource}"
      end
    end
  end

  # Shortcut function rather than going through the whole "used" function
  def object_storage_usage(project_id)
    resources = {}
    # Get the master cloud
    cloud = @novadb.cloud

    # Connect to the nova database on the master cloud's db
    nova = Mysql.new cloud[:server], cloud[:username], cloud[:password], 'nova_yeg'

    # These queries are used to manually calculate the resources
    query = "select in_use as object_mb from quota_usages
            where project_id = '#{project_id}' and resource = 'object_mb'"

    rs = nova.query query
    usage = rs.fetch_hash
    if usage
      usage.each do |column, value|
        next if value.to_i < 0
        if resources.has_key?(column)
          resources[column] += value.to_i
        else
          resources[column] = value.to_i
        end
      end
    end
    resources
  end
end


