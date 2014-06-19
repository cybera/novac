require 'novadb'
require 'projects'
require 'parallel'

# required libs
required_libs = ['mysql2', 'parallel']
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
      'images'                      => 5,
      'object_mb'                   => 204800,
    }
  end

  # Returns a hash of the project's quota limits
  # Including defaults
  # Used when Horizon needs to obtain a user's resource limit
  def project_quota(project_id)
    begin
      # Get the master cloud
      master = @novadb.master_cloud

      # Connect to the nova database on the master cloud's db
      nova = Mysql2::Client.new( :host => master[:server], :username => master[:username], :password => master[:password], :database => 'nova' )
      cinder = Mysql2::Client.new( :host => master[:server], :username => master[:username], :password => master[:password], :database => 'cinder' )
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

      # Return the quota
      quota

    ensure
      nova.close if nova
      cinder.close if cinder
    end
  end


  # Returns a hash of the project's quota limits
  # But no defaults
  # Used to determine what values need syncd to another region.
  # Default values shouldn't be syncd
  def project_quota_limits(project_id)
    quota = {}
    begin
      # Get the master cloud
      master = @novadb.master_cloud

      # Connect to the nova database on the master cloud's db
      nova = Mysql2::Client.new( :host => master[:server], :username => master[:username], :password => master[:password], :database => 'nova' )
      cinder = Mysql2::Client.new( :host => master[:server], :username => master[:username], :password => master[:password], :database => 'cinder' )

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
    ensure
      nova.close if nova
      cinder.close if cinder
    end
  end


  # Return the resources used.
  # No manual calculations -- might be out of sync until next balance
  # Used by Horizon for various functions. Called by novac.
  def get_used_resources(project_id)
    resources = {}
    begin
      # Get the master cloud
      master = @novadb.master_cloud

      # Connect to the nova database on the master cloud's db
      nova = Mysql2::Client.new( :host => master[:server], :username => master[:username], :password => master[:password], :database => 'nova' )
      cinder = Mysql2::Client.new( :host => master[:server], :username => master[:username], :password => master[:password], :database => 'cinder' )
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

      # Return the quota
      return resources
    ensure
      nova.close if nova
      cinder.close if cinder
    end
  end


  # Manually calculates the resources that a project has used
  def calculate_used(project_id)
    resources = {}

    # Loop through all clouds
    @novadb.clouds.each do |region, creds|
      begin
        nova = Mysql2::Client.new( :host => creds[:server], :username => creds[:username], :password => creds[:password], :database => 'nova' )
        cinder = Mysql2::Client.new( :host => creds[:server], :username => creds[:username], :password => creds[:password], :database => 'cinder' )
        glance = Mysql2::Client.new( :host => creds[:server], :username => creds[:username], :password => creds[:password], :database => 'glance' )

        # These queries are used to manually calculate the resources
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
          },
          :image_count => {
            :query => "select count(*) as images from images
              where owner = '#{project_id}' and (status != 'deleted' and status != 'killed')",
            :database => glance,
          },
          :object_mb_usage => {
            :query => "select in_use as object_mb from quota_usages
              where project_id = '#{project_id}' and resource = 'swift_regional_object_usage'",
            :database => nova,
          }
        }

        # Perform all queries to do a manual inventory
        queries.each do |query, query_info|
          q = query_info[:query]
          database = query_info[:database]
          rs = database.query q
          usage = rs.first
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
        end
      rescue
        puts "#{region} failed"
        next
      ensure
        nova.close if nova
        cinder.close if cinder
        glance.close if glance
      end
    end
    resources
  end


  # Manually calculates all resources used by all projects
  # Calls the above function for each project that exists.
  def all_projects_used
    total_usage = {}
    projects = Projects.new

    # Loop through each project
    # And do some wacky data processing due to Parallel
    x = Parallel.map(projects.project_ids, :in_process => 5) do |project_id|
      { project_id => calculate_used(project_id) }
    end
    x.each do |y|
      total_usage.merge!(y)
    end
    total_usage
  end


  # Shortcut function rather than going through the whole "used" function
  def object_storage_usage(project_id)
    resources = {}
    begin
      # Get the master cloud
      master = @novadb.master_cloud

      # Connect to the nova database on the master cloud's db
      nova = Mysql2::Client.new( :host => master[:server], :username => master[:username], :password => master[:password], :database => 'nova' )

      # These queries are used to manually calculate the resources
      query = "select in_use as object_mb from quota_usages
              where project_id = '#{project_id}' and resource = 'object_mb'"

      rs = nova.query query
      usage = rs.first
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
    ensure
      nova.close if nova
    end
  end


  # Balance quotas in each region for a single project
  # Used by quota-daemon
  def balance_quotas(project_id)
    clouds = @novadb.clouds
    total_usage = {}
    total_usage[project_id] = calculate_used(project_id)
    sync_all_used(total_usage)
  end


  # Runs balance_quotas for all projects
  # Called by cron and/or novac
  def balance_all_quotas
    projects = Projects.new
    total_usage = all_projects_used()
    sync_all_used(total_usage)
  end


  # Sets the non-default limits for a project in all regions
  # Not actually used right now.
  # Probably used to be.
  def sync_project_limits(project_id)
    clouds = @novadb.clouds
    limits = project_quota_limits(project_id)
    clouds.each do |region, creds|
      limits.each do |resource, limit|
        set_project_quota_limit(project_id, region, resource, limit)
      end
    end
  end

  # Sets a non-default limit for a certain resource for a single project in a single region
  def set_project_quota_limit(project_id, region, resource, limit)
    cloud = @novadb.clouds[region]
    begin
      nova = Mysql2::Client.new( :host => cloud[:server], :username => cloud[:username], :password => cloud[:password], :database => 'nova' )
      cinder = Mysql2::Client.new( :host => cloud[:server], :username => cloud[:username], :password => cloud[:password], :database => 'cinder' )

      # Since Mysql2 doesn't support prepared statement - create full fledged statements first. Creates extra ifs.
      update = "update quotas set hard_limit = '#{limit}' where resource = '#{resource}' and project_id = '#{project_id}'"
      insert = "insert into quotas (created_at, updated_at, deleted, project_id, resource, hard_limit) VALUES (now(), now(), 0, '#{project_id}', '#{resource}', '#{limit}')"

      if resource == 'volumes' or resource == 'gigabytes'
        quota_rs = cinder.query "select count(*) as c from quotas where project_id = '#{project_id}' and resource = '#{resource}'"
      else
        quota_rs = nova.query "select count(*) as c from quotas where project_id = '#{project_id}' and resource = '#{resource}'"
      end

      count = quota_rs.first
      if count['c'].to_i == 1
        if resource == 'volumes' or resource == 'gigabytes'
          cinder.query update
        else
          nova.query update
        end
      elsif count['c'].to_i == 0
        if resource == 'volumes' or resource == 'gigabytes'
          cinder.query insert
        else
          nova.query insert
        end
      else
        throw "Unable to update default #{resource} to #{limit}. #{project_id} has #{count['c']} entries for #{resource}"
      end
    ensure
      nova.close if nova
      cinder.close if cinder
    end
  end


  # Runs set_project_quota_limit (above) in all regions
  def set_project_quota_limit_in_all_regions(project_id, resource, limit)
    clouds = @novadb.clouds
    clouds.each do |region, creds|
      set_project_quota_limit(project_id, region, resource, limit)
    end
  end


  # Sets non-default limits for all projects in all regions
  # Runs set_project_quota_limit (above) for all projects in all regions
  def sync_all_limits
    # Build a list of limits
    tmp_project_limits = {}
    projects = Projects.new
    projects.project_ids.each do |project_id|
      tmp_project_limits[project_id] = {}
      limits = project_quota_limits(project_id)
      limits.each do |resource, limit|
        tmp_project_limits[project_id][resource] = limit
      end
    end

    clouds = @novadb.clouds
    master = @novadb.master_cloud
    clouds.each do |region, creds|
      cloud = @novadb.clouds[region]
      # Skip the master cloud as that's where the data was pulled from
      next if cloud[:server] == master[:server]
      # Loop through all projects 5 at a time
      Parallel.each(tmp_project_limits.keys, :in_process => 5) do |project_id|
        tmp_project_limits[project_id].each do |resource, limit|
          limit = tmp_project_limits[project_id][resource]
          #puts "#{project_id} #{region} sync #{resource} #{limit}"
          set_project_quota_limit(project_id, region, resource, limit)
        end
      end
    end
  end


  # Used by other functions in this file
  # Calls the below function for each project, region, and resource
  def sync_all_used(total_usage)
    clouds = @novadb.clouds
    clouds.each do |region, creds|
      Parallel.each(total_usage.keys, :in_process => 5) do |project_id|
        total_usage[project_id].each do |resource, in_use|
          set_used(project_id, region, resource, in_use)
        end
      end
    end
  end


  # Sets a project's resource usage in a region
  def set_used(project_id, region, resource, in_use)
    cloud = @novadb.clouds[region]
    begin
      nova = Mysql2::Client.new( :host => cloud[:server], :username => cloud[:username], :password => cloud[:password], :database => 'nova' )
      cinder = Mysql2::Client.new( :host => cloud[:server], :username => cloud[:username], :password => cloud[:password], :database => 'cinder' )

      # Query templates
      update = "update quota_usages set in_use = '#{in_use}' where resource = '#{resource}' and project_id = '#{project_id}'"
      insert = "insert into quota_usages (created_at, updated_at, project_id, resource, in_use, deleted, reserved) VALUES (now(),now(),'#{project_id}','#{resource}','#{in_use}',0,0)"

      if resource == 'volumes' or resource == 'gigabytes'
        quota_rs = cinder.query "select count(*) as c from quota_usages where project_id = '#{project_id}' and resource = '#{resource}'"
      else
        quota_rs = nova.query "select count(*) as c from quota_usages where project_id = '#{project_id}' and resource = '#{resource}'"
      end

      count = quota_rs.first
      if count['c'].to_i == 1
        if resource == 'volumes' or resource == 'gigabytes'
          cinder.query update
        else
          nova.query update
        end
      elsif count['c'].to_i == 0
        unless in_use == 0
          if resource == 'volumes' or resource == 'gigabytes'
            cinder.query insert
          else
            nova.query insert
          end
        end
      else
        throw "Unable to update #{resource} to #{in_use}. #{project_id} has #{count['c']} entries for #{resource}"
      end
    ensure
      nova.close if nova
      cinder.close if cinder
    end
  end

end
