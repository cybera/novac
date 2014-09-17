require 'novadb2'
require 'parallel'
require 'helpers/projects'

class Quotas

  attr_accessor :defaults

  def initialize
    @novadb = NovaDB2.new

    # Get the OpenStack Query Library
    os_release = @novadb.get_openstack_release
    require "openstack/#{os_release}"
    @openstack = Object.const_get(os_release.capitalize).new

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
  def project_quota_with_defaults(project_id)
    quota = @defaults.clone

    # Nova and Cinder Quotas
    @openstack.nova_project_quota(project_id).each do |row|
      quota[row[:resource]] = row[:hard_limit]
    end
    @openstack.cinder_project_quota(project_id).each do |row|
      quota[row[:resource]] = row[:hard_limit]
    end
    quota
  end

  # Returns a hash of the project's quota limits
  # But no defaults
  # Used to determine what values need syncd to another region.
  # Default values shouldn't be syncd
  def project_quota(project_id)
    quota = {}
    ['nova_project_quota', 'cinder_project_quota'].each do |query|
      m = @openstack.method(query)
      m.call(project_id).each do |row|
        quota[row[:resource]] = row[:hard_limit]
      end
    end
    quota
  end


  # Return the resources used.
  # No manual calculations -- might be out of sync until next balance
  # Used by Horizon for various functions. Called by novac.
  def get_used_resources(project_id)
    resources = {}
    # Nova / Compute
    @openstack.nova_quota_usages(project_id).each do |row|
      resources[row[:resource]] = row[:in_use]
    end
    # Cinder / Block Storage
    @openstack.cinder_quota_usages(project_id).each do |row|
      resources[row[:resource]] = row[:in_use]
    end
    resources
  end

  def calculate_user_used(user_id, project_id)
    resources = {}
    queries = [
      'nova_instance_count',
      'nova_compute_usage'
    ]

    # Loop through all clouds
    @novadb.regions.each do |region|
      queries.each do |query|
        m = @openstack.method(query)
        m.call(user_id, project_id, region).each do |row|
          row.each do |column, value|
            value = 0 if not value
            if resources.key?(column)
              resources[column] += value.to_i
            else
              resources[column] = value
            end
          end
        end
      end
    end
    resources
  end

  # Manually calculates the resources that a project has used
  def calculate_project_used(project_id)
    resources = {}

    queries = [
      'nova_floating_ip_count',
      'cinder_volume_count',
      'cinder_volume_usage',
      'glance_image_count',
      'swift_regional_object_mb_usage'
    ]

    # Loop through all clouds
    @novadb.regions.each do |region|
      # If a region was specified, then only calculate for that region
      queries.each do |query|
        m = @openstack.method(query)
        m.call(project_id, region).each do |row|
          row.each do |column, value|
            value = 0 if not value
            if resources.key?(column)
              resources[column] += value.to_i
            else
              resources[column] = value
            end
          end
        end
      end
    end
    resources
  end

  # Get total usage for both users and projects
  def get_project_usage(project_id, region = nil)
    project_usage = {}
    project_usage[project_id] = {}
    project_usage[project_id]['global'] = {}
    project_usage[project_id]['users'] = {}

    calculate_project_used(project_id).each do |resource, value|
      if project_usage[project_id]['global'].has_key?(resource)
        project_usage[project_id]['global'][resource] += value
      else
        project_usage[project_id]['global'][resource] = value
      end
    end

    projects = Projects.new
    projects.users(project_id).each do |user_id, user_name|
      project_usage[project_id]['users'][user_id] = {}
      calculate_user_used(user_id, project_id).each do |resource, value|
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
  # Calls the above function for each project that exists.
  def all_projects_used
    total_usage = {}
    projects = Projects.new

    # Loop through each project
    # And do some wacky data processing due to Parallel
    begin
      x = Parallel.map(projects.project_ids) do |project_id|
        { project_id => get_project_usage(project_id) }
      end
      x.each do |y|
        total_usage.merge!(y)
      end
      total_usage
    rescue
      puts "Could not complete calculating used resources. This might have been due to a network error. If you see this message more than once, please look into this in more detail. If you are on-call, this is not an emergency."
      exit 1
    end
  end


  # Shortcut function rather than going through the whole "used" function
  def object_storage_usage(project_id)
    resources = {}
    @openstack.swift_object_mb_usage(project_id).each do |row|
      resources['object_mb'] = row[:object_mb]
    end
    resources
  end


  # Balance quotas in each region for a single project
  # Used by quota-daemon
  def balance_usage(project_id)
    total_usage = {}
    total_usage[project_id] = get_project_usage(project_id)
    sync_all_used(total_usage)
  end


  # Runs balance_quotas for all projects
  # Called by cron and/or novac
  def balance_all_usage
    total_usage = all_projects_used()
    sync_all_used(total_usage)
  end


  # Sets the non-default limits for a project in all regions
  # Not actually used right now.
  # Probably used to be.
  def sync_project_quota(project_id)
    limits = project_quota(project_id)
    @novadb.regions.each do |region|
      limits.each do |resource, limit|
        set_project_quota(project_id, region, resource, limit)
      end
    end
  end

  # Sets a non-default limit for a certain resource for a single project in a single region
  def set_project_quota(project_id, region, resource, limit)
    if resource == 'volumes' or resource == 'gigabytes'
      @openstack.cinder_set_project_quota(project_id, resource, limit, region)
    else
      @openstack.nova_set_project_quota(project_id, resource, limit, region)
    end
  end

  # Runs set_project_quota (above) in all regions
  def set_project_quota_in_all_regions(project_id, resource, limit)
    @novadb.regions.each do |region|
      set_project_quota(project_id, region, resource, limit)
    end
  end

  # Sets non-default limits for all projects in all regions
  # Runs set_project_quota (above) for all projects in all regions
  def sync_all_quotas
    # Build a list of limits
    tmp_project_limits = {}
    projects = Projects.new
    projects.project_ids.each do |project_id|
      tmp_project_limits[project_id] = {}
      limits = project_quota(project_id)
      limits.each do |resource, limit|
        tmp_project_limits[project_id][resource] = limit
      end
    end

    @novadb.regions.each do |region|
      next if @novadb.master_region?('nova', region)

      # Loop through all projects 5 at a time
      Parallel.each(tmp_project_limits.keys) do |project_id|
        tmp_project_limits[project_id].each do |resource, limit|
          limit = tmp_project_limits[project_id][resource]
          #puts "#{project_id} #{region} sync #{resource} #{limit}"
          set_project_quota(project_id, region, resource, limit)
        end
      end
    end
  end


  # Used by other functions in this file
  # Calls the below function for each project, region, and resource
  def sync_all_used(total_usage)
    @novadb.regions.each do |region|
      Parallel.each(total_usage.keys) do |project_id|
        total_usage[project_id].each do |resource, in_use|
          set_project_used_resource(project_id, region, resource, in_use)
        end
      end
    end
  end

  # Sets a project's resource usage in a region
  def set_project_used_resource(project_id, region, resource, in_use)
    if resource == 'volumes' or resource == 'gigabytes'
      @openstack.cinder_set_project_used(project_id, resource, in_use, region)
    else
      @openstack.nova_set_project_used(project_id, resource, in_use, region)
    end
  end

end
