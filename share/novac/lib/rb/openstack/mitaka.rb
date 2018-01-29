require 'novadb2'
require 'sequel'

class Mitaka

  def initialize
    @novadb = NovaDB2.instance
  end

  # Nova
  def instances_query(region = nil)
    @novadb.get_database('nova', region).fetch("
      select uuid, user_id, instances.project_id, hostname, instances.host, image_ref,
             fixed_ips.address as fixed_ip, floating_ips.address as floating_ip, vm_state,
             instance_types.name as flavor, key_name
      from instances inner join instance_types on instances.instance_type_id=instance_types.id
                     inner join fixed_ips on instances.uuid=fixed_ips.instance_uuid
                     left join floating_ips on fixed_ips.id=floating_ips.fixed_ip_id
      where instances.deleted = 0
    ")
  end

  def instances_by_host(region=nil, host)
    @novadb.get_database('nova', region).fetch("
      select uuid, instances.project_id, hostname, image_ref,
        floating_ips.address as floating_ip, vm_state, instance_types.name as flavor
      from instances inner join instance_types on instances.instance_type_id=instance_types.id
                inner join fixed_ips on instances.uuid=fixed_ips.instance_uuid
                left join floating_ips on fixed_ips.id=floating_ips.fixed_ip_id
      where instances.deleted = 0 and instances.host = '#{host}'
    ")
  end

  def instances_by_project(project_id, region = nil)
    @novadb.get_database('nova', region).fetch("
      select id, display_name from instances
      where project_id = '#{project_id}' and deleted = 0
      order by display_name
    ")
  end

  def all_instances_by_project(project_id, region = nil)
    @novadb.get_database('nova', region).fetch("
      select uuid, display_name, created_at, deleted_at, vm_state from instances
      where project_id = '#{project_id}'
      order by uuid
    ")
  end

  def instances_by_user(user_id, region = nil)
    @novadb.get_database('nova', region).fetch("
      select id, display_name from instances
      where user_id = '#{user_id}' and deleted = 0
      order by display_name
    ")
  end

  def floating_ips_by_project(project_id, region = nil)
    @novadb.get_database('nova', region).fetch("
      select address from floating_ips
      where project_id = '#{project_id}'
      order by address
    ")
  end

  def secgroups_by_project(project_id, region = nil)
    @novadb.get_database('nova', region).fetch("
      select security_groups.name as name, from_port, to_port, cidr, protocol
      from security_group_rules inner join security_groups on security_groups.id=security_group_rules.parent_group_id
        inner join keystone.project on security_groups.project_id=keystone.project.id
      where security_group_rules.deleted = 0 AND keystone.project.id = '#{project_id}'
    ")
  end

  def open_secgroups(region = nil)
    @novadb.get_database('nova', region).fetch("
      select security_groups.project_id, security_groups.name as security_group, from_port, to_port, cidr, protocol
      from security_group_rules inner join security_groups on security_groups.id=security_group_rules.parent_group_id
      where security_group_rules.deleted = 0 AND protocol != 'icmp' AND cidr = '0.0.0.0/0'
    ")
  end

  def secgroup_rules_for_instance(instance_uuid, region = nil)
    @novadb.get_database('nova', region).fetch("
      select security_groups.name, security_groups.description, from_port, to_port, cidr, protocol
      from security_group_rules
      join security_groups on security_groups.id=security_group_rules.parent_group_id
      join security_group_instance_association on security_group_id = security_groups.id
      where security_group_rules.deleted = 0 AND protocol != 'icmp' AND cidr = '0.0.0.0/0' and instance_uuid = '#{instance_uuid}'
    ")
  end

  def instance_launches_since_jan2013(region = nil)
    @novadb.get_database('nova', region).fetch("
      select * from instances
      where date(created_at) >= '2013-01-01'
      order by created_at
    ")
  end

  def ec2_query(instance_uuid, region = nil)
    rows = @novadb.get_database('nova', region).fetch("
      select id
      from instance_id_mappings
      where uuid = '#{instance_uuid}'
    ")
    rows.first[:id]
  end

  def available_ip_count(region = nil)
    @novadb.get_database('nova', region).fetch("
      select COUNT(*) as count
      from floating_ips
      where deleted = 0
    ")
  end

  def used_ips(region = nil)
    @novadb.get_database('nova', region).fetch("
      select floating_ips.address, floating_ips.project_id, instances.display_name
      from floating_ips
      join fixed_ips on floating_ips.fixed_ip_id = fixed_ips.id
      join instances ON fixed_ips.instance_uuid = instances.uuid
    ")
  end

  def free_ips(region = nil)
    @novadb.get_database('nova', region).fetch("
      select address from floating_ips where `project_id` is null and deleted = 0
    ")
  end

  def idle_ips(region = nil)
    @novadb.get_database('nova', region).fetch("
      select address, project_id from floating_ips where project_id is not null and fixed_ip_id is null
    ")
  end

  # Nova quotas
  def nova_project_quota(project_id, region = nil)
    @novadb.get_database('nova', region).fetch("
      select resource, hard_limit
      from quotas
      where project_id = '#{project_id}'
    ")
  end

  def nova_quota_usages(project_id, region = nil)
    @novadb.get_database('nova', region).fetch("
      select resource, in_use
      from quota_usages
      where project_id = '#{project_id}'
    ")
  end

  def nova_instance_count(user_id, project_id, region = nil)
    @novadb.get_database('nova', region).fetch("
      select count(*) as instances
      from instances
      where user_id = '#{user_id}' and project_id = '#{project_id}' and deleted = 0
    ")
  end

  def nova_compute_usage(user_id, project_id, region = nil)
    @novadb.get_database('nova', region).fetch("
      select sum(memory_mb) as ram, sum(vcpus) as cores
      from instances
      where user_id = '#{user_id}' and project_id = '#{project_id}' and deleted = 0
    ")
  end

  def nova_floating_ip_count(project_id, region = nil)
    c = self.floating_ips_by_project(project_id, region).count
    return [{:floating_ips => c}]
  end

  def instance_count(region = nil)
    @novadb.get_database('nova', region).fetch("
      select count(*) as count
      from instances
      WHERE deleted = 0
    ")
  end

  def active_instances_count(project_id, region = nil)
    @novadb.get_database('nova', region).fetch("
      select count(*) as count
      from instances
      where project_id = '#{project_id}' and deleted = 0
    ")
  end

  def deleted_instances_count(project_id, region = nil)
    @novadb.get_database('nova', region).fetch("
      select count(*) as count
      from instances
      where project_id = '#{project_id}' and deleted > 0
    ")
  end

  def nova_set_project_quota(project_id, resource, limit, region = nil)
    db = @novadb.get_database('nova', region)

    # Check to see if the current value is the same.
    # This should save on resources
    count = db.fetch("
              select count(*) as c
              from quotas
              where project_id = '#{project_id}' and resource = '#{resource}' and hard_limit = '#{limit}'
            ").first[:c].to_i
    if count == 1
      #puts "No change for #{project_id} #{resource} #{limit} #{region}"
      return true
    end

    count = db.fetch("
              select count(*) as c
              from quotas
              where project_id = '#{project_id}' and resource = '#{resource}'
            ").first[:c].to_i
    if count == 1
      #puts "Updating #{project_id} #{resource} to #{limit}"
      ds = db['update quotas set hard_limit = ?, updated_at = now() where resource = ? and project_id = ?',
              limit, resource, project_id
           ]
      ds.update
    elsif count == 0
      ds = db['insert into quotas (created_at, updated_at, deleted, project_id, resource, hard_limit)
               values (now(), now(), 0, ?, ?, ?)',
               project_id, resource, limit
           ]
      ds.insert
    else
      throw "Unable to update default #{resource} to #{limit}. #{project_id} has #{count} entries for #{resource}"
    end
  end

  def nova_set_project_used(project_id, resource, in_use, user_id = nil, region = nil)
    db = @novadb.get_database('nova', region)

    user_id_select = nil
    user_id_insert = nil
    user_id_update = nil

    if not user_id
      user_id_select = 'user_id is NULL'
      user_id_insert = 'NULL'
      user_id_update = 'user_id is NULL'
    else
      user_id_select = "user_id = '#{user_id}'"
      user_id_insert = "'#{user_id}'"
      user_id_update = "user_id = '#{user_id}'"
    end

    # Check to see if the current value is the same.
    # This should save on resources
    count = db.fetch("
              select count(*) as c
              from quota_usages
              where project_id = '#{project_id}' and resource = '#{resource}' and in_use = '#{in_use}' and #{user_id_select}
            ").first[:c].to_i
    if count == 1
      #puts "No change for #{project_id} #{resource} #{in_use} #{region}"
      return true
    end

    count = db.fetch("
              select count(*) as c
              from quota_usages
              where project_id = '#{project_id}' and resource = '#{resource}' and #{user_id_select}
            ").first[:c].to_i
    if count == 1
      #puts "#{project_id} #{resource} #{in_use} #{user_id_update} #{region} #{count} update"
      ds = db["update quota_usages set in_use = '#{in_use}' where resource = '#{resource}' and project_id = '#{project_id}' and #{user_id_update}"]
      ds.update
    elsif count == 0
      #puts "#{project_id} #{resource} #{in_use} #{user_id_insert} #{region} #{count} insert"
      ds = db["insert into quota_usages (created_at, updated_at, deleted, reserved, project_id, resource, in_use, user_id)
               values (now(), now(), 0, 0, '#{project_id}', '#{resource}', '#{in_use}', #{user_id_insert})"
           ]
      ds.insert
    else
      throw "Unable to update #{resource} to #{in_use}. Project #{project_id}, user #{user_id} has #{count} entries for #{resource}"
    end
  end


  # Keystone
  def projects(region = nil)
    @novadb.get_database('keystone', region).fetch("
      select id, name from project
    ")
  end

  def enabled_projects(region = nil)
    @novadb.get_database('keystone', region).fetch("
      select id, name from project where enabled = 1
    ")
  end

  def all_projects_with_email(region = nil)
    @novadb.get_database('keystone', region).fetch("
      select project.id, project.name, user.extra as email from project
      left join user on default_project_id=project.id
    ")
  end

  def all_projects(region = nil)
    @novadb.get_database('keystone', region).fetch("
      select id, name from project
    ")
  end

  def users_in_project(project_id, region = nil)
    @novadb.get_database('keystone', region).fetch("
      select actor_id as user_id, local_user.name as name
      from assignment inner join user on assignment.actor_id=user.id
      join local_user on user.id = local_user.user_id
      where assignment.target_id = '#{project_id}'
    ")
  end

  def user_belongs_to_projects(user_id, region = nil)
    @novadb.get_database('keystone', region).fetch("
      select project.id, project.name
      from keystone.assignment inner join keystone.user on keystone.assignment.actor_id = keystone.user.id
          inner join keystone.project on keystone.project.id = keystone.assignment.target_id
      where keystone.user.id = '#{user_id}' order by project.name
    ")
  end

  def users(region = nil)
    @novadb.get_database('keystone', region).fetch("
      select user.id as id, name
      from user
      join local_user on user_id=user.id
    ")
  end

  def user_by_id(user_id, region = nil)
    @novadb.get_database('keystone', region).fetch("
      select *
      from user
      where id = '#{user_id}'
    ")
  end

  def user_roles(user_id = nil, region = nil)
    if user_id != nil
      query_args_user = "and actor_id = '#{user_id}'"
    else
      query_args_user = ''
    end

    @novadb.get_database('keystone', region).fetch("
      select role.id as role_id, role.name as role_name, project.name as project_name, project.id as project_id, user.id as user_id, local_user.name as user_name
      from role inner join assignment on role.id = assignment.role_id
        inner join user on user.id = assignment.actor_id
        inner join local_user on user.id = local_user.user_id
        inner join project on project.id = assignment.target_id
      where
        1=1
        #{query_args_user}
    ")
  end

  def email_from_extra_field(project_id = nil, region = nil)
    @novadb.get_database('keystone', region).fetch("
      select extra from user where default_project_id='#{project_id}'
    ")
  end


  # Glance
  def images_query(region = nil)
    @novadb.get_database('glance', region).fetch("
      select images.id, images.name as image_name, size, is_public, project.name as tenant_name
      from glance.images inner join keystone.project on keystone.project.id=glance.images.owner
      where glance.images.status = 'active' order by project.name
    ")
  end

  def image_by_id(image_id, region = nil)
    @novadb.get_database('glance', region).fetch("
      select *
      from images
      where id = '#{image_id}'
    ").first
  end

  def images_by_owner(project_id, region = nil)
    @novadb.get_database('glance', region).fetch("
      select id, name from images
      where owner = '#{project_id}' order by name
    ")
  end

  def image_properties_query(image_id, region = nil)
    @novadb.get_database('glance', region).fetch("
      select name, value
      from image_properties
      where image_id = '#{image_id}'
    ")
  end

  # Glance quotas
  def glance_image_count(project_id, region = nil)
    @novadb.get_database('glance', region).fetch("
      select count(*) as images
      from images
      where owner = '#{project_id}' and (status != 'deleted' and status != 'killed')
    ")
  end

  # Cinder
  def volumes_query(region = nil)
    cinder_db = @novadb.get_database_name('cinder', region)
    nova_db = @novadb.get_database_name('nova', region)
    @novadb.get_database('cinder', region).fetch("
      select volumes.id as id, volumes.project_id as project_id, size, instances.host as host, instances.display_name as instance, mountpoint, status, attach_status, volumes.display_name as volume
      from volumes left join #{nova_db}.instances on #{cinder_db}.volumes.instance_uuid=#{nova_db}.instances.uuid
      where status in ('in-use', 'available') order by volumes.display_name, status
    ")
  end

  def volume_type_query(region = nil)
    cinder_db = @novadb.get_database_name('cinder', region)
    @novadb.get_database('cinder', region).fetch("
      select volumes.id as id, project_id, size, attach_status, volumes.display_name as volume, volume_types.name as volume_type
      from volumes inner join #{cinder_db}.volume_types on volumes.volume_type_id=volume_types.id
      where status in ('in-use', 'available') order by volumes.display_name, status
    ")
  end

  def volumes_by_project(project_id, region = nil)
    @novadb.get_database('cinder', region).fetch("
      select id, display_name from volumes
      where project_id = '#{project_id}' and deleted = 0
      order by display_name
    ")
  end

  def volumes_by_user(user_id, region = nil)
    @novadb.get_database('cinder', region).fetch("
      select id, display_name from volumes
      where user_id = '#{user_id}' and deleted = 0
      order by display_name
    ")
  end

  def volume_count(region = nil)
    @novadb.get_database('cinder', region).fetch("
      select count(*) as count
      from volumes
      WHERE deleted = 0
    ")
  end

  def active_volumes_count(project_id, region = nil)
    @novadb.get_database('cinder', region).fetch("
      select count(*) as count
      from volumes
      where project_id = '#{project_id}' and deleted = 0
    ")
  end

  def deleted_volumes_count(project_id, region = nil)
    @novadb.get_database('cinder', region).fetch("
      select count(*) as count
      from volumes
      where project_id = '#{project_id}' and deleted > 0
    ")
  end

  # Cinder quotas
  def cinder_project_quota(project_id, region = nil)
    @novadb.get_database('cinder', region).fetch("
      select resource, hard_limit from quotas
      where project_id = '#{project_id}'
    ")
  end

  def cinder_quota_usages(project_id, region = nil)
    @novadb.get_database('cinder', region).fetch("
      select resource, in_use
      from quota_usages
      where project_id = '#{project_id}'
    ")
  end

  def cinder_volume_count(project_id, region = nil)
    @novadb.get_database('cinder', region).fetch("
      select count(*) as volumes
      from volumes
      where project_id = '#{project_id}' and deleted = 0
    ")
  end

  def cinder_volume_usage(project_id, region = nil)
    @novadb.get_database('cinder', region).fetch("
      select sum(size) as gigabytes
      from volumes
      where project_id = '#{project_id}' and deleted = 0
    ")
  end

  # Swift Quotas
  def swift_regional_object_mb_usage(project_id, region = nil)
    @novadb.get_database('nova', region).fetch("
      select in_use as object_mb
      from quota_usages
      where project_id = '#{project_id}' and resource = 'swift_regional_object_usage'
    ")
  end

  def swift_object_mb_usage(project_id, region = nil)
    @novadb.get_database('nova', region).fetch("
      select in_use as object_mb
      from quota_usages
      where project_id = '#{project_id}' and resource = 'object_mb'
    ")
  end

  def cinder_set_project_quota(project_id, resource, limit, region = nil)
    db = @novadb.get_database('cinder', region)

    # Check to see if the current value is the same.
    # This should save on resources
    count = db.fetch("
              select count(*) as c
              from quotas
              where project_id = '#{project_id}' and resource = '#{resource}' and hard_limit = '#{limit}'
            ").first[:c].to_i
    if count == 1
      #puts "No change for #{project_id} #{resource} #{limit} #{region}"
      return true
    end

    count = db.fetch("
              select count(*) as c
              from quotas
              where project_id = '#{project_id}' and resource = '#{resource}'
            ").first[:c].to_i
    if count == 1
      ds = db['update quotas set hard_limit = ? where resource = ? and project_id = ?',
              limit, resource, project_id
           ]
      ds.update
    elsif count == 0
      # Allocated is not actually used - instead the usages table is used but needs to be set or else it's set to NULL
      ds = db['insert into quotas (created_at, updated_at, deleted, project_id, resource, hard_limit, allocated)
               values (now(), now(), 0, ?, ?, ?, 0)',
               project_id, resource, limit
           ]
      ds.insert
    else
      throw "Unable to update default #{resource} to #{limit}. #{project_id} has #{count} entries for #{resource}"
    end
  end

  def cinder_set_project_used(project_id, resource, in_use, region = nil)
    db = @novadb.get_database('cinder', region)

    # Check to see if the current value is the same.
    # This should save on resources
    count = db.fetch("
              select count(*) as c
              from quota_usages
              where project_id = '#{project_id}' and resource = '#{resource}' and in_use = '#{in_use}'
            ").first[:c].to_i
    if count == 1
      #puts "No change for #{project_id} #{resource} #{in_use} #{region}"
      return true
    end

    count = db.fetch("
              select count(*) as c
              from quota_usages
              where project_id = '#{project_id}' and resource = '#{resource}'
            ").first[:c].to_i
    if count == 1
      ds = db["update quota_usages set in_use = '#{in_use}' where resource = '#{resource}' and project_id = '#{project_id}'"]
      ds.update
    elsif count == 0
      ds = db["insert into quota_usages (created_at, updated_at, deleted, reserved, project_id, resource, in_use)
               values (now(), now(), 0, 0, '#{project_id}', '#{resource}', '#{in_use}')"
           ]
      ds.insert
    else
      throw "Unable to update default #{resource} to #{limit}. #{project_id} has #{count} entries for #{resource}"
    end
  end

end
