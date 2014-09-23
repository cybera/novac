require 'novadb2'
require 'sequel'

class Icehouse

  def initialize
    @novadb = NovaDB2.new
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

  def instances_by_project(project_id, region = nil)
    @novadb.get_database('nova', region).fetch("
      select id, display_name from instances
      where project_id = '#{project_id}' and deleted = 0
      order by display_name
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
      select from_port, to_port, cidr
      from security_group_rules inner join security_groups on security_groups.id=security_group_rules.parent_group_id
        inner join keystone.project on security_groups.project_id=keystone.project.id
      where security_group_rules.deleted = 0 AND keystone.project.id = '#{project_id}'
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

  def nova_set_project_quota(project_id, resource, limit, region = nil)
    db = @novadb.get_database('nova', region)
    count = db.fetch("
              select count(*) as c
              from quotas
              where project_id = '#{project_id}' and resource = '#{resource}'
            ")[0][:c].to_i
    if count == 1
      ds = db['update quotas set hard_limit = ? where resource = ? and project_id = ?',
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
    require 'pp'

    if not user_id
      user_id = 'NULL'
    end

    db = @novadb.get_database('nova', region)
    count = db.fetch("
              select count(*) as c
              from quota_usages
              where project_id = '#{project_id}' and resource = '#{resource}' and user_id = '#{user_id}'
            ")
    c = -1
    count.each do |row|
      c = row[:c].to_i
    end
    if c == 1
      ds = db["update quota_usages set in_use = '#{in_use}' where resource = '#{resource}' and project_id = '#{project_id}'"]
      ds.update
    elsif c == 0
      ds = db["insert into quota_usages (created_at, updated_at, deleted, reserved, project_id, resource, in_use, user_id)
               values (now(), now(), 0, 0, '#{project_id}', '#{resource}', '#{in_use}', '#{user_id}'"
           ]
      ds.insert
    else
      throw "Unable to update default #{resource} to #{limit}. #{project_id} has #{count} entries for #{resource}"
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

  def users_in_project(project_id, region = nil)
    @novadb.get_database('keystone', region).fetch("
      select actor_id as user_id, user.name as name
      from assignment inner join user on assignment.actor_id=user.id
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
      select id, name
      from user
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
      select role.id as role_id, role.name as role_name, project.name as project_name, project.id as project_id, user.id as user_id, user.name as user_name
      from role inner join assignment on role.id = assignment.role_id
        inner join user on user.id = assignment.actor_id
        inner join project on project.id = assignment.target_id
      where
        1=1
        #{query_args_user}
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
    ")
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
    @novadb.get_database('cinder', region).fetch("
      select volumes.id as id, volumes.project_id as project_id, size, instances.host as host, instances.display_name as instance, mountpoint, status, attach_status, volumes.display_name as volume
      from volumes left join nova.instances on cinder.volumes.instance_uuid=nova.instances.uuid
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
    count = db.fetch("
              select count(*) as c
              from quotas
              where project_id = '#{project_id}' and resource = '#{resource}'
            ")[0][:c].to_i
    if count == 1
      ds = db['update quotas set hard_limit = ? where resource = ? and project_id = ?',
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

  def cinder_set_project_used(project_id, resource, in_use, region = nil)
    db = @novadb.get_database('cinder', region)
    count = db.fetch("
              select count(*) as c
              from quota_usages
              where project_id = '#{project_id}' and resource = '#{resource}'
            ")[0][:c].to_i
    if count == 1
      ds = db["update quota_usages set in_use = '#{in_use} where resource = '#{resource}' and project_id = '#{project_id}'"]
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
