require 'novadb2'
require 'helpers/projects'

class Users

  attr_accessor :users

  def initialize
    @users = {}
    novadb = NovaDB2.new
    # Get the OpenStack Query Library
    os_release = novadb.get_openstack_release
    require "openstack/#{os_release}"
    @openstack = Object.const_get(os_release.capitalize).new

    @openstack.users.each do |row|
        @users[row[:id]] = row[:name]
    end
  end

  def user_names
    @users.values
  end

  def user_ids
    @users.keys
  end

  def projects(user_id)
    projects = []
    @openstack.user_belongs_to_projects(user_id).each do |row|
      projects << row[:name]
    end
    return projects
  end

  def list_user_roles(user_id)
    user_roles = {}
    @openstack.user_roles(user_id).each do |row|
      unless user_roles.has_key?(row[:user_id])
        user_roles[row[:user_id]] = {}
      end
      unless user_roles[row[:user_id]].has_key?(row[:project_id])
        user_roles[row[:user_id]][row[:project_id]] = {}
      end
      user_roles[row[:user_id]][row[:project_id]]['role_id'] = row[:role_id]
      user_roles[row[:user_id]][row[:project_id]]['role_name'] = row[:role_name]
      user_roles[row[:user_id]][row[:project_id]]['project_name'] = row[:project_name]
    end
    user_roles
  end

  def fuzzy_search(x)

    return {} if not x

    # All users
    users = self.users
    user = {}

    # Was a UUID given?
    if users.key?(x)
      user[x] = user[x]
    end

    # Was a username given?
    unless user.length > 0
      user = users.select { |k,v| v.downcase =~ /#{x.downcase}/ }
    end

    # Not found or more than one found
    if user.keys.length == 0
      throw "No user found."
    elsif user.keys.length > 1
      throw "More than one user found."
    else
      return user
    end
  end

end
