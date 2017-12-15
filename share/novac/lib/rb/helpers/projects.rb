require 'novadb2'

class Projects

  attr_accessor :projects

  def initialize
    novadb = NovaDB2.instance
    # Get the OpenStack Query Library
    os_release = novadb.get_openstack_release
    require "openstack/#{os_release}"
    @openstack = Object.const_get(os_release.capitalize).new

    @projects = {}

    @openstack.projects.each do |row|
      next if row[:name] == 'services'
      @projects[row[:id]] = row[:name]
    end
  end

  def users(project_id)
    users = {}
    @openstack.users_in_project(project_id).each do |row|
      users[row[:user_id]] = row[:name]
    end
    users
  end

  def enabled_projects
    projects = {}
    @openstack.enabled_projects.each do |row|
      projects[row[:id]] = row[:name]
    end
    projects
  end

  def all_projects
    projects = {}
    @openstack.all_projects.each do |row|
      projects[row[:id]] = row[:name]
    end
    projects
  end

  def all_projects_with_email
    projects = {}
    @openstack.all_projects_with_email.each do |row|
      projects[row[:id]] = {:name => row[:name], :email => row[:email] }
    end
    projects
  end


  def fuzzy_search(x)

    return {} if not x

    # All projects
    projects = self.projects
    project = {}

    # Was a UUID given?
    if projects.key?(x)
      project[x] = projects[x]
    end

    # Was a project name given?
    unless project.length > 0
      project = projects.select { |k, v| v.downcase =~ /#{x.downcase}/ }
    end

    # Not found or more than one found
    if project.keys.length == 0
      throw "No projects found."
    elsif project.keys.length > 1
      throw "More than one project found."
    else
      return project
    end
  end

  def project_names
    @projects.values
  end

  def project_ids
    @projects.keys
  end
end
