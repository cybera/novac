require 'novadb'

# required libs
required_libs = ['mysql2']
begin
  required_libs.each { |l| require l }
rescue LoadError
  puts "This script needs the following external libraries: "
  required_libs.each { |l| puts " * #{l}" }
end

class Projects

  attr_accessor :projects

  def initialize
    novadb = NovaDB.new
    master = novadb.master_cloud
    @projects = {}
    begin
      keystone = Mysql2::Client.new( :host => master[:server], :username => master[:username], :password => master[:password], :database => 'keystone' )

      # Get the id and name of all projects
      project_rs = keystone.query "select id, name from project"
      project_rs.each do |row|
        # Ignore services project
        next if row['name'] == 'services'
        # Give each project a default quota
        @projects[row['id']] = row['name']
      end
    ensure
      keystone.close if keystone
    end
  end

  def users(project_id)
    novadb = NovaDB.new
    master = novadb.master_cloud
    @users = {}
    begin
      keystone = Mysql2::Client.new( :host => master[:server], :username => master[:username], :password => master[:password], :database => 'keystone' )

      # Get all users in a certain project
      # icehouse
      users_rs = keystone.query "select actor_id as user_id, user.name as name from assignment inner join user on assignment.actor_id=user.id where assignment.target_id = '#{project_id}'"
      #users_rs = keystone.query "select user_id, user.name as name from user_project_metadata inner join user on user_project_metadata.user_id=user.id where project_id = '#{project_id}'"
      users_rs.each do |row|
        @users[row['user_id']] = row['name']
      end
      @users
    ensure
      keystone.close if keystone
    end
  end

  def enabled_projects
    novadb = NovaDB.new
    master = novadb.master_cloud
    begin
      p = {}
      keystone = Mysql2::Client.new( :host => master[:server], :username => master[:username], :password => master[:password], :database => 'keystone' )

      # Get the id and name of all projects
      project_rs = keystone.query "select id, name from project where enabled = 1"
      project_rs.each do |row|
        # Ignore services project
        next if row['name'] == 'services'
        # Give each project a default quota
        p[row['id']] = row['name']
      end
      p
    ensure
      keystone.close if keystone
    end
  end

  def fuzzy_search(x)
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

    # Not found at all
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
