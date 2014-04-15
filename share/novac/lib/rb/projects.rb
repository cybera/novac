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
    cloud = novadb.cloud
    @projects = {}
    begin
      keystone = Mysql2::Client.new( :host => cloud[:server], :username => cloud[:username], :password => cloud[:password], :database => 'keystone' )

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
    cloud = novadb.cloud
    @users = {}
    begin
      keystone = Mysql.new cloud[:server], cloud[:username], cloud[:password], 'keystone'

      # Get all users in a certain project
      users_rs = keystone.query "select user_id, user.name as name from user_project_metadata inner join user on user_project_metadata.user_id=user.id where project_id = '#{project_id}'"
      users_rs.each_hash do |row|
        @users[row['user_id']] = row['name']
      end
      @users
    ensure
      keystone.close if keystone
    end
  end

  def project_names
    @projects.values
  end

  def project_ids
    @projects.keys
  end
end
