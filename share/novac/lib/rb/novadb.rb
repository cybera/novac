class NovaDB
  attr_accessor :cloud, :regions, :queues
  def initialize
    # A .mysqlrc file is a custom rc file that contains entries of
    # all databases in all clouds that are being controlled.
    # Each database's nova table is queried for running instances.
    # If no .mysqlrc file is found, .my.cnf is used for a single-
    # cloud install
    r = {}
    @cloud = {}
    @queues = {}
    if File.exists?('/root/.mysqlrc')
      File.open('/root/.mysqlrc').each do |line|
        region, server, username, password, comment = line.strip.split(',')
        r[region] = 1
        my_fqdn = `facter fqdn`.chomp
        if comment == my_fqdn
          @cloud = {
            :username => username,
            :password => password,
            :server   => server
          }
        end
      end
      @regions = r.keys
    elsif File.exists?('/root/.my.cnf')
      @clouds[:nova] = {}
      @clouds[:nova][:server] = 'localhost'
      File.open('/root/.my.cnf').each do |line|
        key, value = line.split('=')
        case key
        when 'user'
          @clouds[:nova][:username] = value.strip!
        when 'password'
          @clouds[:nova][:password] = value.strip!
        end
      end
      @master_cloud = @clouds[:nova]
    else
      throw "/root/.mysqlrc or /root/.my.cnf file is needed."
    end

    if File.exists?('/root/.rabbitmqrc')
      File.open('/root/.rabbitmqrc').each do |line|
        region, server, username, password, comment = line.strip.split(',')
        @queues[region] = {}
        @queues[region] = {
          :host     => server,
          :username => username,
          :password => password
        }
      end
    end
  end
end
