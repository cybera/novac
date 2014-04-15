class NovaDB
  attr_accessor :cloud, :regions, :queues, :master_region
  def initialize
    # A .mysqlrc file is a custom rc file that contains entries of
    # all databases in all clouds that are being controlled.
    # Each database's nova table is queried for running instances.
    # If no .mysqlrc file is found, .my.cnf is used for a single-
    # cloud install
    r = {}
    @cloud = {}
    @queues = {}
    @master_region = nil
    if File.exists?('/root/.mysqlrc')
      File.open('/root/.mysqlrc').each do |line|
        region, server, username, password, fqdn, comment = line.strip.split(',')
        r[region] = 1
        my_fqdn = `facter fqdn`.chomp
        if fqdn == my_fqdn
          @cloud = {
            :username => username,
            :password => password,
            :server   => server
          }
        end
        if comment == 'master'
          @master_region = region
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
        region, server, username, password, fqdn, comment = line.strip.split(',')

        if not @queues.has_key?(region)
          @queues[region] = {}
        end

        @queues[region] = {
          :host     => server,
          :port     => 5673,
          :username => username,
          :password => password,
          :vhost => 'openstack'
        }
      end
    end
  end
end
