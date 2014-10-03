require 'inifile'
require 'sequel'
require 'pp'

class NovaDB2

  attr_accessor :master_cloud, :master_region, :clouds, :queues

  def initialize

    @clouds = {}
    @databases = {}
    @queues = {}
    @openstack_config = {}

    if File.exists?('/etc/novac/config.ini')
      ini = IniFile.load('/etc/novac/config.ini')
      ini.each_section do |section|
        if section == 'openstack'
          ini[section].each do |k,v|
            @openstack_config[k.to_sym] = v
          end
        else
          type, region, x = section.split(':')
          unless @clouds.key?(region)
            @clouds[region] = {}
          end
          unless @clouds[region].key?(type)
            @clouds[region][type] = {}
          end
          unless @clouds[region][type].key?(x)
            @clouds[region][type][x] = {}
            ini[section].each do |k,v|
              @clouds[region][type][x][k.to_sym] = v
            end
          end
        end
      end
    else
      throw '/etc/novac/config.ini does not exist.'
    end

  end

  def _connect_to_dbs
    @clouds.each do |region, region_info|
      unless @clouds[region].key?('db')
        @clouds[region]['db'] = {}
      end

      region_info['mysql'].each do |db, db_info|
        unless @clouds[region]['db'].key?(db)
          @clouds[region]['db'][db] = Sequel.mysql2(db_info)
        end
      end

    end
  end

  def get_openstack_release
    @openstack_config[:release]
  end

  def get_database(db, region = nil)
    _connect_to_dbs

    if not region
      @clouds.keys.each do |r|
        if @clouds[r]['mysql'][db][:master] == true
          return @clouds[r]['db'][db]
        end
      end
    else
      return @clouds[region]['db'][db]
    end
  end

  def get_database_name(db, region = nil)
    require 'pp'
    if not region
      @clouds.keys.each do |r|
        if @clouds[r]['mysql'][db][:master] == true
          return @clouds[r]['mysql'][db][:database]
        end
      end
    else
      return @clouds[region]['mysql'][db][:database]
    end
  end

  def get_queue(region)
    @clouds[region]['rabbitmq']
  end

  def master_region?(db, region)
    return @clouds[region]['mysql'][db][:master]
  end

  def regions
    return @clouds.keys
  end

end
