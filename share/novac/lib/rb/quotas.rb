module Quotas
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
    'reservation_expire'          => 86400
  }
  def self.get_default_quotas
    @defaults
  end
end
