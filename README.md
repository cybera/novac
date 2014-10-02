# Novac

novac is a collection of scripts that assist with day-to-day OpenStack Operations. It's very site-specific to the clouds that Cybera deploys, but you may find it useful.

## Branches

Dev = Grizzly based Cloud
Cloud = Havana based Cloud
Icehouse = Icehouse based Clouds

## Commands

The Dev and Cloud branches are very different. It's recommended to use the Icehouse branch as it is now able to accommodate deployments of different configurations. It's also able to support different versions of OpenStack by writing appropriate queries in `share/novac/lib/rb/openstack`.

Commands | Cloud | Dev
------- | ----------- | ----------
cloud-instance-metrics | NO | Good
cloud-instance-metrics-collectd | NO | Good
cloud-stat | NO | Good
cloud-user-metrics | NO | Good
cloud-user-metrics-bulk | NO | Good
list-images | Good | Good
list-instances | Good | Good
list-users | Good | Good
list-volumes | Good | Good
node-rename | Unknown | Unknown
project-artifacts | Good | BROKEN. Not working with names
quota-balance | Good | Good
quota-cron | Good | Good
quota-get-used-resources | Good | Good
quota-image-get | Good | Good
quota-image-set | Good | Good
quota-object_mb-get | Good | Good
quota-object_mb-set | Good | Good
quota-object_mb-usage | BROKEN | Good
quota-report | BROKEN | Good
quota-snafu | BROKEN | Good
quota-sync-limits | Good | Good
secgroup-list | Good | Good
swift-set-quota | BROKEN  | Good
user-artifacts | Good | Good
windows-audit | Good | Good

Last Checked: 2014-10-02

