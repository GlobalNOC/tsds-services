
## tsds_fix_measurements.pl
### How to use
run the script with `--doit` to run. the script will check measurement, but won't correct it without `--doit`.
If you want to run this script against only specific measurment type. (i.e. interface). use `--target_measurement`.
If you need to run this against only a node, use `--target_hostname`. These options can save a lot of time if you know what measurement_type/hostname is needed to be fixed.
 
### When to use
This script is used to fix unmatching measurments between same node/intf. this generally happens with migration of data from another instance. For example, a node/intf is collected in hostA, but another collectin is started on hostB to migrate host. And the data can be migratated by tsds-to-tsds importer from hostA to hostB. but in this case, conflict between old and new can happen. 
