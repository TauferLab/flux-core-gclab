#!/bin/sh

test_description='Test resource exclusion'

. `dirname $0`/sharness.sh

# Start out with empty config object
# Then we will reload after adding TOML to cwd
export FLUX_CONF_DIR=$(pwd)

SIZE=4
test_under_flux $SIZE

# Usage: waitup N
#   where N is a count of online ranks
waitup () {
    run_timeout 5 flux python -c "import flux; print(flux.Flux().rpc(\"resource.monitor-waitup\",{\"up\":$1}).get())"
}
waitdown () {
    waitup $(($SIZE-$1))
}

has_resource_event () {
    flux kvs eventlog get resource.eventlog | awk '{ print $2 }' | grep $1
}

test_expect_success 'wait for monitor to declare all nodes are up' '
    waitdown 0
'
test_expect_success 'reconfigure with rank 0 exclusion' '
	cat >resource.toml <<-EOT &&
	[resource]
	exclude = "0"
	EOT
	flux config reload
'

test_expect_success 'flux resource list shows one node down' '
	test $(flux resource list -n -s down -o {nnodes}) -eq 1
'

test_expect_success 'but monitor still says all nodes are up' '
	waitdown 0
'

test_expect_success 'exclude event was posted' '
	test $(has_resource_event exclude | wc -l) -eq 1
'

test_expect_success 'reconfig with bad exclude idset fails' '
	cat >resource.toml <<-EOT &&
	[resource]
	exclude = "xxzz"
	EOT
	test_must_fail flux config reload
'

test_expect_success 'flux resource list still shows one node down' '
	test $(flux resource list -n -s down -o {nnodes}) -eq 1
'

test_expect_success 'reconfig with out of range exclude idset fails' '
	cat >resource.toml <<-EOT &&
	[resource]
	exclude = "0-$SIZE"
	EOT
	test_must_fail flux config reload
'

test_expect_success 'reconfig with no exclude idset' '
	cat >resource.toml <<-EOT &&
	[resource]
	EOT
	flux config reload
'

test_expect_success 'flux resource list shows zero nodes down' '
	test $(flux resource list -n -s down -o {nnodes}) -eq 0
'

test_expect_success 'unexclude event was posted' '
	test $(has_resource_event unexclude | wc -l) -eq 1
'

test_done
