#!/bin/sh

test_description='Test flux emulator command'

. $(dirname $0)/sharness.sh

test_under_flux 1

# Set CLIMain log level to logging.DEBUG (10), to enable stack traces
export FLUX_PYCLI_LOGLEVEL=3

flux setattr log-stderr-level 1
SHARNESS_TEST_SRCDIR='/home/j/Desktop/flux/flux-core-gclab/t'
SIM_JOBTRACES_DIR=${SHARNESS_TEST_SRCDIR}/simulator/job-traces

test_expect_success 'flux emulator fails with usage message' '
	test_must_fail flux emulator 2>usage.err &&
	grep -i usage: usage.err
'

test_expect_success 'flux emulator with single node works' '
    flux emulator /home/j/Desktop/flux/flux-core-gclab/t/simulator/job-traces/10-single-node.csv 1 16 >run1.out 2>run1.err &&
    grep -i "utilization: 100" run1.out
'

test_expect_success 'flux emulator with multiple nodes works' '
	flux emulator /home/j/Desktop/flux/flux-core-gclab/t/simulator/job-traces/10-multi-node.csv 30 16 >run2.out 2>run2.err &&
    grep -i "utilization: 83" run2.out
'

test_expect_failure 'flux emulator errors out on unstaisfiable jobs' '
	run_timeout 5 flux emulator $SIM_JOBTRACES_DIR/10-multi-node.csv 1 16 >run3.out 2>run3.err &&
    grep -i "unsatisfiable" run3.err
'

test_done