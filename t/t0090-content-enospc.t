#!/bin/sh

test_description='Test content ENOSPC corner cases'

. `dirname $0`/sharness.sh

if ! ls /test/tmpfs-1m; then
	skip_all='skipping ENOSPC tests, no small tmpfs directory mounted'
	test_done
fi

test_expect_success 'create script to fill statedir' '
	cat >fillstatedir.sh <<-EOT &&
	#!/bin/sh
	while true ; do
	      flux run echo 0123456789 > /dev/null 2>&1
	      if flux dmesg | grep -q "No space left on device"; then
		   break
	      fi
	done
	EOT
	chmod +x fillstatedir.sh
'

test_expect_success 'clear & setup test statedir' '
	rm -rf /test/tmpfs-1m/* &&
	mkdir /test/tmpfs-1m/statedir
'

# rc3 currently hangs under ENOSPC, so disable it. flux start will fail for time being
test_expect_success 'flux still operates with content-sqlite running out of space' '
	test_must_fail flux start \
	    -o,-Scontent.backing-module=content-sqlite \
	    -o,-Sstatedir=/test/tmpfs-1m/statedir \
	    -o,-Sbroker.rc3_path= \
	    "./fillstatedir.sh; flux dmesg; flux run echo helloworld" > sql.out 2> sql.err &&
        grep -q "No space left on device" sql.out &&
        grep "helloworld" sql.out
'

test_expect_success 'clear & setup test statedir' '
	rm -rf /test/tmpfs-1m/* &&
	mkdir /test/tmpfs-1m/statedir
'

# rc3 currently hangs under ENOSPC, so disable it. flux start will fail for time being
test_expect_success 'flux still operates with content-files running out of space' '
	test_must_fail flux start \
	    -o,-Scontent.backing-module=content-files \
	    -o,-Sstatedir=/test/tmpfs-1m/statedir \
	    -o,-Sbroker.rc3_path= \
	    "./fillstatedir.sh; flux dmesg; flux run echo helloworld" > files.out 2> files.err &&
        grep -q "No space left on device" files.out &&
        grep "helloworld" files.out
'

test_expect_success 'clear & setup test statedir' '
	rm -rf /test/tmpfs-1m/* &&
	mkdir /test/tmpfs-1m/statedir
'

# flux start will fail b/c rc3 will fail due to ENOSPC
test_expect_success 'content flush returns error on ENOSPC' '
	test_must_fail flux start \
	    -o,-Scontent.backing-module=content-sqlite \
	    -o,-Sstatedir=/test/tmpfs-1m/statedir \
	    "./fillstatedir.sh; flux dmesg; flux content flush" > flush.out 2> flush.err &&
        grep -q "No space left on device" flush.out &&
        grep "content.flush: No space left on device" flush.err
'

test_done
