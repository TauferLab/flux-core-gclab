# Legacy function for the flux emulator. needs to be revisited for modern flux
# used in post sim analysis
# The entire job.py file needs to be removed because it has been reworked. This can probably be rewritten for that.  

def convert_id(jobid, src="dec", dst="dec"):
    valid_id_types = six.string_types + six.integer_types
    if not any((isinstance(jobid, id_type) for id_type in valid_id_types)):
        raise TypeError("Jobid must be an integer or string, not {}".format(type(jobid)))

    valid_formats = ("dec", "hex", "kvs", "words")
    if src not in valid_formats:
        raise EnvironmentError(errno.EINVAL, "src must be one of {}", valid_formats)
    if dst not in valid_formats:
        raise EnvironmentError(errno.EINVAL, "dst must be one of {}", valid_formats)

    if isinstance(jobid, six.text_type):
        jobid = jobid.encode('utf-8')

    if src == dst:
        return jobid

    dec_jobid = ffi.new('uint64_t [1]') # uint64_t*
    if src == "dec":
        dec_jobid = jobid
    elif src == "hex":
        if (lib.fluid_decode (jobid, dec_jobid, flux.constants.FLUID_STRING_DOTHEX) < 0):
            raise EnvironmentError(errno.EINVAL, "malformed jobid: {}".format(src));
        dec_jobid = dec_jobid[0]
    elif src == "kvs":
        if jobid[0:4] != 'job.':
            raise EnvironmentError(errno.EINVAL, "missing 'job.' prefix")
        if (lib.fluid_decode (jobid[4:], dec_jobid, flux.constants.FLUID_STRING_DOTHEX) < 0):
            raise EnvironmentError(errno.EINVAL, "malformed jobid: {}".format(src));
        dec_jobid = dec_jobid[0]
    elif src == "words":
        if (lib.fluid_decode (jobid, dec_jobid, flux.constants.FLUID_STRING_MNEMONIC) < 0):
            raise EnvironmentError(errno.EINVAL, "malformed jobid: {}".format(src));
        dec_jobid = dec_jobid[0]


    buf_size = 64
    buf = ffi.new('char []', buf_size)
    def encode(id_format):
        pass

    if dst == 'dec':
        return dec_jobid
    elif dst == 'kvs':
        key_len = RAW.flux_job_kvs_key(buf, buf_size, dec_jobid, ffi.NULL)
        if key_len < 0:
            raise RuntimeError("error enconding id")
        return ffi.string(buf, key_len).decode('utf-8')
    elif dst == 'hex':
        if (lib.fluid_encode (buf, buf_size, dec_jobid, flux.constants.FLUID_STRING_DOTHEX) < 0):
            raise RuntimeError("error enconding id")
        return ffi.string(buf).decode('utf-8')
    elif dst == 'words':
        if (lib.fluid_encode (buf, buf_size, dec_jobid, flux.constants.FLUID_STRING_MNEMONIC) < 0):
            raise RuntimeError("error enconding id")
        return ffi.string(buf).decode('utf-8')
    

# Functions from t/python/t0010_job.py

def test_12_convert_id(self):
        variants = {
            "dec": 74859937792,
            "hex": "0000.0011.6e00.0000",
            "words": "algebra-arizona-susan--album-academy-academy",
        }
        variants["kvs"] = 'job.{}'.format(variants['hex'])

        for (src_type, src_value), (dest_type, dest_value) in \
            itertools.product(six.iteritems(variants), repeat=2):

            converted_value = job.convert_id(src_value, src_type, dest_type)
            self.assertEqual(
                converted_value,
                dest_value,
                msg="Failed to convert id of type {} into an id of type {} ({} != {})".format(
                    src_type,
                    dest_type,
                    converted_value,
                    dest_value,
                )
            )

    def test_13_convert_id_unicode(self):
        converted_value = job.convert_id(
            u"algebra-arizona-susan--album-academy-academy",
            "words",
            "hex")
        self.assertEqual(converted_value, u"0000.0011.6e00.0000")
        self.assertEqual(converted_value, b"0000.0011.6e00.0000")

    def test_05_convert_id_errors(self):
        with self.assertRaises(TypeError) as error:
            job.convert_id(5.0)

        with self.assertRaises(EnvironmentError) as error:
            job.convert_id(74859937792, src="foo")
        self.assertEqual(error.exception.errno, errno.EINVAL)

        with self.assertRaises(EnvironmentError) as error:
            job.convert_id(74859937792, dst="foo")
        self.assertEqual(error.exception.errno, errno.EINVAL)

        with self.assertRaises(EnvironmentError) as error:
            job.convert_id("foo.bar", src="kvs")
        self.assertEqual(error.exception.errno, errno.EINVAL)


# update note1: had to make a workaround for the "idle" bool in sched.c in sched-simple
# There is now a flux_watcher_t for "idle" that is separate from the one that was added for the flux emulator
# For now, I am renaming the bool "idle" to "busy" and flipping the values, but it may be better to utilize flux_watcher_t

#Update note2: flux module interface has changed. Needed to change instances of "cmb.x" to "module.x" 
# Also removing modfind because its not needed or supported in its current form

#Update note3: Removed job.py because it has been replaced with something else in newest flux ver

#Update note4: removed flags parameter from submit function calling submit_async in emu Job class
# It has been removed from submit_async.    