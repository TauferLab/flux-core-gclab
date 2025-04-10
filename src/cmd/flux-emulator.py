#!/usr/bin/env python

from __future__ import print_function
import argparse
import re
import csv
import math
import json
import logging
import heapq
from abc import ABCMeta, abstractmethod
from datetime import datetime, timedelta
from collections.abc import Sequence
from collections import namedtuple
import math
import six
import flux
import flux.job
import flux.util
import flux.kvs
import flux.constants
from flux.core.watchers import TimerWatcher
from flux.resource import Rlist
from flux.job import JournalConsumer
import time


def create_resource(res_type, count, with_child=[]):
    '''
    Creates a resource dictionary for the 

    Note: 'count' variable must be of type int. Otherwise it will cause issues during scheduling. 
    '''
    assert isinstance(
        with_child, Sequence), "child resource must be a sequence"
    assert not isinstance(
        with_child, str), "child resource must not be a string"
    assert count > 0, "resource count must be > 0"
    assert isinstance(count, int), "Count parameter must be of type int"
    res = {"type": res_type, "count": count}

    if len(with_child) > 0:
        res["with"] = with_child
    return res


def create_slot(label, count, with_child):
    slot = create_resource("slot", math.ceil(count), with_child)
    slot["label"] = label
    return slot


class Job(object):
    def __init__(self, nnodes, ncpus, submit_time, elapsed_time, timelimit, exitcode=0):
        self.nnodes = nnodes
        self.ncpus = ncpus
        self.submit_time = submit_time
        self.elapsed_time = elapsed_time
        self.timelimit = timelimit
        self.exitcode = exitcode
        self.start_time = None
        self.state_transitions = {}
        self._jobid = None
        self._jobspec = None
        self._submit_future = None
        self._start_msg = None

    @property
    def jobspec(self):
        if self._jobspec is not None:
            return self._jobspec

        assert self.ncpus % self.nnodes == 0
        core = create_resource("core", math.ceil(self.ncpus / self.nnodes))
        slot = create_slot("task", 1, [core])
        if self.nnodes > 0:
            resource_section = create_resource("node", self.nnodes, [slot])
        else:
            resource_section = slot

        jobspec = {
            "version": 1,
            "resources": [resource_section],
            "tasks": [
                {
                    "command": ["sleep", "0"],
                    "slot": "task",
                    "count": {"per_slot": 1},
                }
            ],
            "attributes": {"system": {"duration": self.timelimit}},
        }

        self._jobspec = jobspec
        return self._jobspec

    def submit(self, flux_handle):
        jobspec_json = json.dumps(self.jobspec)
        logger.log(9, jobspec_json)
        flags = 0
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("Submitting job with FLUX_JOB_DEBUG enabled")
            flags = flux.constants.FLUX_JOB_DEBUG
        self._submit_future = flux.job.submit_async(flux_handle, jobspec_json)

    @property
    def jobid(self):
        if self._jobid is None:
            if self._submit_future is None:
                raise ValueError("Job was not submitted yet. No ID assigned.")
            logger.log(9, "Waiting on jobid")
            self._jobid = flux.job.submit_get_id(self._submit_future)
            self._submit_future = None
            logger.log(9, "Received jobid: {}".format(self._jobid))
        return self._jobid

    @property
    def complete_time(self):
        if self.start_time is None:
            raise ValueError("Job has not started yet")
        return self.start_time + self.elapsed_time

    def start(self, flux_handle, start_msg, start_time):
        self.start_time = start_time
        self._start_msg = start_msg.copy()
        flux_handle.respond(
            self._start_msg, payload={
                "id": self.jobid, "type": "start", "data": {}}
        )

    def complete(self, flux_handle):
        # TODO: emit "finish" event
        flux_handle.respond(
            self._start_msg,
            payload={"id": self.jobid, "type": "finish", "data": {"status": 0}}
        )
        # TODO: emit "done" event
        flux_handle.respond(
            self._start_msg,
            payload={"id": self.jobid, "type": "release",
                     "data": {"ranks": "all", "final": True}}
        )

    def cancel(self, flux_handle):
        flux.job.RAW.cancel(flux_handle, self.jobid, "Canceled by emulator")

    def insert_apriori_events(self, simulation):
        # TODO: add priority to `add_event` so that all submits for a given time
        # can happen consecutively, followed by the waits for the jobids
        simulation.add_event(
            self.submit_time, lambda: simulation.submit_job(self))

    def record_state_transition(self, state, time):
        self.state_transitions[state] = time


class EventList(six.Iterator):
    def __init__(self):
        self.time_heap = []
        self.time_map = {}
        self._current_time = None

    def add_event(self, time, callback):
        if self._current_time is not None and time <= self._current_time:
            logger.warning(
                "Adding a new event at a time ({}) <= the current time ({})".format(
                    time, self._current_time
                )
            )

        if time in self.time_map:
            self.time_map[time].append(callback)
        else:
            new_event_list = [callback]
            self.time_map[time] = new_event_list
            heapq.heappush(self.time_heap, (time, new_event_list))

    def __len__(self):
        return len(self.time_heap)

    def __iter__(self):
        return self

    def min(self):
        if self.time_heap:
            return self.time_heap[0]
        else:
            return None

    def max(self):
        if self.time_heap:
            time = max(self.time_map.keys())
            return self.time_map[time]
        else:
            return None

    def __next__(self):
        try:
            time, event_list = heapq.heappop(self.time_heap)
            self.time_map.pop(time)
            self._current_time = time  # used for warning messages in `add_event`
            return time, event_list
        except (IndexError, KeyError):
            raise StopIteration()


class Simulation(object):
    def __init__(
            self,
            flux_handle,
            event_list,
            job_map,
            submit_job_hook=None,
            start_job_hook=None,
            complete_job_hook=None,
    ):
        self.event_list = event_list
        self.job_map = job_map
        self.current_time = 0
        self.flux_handle = flux_handle
        self.num_submits = 0
        self.num_complete = 0
        self.pending_inactivations = set()
        self.job_manager_quiescent = True
        self.submit_job_hook = submit_job_hook
        self.start_job_hook = start_job_hook
        self.complete_job_hook = complete_job_hook
        self.pending_continuation = False

    def add_event(self, time, callback):
        self.event_list.add_event(time, callback)

    def submit_job(self, job):
        self.num_submits += 1
        job.record_state_transition("SUBMITTED", self.current_time)
        if self.submit_job_hook:
            self.submit_job_hook(self, job)
        logger.debug("Submitting a new job")
        job.submit(self.flux_handle)
        self.job_map[job.jobid] = job
        logger.info("Submitted job {}".format(job.jobid))

    def start_job(self, jobid, start_msg):
        job = self.job_map[jobid]
        job.record_state_transition("STARTED", self.current_time)
        if self.start_job_hook:
            self.start_job_hook(self, job)
        job.start(self.flux_handle, start_msg, self.current_time)
        logger.info("Started job {}".format(job.jobid))
        self.add_event(job.complete_time, lambda: self.complete_job(job))
        logger.info("Registered job {} to complete at {}".format(
            job.jobid, job.complete_time))
        
        if self.pending_continuation:
            self.pending_continuation = False
            self.flux_handle.rpc("job-manager.quiescent", {"time": self.current_time}).then(
                lambda fut, arg: arg.quiescent_cb(), arg=self
            )


    def complete_job(self, job):
        self.num_complete += 1
        job.record_state_transition("COMPLETED", self.current_time)
        if self.complete_job_hook:
            self.complete_job_hook(self, job)
        job.complete(self.flux_handle)
        logger.info("Completed job {}".format(job.jobid))
        self.pending_inactivations.add(job)


    def record_job_state_transition(self, jobid, state):
        """
        Gets called by job_journal_cb to track the state of jobs to make sure they are going into the inactive state.
        """

        job = self.job_map[jobid]
        job.record_state_transition(state, self.current_time)
        if state == 'INACTIVE' and job in self.pending_inactivations:
            # Have to add this event or the emulator will try to exit before actually running the next job if there is one
            # Needs to be revisited
            self.add_event(self.current_time + 1e-9, lambda: None)
            self.pending_inactivations.remove(job)
            if self.is_quiescent():
                self.pending_continuation = False
                print("advancing from state transition")
                self.flux_handle.rpc("job-manager.quiescent", {"time": self.current_time}).then(
                lambda fut, arg: arg.quiescent_cb(), arg=self
                )

    def advance(self, *args, **kwargs):
        '''
        Primary loop for the emulator
        '''
        events_at_time = []  
        try:
            self.current_time, events_at_time = next(self.event_list)
        except StopIteration:
            if self.num_complete < self.num_submits:
                self.pending_continuation = True
                pass
            else:
                print(f"completes {self.num_complete} submits {self.num_submits}")
                logger.info(
                    "No more events in event list, running post-sim analysis")
                self.post_verification()
                logger.info("Ending simulation")
                self.flux_handle.reactor_stop(self.flux_handle.get_reactor())
                return  

        # Now events_at_time is guaranteed to be defined
        logger.info("Fast-forwarding time to {}".format(self.current_time))
        if len(events_at_time) > 0:
            for event in events_at_time:
                event()
            logger.info(
                "Sending quiescent request for time {}".format(self.current_time))
            self.flux_handle.rpc("job-manager.quiescent", {"time": self.current_time}).then(
                lambda fut, arg: arg.quiescent_cb(), arg=self
            )
        self.job_manager_quiescent = False

    def is_quiescent(self):
        return self.job_manager_quiescent and len(self.pending_inactivations) == 0

    def quiescent_cb(self):
        logger.info("Received a response indicating the system is quiescent")
        self.job_manager_quiescent = True

        if self.is_quiescent():
            self.pending_continuation = False
            logger.info("Scheduling next advance after 100ms delay")
            self.timer = TimerWatcher(
                self.flux_handle,
                0.1,       
                self.advance  
            )
            self.timer.start() 
        else:
            logger.info("Ending from quiescent")


    def post_verification(self):
        '''
        This function looks to make sure all jobs have run to completion before program ends
        If they have not, it likely means an issue with the emulator
        As a result, job event log will be output in the logger for each job that didn't complete
        '''
        for jobid, job in six.iteritems(self.job_map):
            if 'INACTIVE' not in job.state_transitions:
                logger.warning(
                    "Job {} had not reached the inactive state by simulation termination time.".format(jobid))
                eventlog = flux.job.job_kvs_lookup(
                    self.flux_handle, jobid, keys=["eventlog"])
                logger.debug(f"Job ID: {flux.job.JobID(eventlog["id"]).f58}")
                lines = eventlog["eventlog"].strip().split("\n")
                for line in lines:
                    parsed = json.loads(line)
                    pretty_str = json.dumps(parsed, indent=4)
                    logger.debug(pretty_str)

    def dump_eventlog(self):
        fieldnames = [
            "jobid", "submit", "validate", "depend", "priority", 
            "alloc", "start", "finish", "release", "free", "clean"
        ]
        
        with open("eventlog.csv", "w", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for jobid, job in six.iteritems(self.job_map):
                eventlog = flux.job.job_kvs_lookup(self.flux_handle, jobid, keys=["eventlog"])
                
                row = {"jobid": jobid}
                for event in fieldnames[1:]:
                    row[event] = ""
                
                lines = eventlog["eventlog"].strip().split("\n")
                for line in lines:
                    parsed = json.loads(line)
                    evt = parsed.get("name", parsed.get("type", "")).lower()
                    if evt in fieldnames[1:]:
                        if not row[evt]:
                            row[evt] = parsed.get("timestamp", "")
                writer.writerow(row)


def datetime_to_epoch(dt):
    return int((dt - datetime(1970, 1, 1)).total_seconds())


re_dhms = re.compile(r"^\s*(\d+)[:-](\d+):(\d+):(\d+)\s*$")
re_hms = re.compile(r"^\s*(\d+):(\d+):(\d+)\s*$")


def walltime_str_to_timedelta(walltime_str):
    (days, hours, mins, secs) = (0, 0, 0, 0)
    match = re_dhms.search(walltime_str)
    if match:
        days = int(match.group(1))
        hours = int(match.group(2))
        mins = int(match.group(3))
        secs = int(match.group(4))
    else:
        match = re_hms.search(walltime_str)
        if match:
            hours = int(match.group(1))
            mins = int(match.group(2))
            secs = int(match.group(3))
    return timedelta(days=days, hours=hours, minutes=mins, seconds=secs)


@six.add_metaclass(ABCMeta)
class JobTraceReader(object):
    def __init__(self, tracefile):
        self.tracefile = tracefile

    @abstractmethod
    def validate_trace(self):
        pass

    @abstractmethod
    def read_trace(self):
        pass


def job_from_slurm_row(row):
    kwargs = {}
    if "ExitCode" in row:
        kwargs["exitcode"] = "ExitCode"

    submit_time = datetime_to_epoch(
        datetime.strptime(row["Submit"], "%Y-%m-%dT%H:%M:%S")
    )
    elapsed = walltime_str_to_timedelta(row["Elapsed"]).total_seconds()
    if elapsed <= 0:
        logger.warning("Elapsed time ({}) <= 0".format(elapsed))
    timelimit = walltime_str_to_timedelta(row["Timelimit"]).total_seconds()
    if elapsed > timelimit:
        logger.warning(
            "Elapsed time ({}) greater than Timelimit ({})".format(
                elapsed, timelimit)
        )
    nnodes = int(row["NNodes"])
    ncpus = int(row["NCPUS"])
    if nnodes > ncpus:
        logger.warning(
            "Number of Nodes ({}) greater than Number of CPUs ({}), setting NCPUS = NNodes".format(
                nnodes, ncpus
            )
        )
        ncpus = nnodes
    elif ncpus % nnodes != 0:
        old_ncpus = ncpus
        ncpus = math.ceil(ncpus / nnodes) * nnodes
        logger.warning(
            "Number of Nodes ({}) does not evenly divide the Number of CPUs ({}), setting NCPUS to an integer multiple of the number of nodes ({})".format(
                nnodes, old_ncpus, ncpus
            )
        )

    return Job(nnodes, ncpus, submit_time, elapsed, timelimit, **kwargs)


class SacctReader(JobTraceReader):
    required_fields = ["Elapsed", "Timelimit", "Submit", "NNodes", "NCPUS"]

    def __init__(self, tracefile):
        super(SacctReader, self).__init__(tracefile)
        self.determine_delimiter()

    def determine_delimiter(self):
        """
        sacct outputs data with '|' as the delimiter by default, but ',' is a more
        common delimiter in general.  This is a simple heuristic to figure out if
        the job trace is straight from sacct or has had some post-processing
        done that converts the delimiter to a comma.
        """
        with open(self.tracefile) as infile:
            first_line = infile.readline()
        self.delim = '|' if '|' in first_line else ','

    def validate_trace(self):
        with open(self.tracefile) as infile:
            reader = csv.reader(infile, delimiter=self.delim)
            header_fields = set(next(reader))
        for req_field in SacctReader.required_fields:
            if req_field not in header_fields:
                raise ValueError("Job file is missing '{}'".format(req_field))

    def read_trace(self):
        """
        You can obtain the necessary information from the sacct command using the -o flag.
        For example: sacct -o nnodes,ncpus,timelimit,state,submit,elapsed,exitcode
        """
        with open(self.tracefile) as infile:
            lines = [line for line in infile.readlines()
                     if not line.startswith('#')]
            reader = csv.DictReader(lines, delimiter=self.delim)
            jobs = [job_from_slurm_row(row) for row in reader]
        return jobs


def insert_resource_data(flux_handle, num_ranks, cores_per_rank, hostname_pattern="node{rank}"):
    if num_ranks <= 0 or cores_per_rank <= 0:
        raise ValueError(
            "Number of ranks and cores per rank must be positive integers")

    rlist = Rlist()

    for rank in range(num_ranks):
        core_range = f'0-{cores_per_rank - 1}' if cores_per_rank > 1 else '0'
        hostname = hostname_pattern.format(rank=rank)
        rlist.add_rank(rank, hostname=hostname, cores=core_range)

    rlist_str = rlist.encode()
    rlist_json = json.loads(rlist_str)

    kvs_key = "resource.R"
    put_rc = flux.kvs.put(flux_handle, kvs_key, rlist_json)
    if put_rc is not None:
        raise ValueError(
            f"Error inserting resource data into KVS, rc={put_rc}")

    commit_rc = flux.kvs.commit(flux_handle)
    if commit_rc is not None:
        raise ValueError(
            f"Error committing resource data to KVS, rc={commit_rc}")


def get_loaded_modules(flux_handle):
    """
    Retrieve the list of loaded modules in the current Flux instance.
    """
    try:
        modules = flux_handle.rpc("module.list").get()["mods"]
        return modules
    except Exception as e:
        raise RuntimeError(f"Error retrieving loaded modules: {e}")


def load_missing_modules(flux_handle):
    # TODO: check that necessary modules are loaded
    # if not, load them
    # return an updated list of loaded modules
    loaded_modules = get_loaded_modules(flux_handle)
    pass


def reload_modules(flux_handle, scheduler, queue_policy = "fcfs"):
    '''
    To make the resource.R that we submitted to KVS earlier register with the 
    Flux instance, we need to reload both the resource module and scheduler in 
    a specific order 

    (Sched Unload -> Res Unload -> Res Load -> Sched Load)

    It has to be in that order or the scheduler becomes confused

    scheduler parameter defines if we want to use fluxion or sched simple. Eventually, we will define policies here when reloading schedulers
    '''
    sched_module = "sched-simple"
    path = None
    resource_module_path = None
    # Acquire the path to the scheduling module being used
    # Additionally, acquire the path to the resource module
    for module in get_loaded_modules(flux_handle):
        print(module)
        if "sched-simple" in module["services"]:
            sched_module = module["name"]
            path = module["path"]
        elif "sched-fluxion-qmanager" in module["name"]:
            sched_module = "fluxion"
            fluxion_qmanager_path = module["path"]
        elif "sched-fluxion-resource" in module["name"]:
            fluxion_resource_path = module["path"]
        elif "resource" in module["name"]:
            resource_module_path = module["path"]

    if path:
        print(f"{path}")
    elif fluxion_qmanager_path:
        print(fluxion_qmanager_path)
    logger.debug(
        "Reloading the '{}' and 'resource' module".format(sched_module))
    if  resource_module_path is not None:
        try:
            if sched_module == "sched-simple":
                flux_handle.rpc("module.remove", payload={
                                "name": "sched-simple"}).get()
            else:
                flux_handle.rpc("module.remove", payload={
                                "name": "sched-fluxion-qmanager"}).get()
                flux_handle.rpc("module.remove", payload={
                                "name": "sched-fluxion-resource"}).get()
            flux_handle.rpc("module.remove", payload={
                            "name": "resource"}).get()
            
        except Exception as e:
            logger.error(f"Error removing module: {e}")
        
        
        try:
            flux_handle.rpc("module.load",
                            payload={
                                "path": resource_module_path,
                                "args": ["noverify", "monitor-force-up"],
                            }).get()
            if sched_module == "sched-simple":
                flux_handle.rpc("module.load", payload={
                                "path": path, "args": []}).get()
            else:
                flux_handle.rpc("module.load", payload={
                                "path": fluxion_resource_path, "args": []}).get()
                # "queue-policy=conservative"
                flux_handle.rpc("module.load", payload={
                                "path": fluxion_qmanager_path, "args": [f"queue-policy={queue_policy}"]}).get()

        except Exception as e:
            logger.error(e)
    else:
        raise RuntimeError(
            "Unable to get scheduler path (is your scheduler module loaded?)")



def job_exception_cb(flux_handle, watcher, msg, cb_args):
    logger.warning("Detected a job exception, but not handling it")


def sim_exec_start_cb(flux_handle, watcher, msg, simulation):
    payload = msg.payload
    logger.log(9, "Received sim-exec.start request. Payload: {}".format(payload))
    jobid = payload["id"]
    simulation.start_job(jobid, msg)


def exec_hello(flux_handle):
    logger.debug("Registering sim-exec with job-manager")
    flux_handle.rpc("job-manager.exec-hello",
                    payload={"service": "sim-exec"}).get()


def service_add(f, name):
    future = f.service_register(name)
    return f.future_get(future, None)


def service_remove(f, name):
    future = f.service_unregister(name)
    return f.future_get(future, None)


def journal_event_cb(event, simulation):
    """Callback invoked for each event from JournalConsumer."""
    if event is None:
        return

    # Each 'event' is a JournalEvent with attributes like:
    #   event.name       (e.g. 'submit', 'alloc', 'start', 'cleanup', 'inactive')
    #   event.jobid
    #   event.timestamp
    #   event.jobspec    (if event.name == 'submit')
    #   event.R          (if event.name == 'alloc')
    #
    if event.name.lower() == "clean":
        simulation.record_job_state_transition(event.jobid, "INACTIVE")


def setup_journal(flux_handle, simulation):
    '''
    Function to setup a consumer for job journaling using the JournalConsumer from flux.job.journal
    '''
    consumer = JournalConsumer(flux_handle, full=False)
    consumer.set_callback(journal_event_cb, simulation)
    consumer.start()

    return consumer


def setup_watchers(flux_handle, simulation):
    watchers = []
    services = set()
    for type_mask, topic, cb, args in [
        (
            flux.constants.FLUX_MSGTYPE_REQUEST,
            "sim-exec.start",
            sim_exec_start_cb,
            simulation,
        ),
    ]:
        watcher = flux_handle.msg_watcher_create(
            cb, type_mask=type_mask, topic_glob=topic, args=args
        )
        watcher.start()
        watchers.append(watcher)
        if type_mask == flux.constants.FLUX_MSGTYPE_REQUEST:
            service_name = topic.split(".")[0]
            if service_name not in services:
                service_add(flux_handle, service_name)
                services.add(service_name)
    return watchers, services


def teardown_watchers(flux_handle, watchers, services):
    for watcher in watchers:
        watcher.stop()
    for service_name in services:
        service_remove(flux_handle, service_name)


Makespan = namedtuple('Makespan', ['beginning', 'end'])


class SimpleExec(object):
    def __init__(self, num_nodes, cores_per_node):
        self.num_nodes = num_nodes
        self.cores_per_node = cores_per_node
        self.num_free_nodes = num_nodes
        self.used_core_hours = 0

        self.makespan = Makespan(
            beginning=float('inf'),
            end=-1,
        )

    def update_makespan(self, current_time):
        if current_time < self.makespan.beginning:
            self.makespan = self.makespan._replace(beginning=current_time)
        if current_time > self.makespan.end:
            self.makespan = self.makespan._replace(end=current_time)

    def submit_job(self, simulation, job):
        self.update_makespan(simulation.current_time)

    def start_job(self, simulation, job):
        self.num_free_nodes -= job.nnodes
        if self.num_free_nodes < 0:
            logger.error("Scheduler over-subscribed nodes")
        if (job.ncpus / job.nnodes) > self.cores_per_node:
            logger.error("Scheduler over-subscribed cores on the node")

    def complete_job(self, simulation, job):
        self.num_free_nodes += job.nnodes
        self.used_core_hours += (job.ncpus * job.elapsed_time) / 3600
        self.update_makespan(simulation.current_time)

    def post_analysis(self, simulation):
        if self.makespan.beginning > self.makespan.end:
            logger.warning("Makespan beginning ({}) greater than end ({})".format(
                self.makespan.beginning,
                self.makespan.end,
            ))

        total_num_cores = self.num_nodes * self.cores_per_node
        print("Makespan (hours): {:.1f}".format(
            (self.makespan.end - self.makespan.beginning) / 3600))
        total_core_hours = (
            total_num_cores * (self.makespan.end - self.makespan.beginning)) / 3600
        print("Total Core-Hours: {:,.1f}".format(total_core_hours))
        print("Used Core-Hours: {:,.1f}".format(self.used_core_hours))
        try:
            print("Average Core-Utilization: {:.2f}%".format(
                (self.used_core_hours / total_core_hours) * 100))
        except:
            print(
                "ERROR: Total core hours is 0. Simulation likely didn't run or no jobs were submitted. ")


logger = logging.getLogger("flux-emulator")

def dump_transitions_to_csv(simulation, filename="job_transitions.csv"):
    '''
    This notes all of the submit, start, and end times of the jobs in the simulation
    '''
    fieldnames = ["jobid", "SUBMIT", "START", "FINISH", "nnodes"]
    with open(filename, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for jobid, job in simulation.job_map.items():
            row = {
                "jobid": jobid,
                "SUBMIT": job.state_transitions.get("SUBMITTED", ""),
                "START": job.state_transitions.get("STARTED", ""),
                "FINISH": job.state_transitions.get("COMPLETED", ""),
                "nnodes": job.nnodes
            }
            writer.writerow(row)

@flux.util.CLIMain(logger)
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("job_file")
    parser.add_argument("num_ranks", type=int)
    parser.add_argument("cores_per_rank", type=int)
    parser.add_argument("--log-level", type=int)
    args = parser.parse_args()

    if args.log_level:
        logger.setLevel(args.log_level)

    flux_handle = flux.Flux()

    exec_validator = SimpleExec(args.num_ranks, args.cores_per_rank)
    simulation = Simulation(
        flux_handle,
        EventList(),
        {},
        submit_job_hook=exec_validator.submit_job,
        start_job_hook=exec_validator.start_job,
        complete_job_hook=exec_validator.complete_job,
    )
    reader = SacctReader(args.job_file)
    reader.validate_trace()
    insert_resource_data(flux_handle, args.num_ranks, args.cores_per_rank)
    jobs = list(reader.read_trace())
    for job in jobs:
        job.insert_apriori_events(simulation)
    scheduler = 1
    reload_modules(flux_handle, scheduler, queue_policy="conservative")

    load_missing_modules(flux_handle )
    watchers, services = setup_watchers(flux_handle, simulation)
    consumer = setup_journal(flux_handle, simulation)
    exec_hello(flux_handle)
    simulation.advance()

    try:
        flux_handle.reactor_run(flux_handle.get_reactor(), 0)
    except Exception as e:
        logger.error(f"Reactor encountered an exception: {e}")
    try:
        teardown_watchers(flux_handle, watchers, services)
    except Exception as e:
        logger.error(f"Error tearing down watchers {e}")
    exec_validator.post_analysis(simulation)
    time.sleep(2)
    simulation.dump_eventlog()

    dump_transitions_to_csv(simulation, "job_transitions.csv")
    print("Job Life Cycle Transitions:")
    for jobid, job in simulation.job_map.items():
        print("Job {}:".format(jobid))
        for state, timestamp in job.state_transitions.items():
            print("  {} at time {}".format(state, timestamp))

    


if __name__ == "__main__":
    main()
