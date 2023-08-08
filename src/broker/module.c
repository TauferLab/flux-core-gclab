/************************************************************\
 * Copyright 2014 Lawrence Livermore National Security, LLC
 * (c.f. AUTHORS, NOTICE.LLNS, COPYING)
 *
 * This file is part of the Flux resource manager framework.
 * For details, see https://github.com/flux-framework.
 *
 * SPDX-License-Identifier: LGPL-3.0
\************************************************************/

#if HAVE_CONFIG_H
#include "config.h"
#endif
#include <dlfcn.h>
#include <argz.h>
#include <czmq.h>
#undef streq // redefined by ccan/str/str.h below
#include <uuid.h>
#ifndef UUID_STR_LEN
#define UUID_STR_LEN 37     // defined in later libuuid headers
#endif
#include <flux/core.h>
#include <jansson.h>
#if HAVE_CALIPER
#include <caliper/cali.h>
#include <sys/syscall.h>
#endif

#include "src/common/libzmqutil/msg_zsock.h"
#include "src/common/libzmqutil/reactor.h"
#include "src/common/libczmqcontainers/czmq_containers.h"
#include "src/common/libutil/log.h"
#include "src/common/libutil/errprintf.h"
#include "src/common/libutil/errno_safe.h"
#include "ccan/str/str.h"

#include "module.h"
#include "modservice.h"

struct broker_module {
    flux_watcher_t *broker_w;

    double lastseen;

    zsock_t *sock;          /* broker end of PAIR socket */
    struct flux_msg_cred cred; /* cred of connection */

    uuid_t uuid;            /* uuid for unique request sender identity */
    char uuid_str[UUID_STR_LEN];
    char *parent_uuid_str;
    int rank;
    attr_t *attrs;
    const flux_conf_t *conf;
    pthread_t t;            /* module thread */
    mod_main_f *main;       /* dlopened mod_main() */
    char *name;
    char *path;             /* retain the full path as a key for lookup */
    void *dso;              /* reference on dlopened module */
    size_t argz_len;
    char *argz;
    int status;
    int errnum;
    bool muted;             /* module is under directive 42, no new messages */

    modpoller_cb_f poller_cb;
    void *poller_arg;
    module_status_cb_f status_cb;
    void *status_arg;

    struct disconnect *disconnect;

    zlist_t *rmmod;
    flux_msg_t *insmod;

    flux_t *h;               /* module's handle */

    zlist_t *subs;          /* subscription strings */
};

static int setup_module_profiling (module_t *p)
{
#if HAVE_CALIPER
    cali_begin_string_byname ("flux.type", "module");
    cali_begin_int_byname ("flux.tid", syscall (SYS_gettid));
    cali_begin_int_byname ("flux.rank", p->rank);
    cali_begin_string_byname ("flux.name", p->name);
#endif
    return (0);
}

/*  Synchronize the FINALIZING state with the broker, so the broker
 *   can stop messages to this module until we're fully shutdown.
 */
static int module_finalizing (module_t *p)
{
    flux_future_t *f;

    if (!(f = flux_rpc_pack (p->h,
                             "broker.module-status",
                             FLUX_NODEID_ANY,
                             0,
                             "{s:i}",
                             "status", FLUX_MODSTATE_FINALIZING))
        || flux_rpc_get (f, NULL)) {
        flux_log_error (p->h, "broker.module-status FINALIZING error");
        flux_future_destroy (f);
        return -1;
    }
    flux_future_destroy (f);
    return 0;
}

static void *module_thread (void *arg)
{
    module_t *p = arg;
    sigset_t signal_set;
    int errnum;
    char *uri = NULL;
    char **av = NULL;
    int ac;
    int mod_main_errno = 0;
    flux_msg_t *msg;
    flux_conf_t *conf;
    flux_future_t *f;

    setup_module_profiling (p);

    /* Connect to broker socket, enable logging, register built-in services
     */
    if (asprintf (&uri, "shmem://%s", p->uuid_str) < 0) {
        log_err ("asprintf");
        goto done;
    }
    if (!(p->h = flux_open (uri, 0))) {
        log_err ("flux_open %s", uri);
        goto done;
    }
    if (attr_cache_immutables (p->attrs, p->h) < 0) {
        log_err ("%s: error priming broker attribute cache", p->name);
        goto done;
    }
    flux_log_set_appname (p->h, p->name);
    /* Copy the broker's config object so that modules
     * can call flux_get_conf() and expect it to always succeed.
     */
    if (!(conf = flux_conf_copy (p->conf))
            || flux_set_conf (p->h, conf) < 0) {
        flux_conf_decref (conf);
        log_err ("%s: error duplicating config object", p->name);
        goto done;
    }
    if (modservice_register (p->h, p) < 0) {
        log_err ("%s: modservice_register", p->name);
        goto done;
    }

    /* Block all signals
     */
    if (sigfillset (&signal_set) < 0) {
        log_err ("%s: sigfillset", p->name);
        goto done;
    }
    if ((errnum = pthread_sigmask (SIG_BLOCK, &signal_set, NULL)) != 0) {
        log_errn (errnum, "pthread_sigmask");
        goto done;
    }

    /* Run the module's main().
     */
    ac = argz_count (p->argz, p->argz_len);
    if (!(av = calloc (1, sizeof (av[0]) * (ac + 1)))) {
        log_err ("calloc");
        goto done;
    }
    argz_extract (p->argz, p->argz_len, av);
    if (p->main (p->h, ac, av) < 0) {
        mod_main_errno = errno;
        if (mod_main_errno == 0)
            mod_main_errno = ECONNRESET;
        flux_log (p->h, LOG_CRIT, "module exiting abnormally");
    }

    /* Before processing unhandled requests, ensure that this module
     * is "muted" in the broker. This ensures the broker won't try to
     * feed a message to this module after we've closed the handle,
     * which could cause the broker to block.
     */
    if (module_finalizing (p) < 0)
        flux_log_error (p->h, "failed to set module state to finalizing");

    /* If any unhandled requests were received during shutdown,
     * respond to them now with ENOSYS.
     */
    while ((msg = flux_recv (p->h, FLUX_MATCH_REQUEST, FLUX_O_NONBLOCK))) {
        const char *topic = "unknown";
        (void)flux_msg_get_topic (msg, &topic);
        flux_log (p->h, LOG_DEBUG, "responding to post-shutdown %s", topic);
        if (flux_respond_error (p->h, msg, ENOSYS, NULL) < 0)
            flux_log_error (p->h, "responding to post-shutdown %s", topic);
        flux_msg_destroy (msg);
    }
    if (!(f = flux_rpc_pack (p->h,
                             "broker.module-status",
                             FLUX_NODEID_ANY,
                             FLUX_RPC_NORESPONSE,
                             "{s:i s:i}",
                             "status", FLUX_MODSTATE_EXITED,
                             "errnum", mod_main_errno))) {
        flux_log_error (p->h, "broker.module-status EXITED error");
        goto done;
    }
    flux_future_destroy (f);
done:
    free (uri);
    free (av);
    flux_close (p->h);
    p->h = NULL;
    return NULL;
}

static void module_cb (flux_reactor_t *r,
                       flux_watcher_t *w,
                       int revents,
                       void *arg)
{
    module_t *p = arg;
    p->lastseen = flux_reactor_now (r);
    if (p->poller_cb)
        p->poller_cb (p, p->poller_arg);
}

static char *module_name_from_path (const char *s)
{
    char *path, *name, *cpy;
    char *cp;

    if (!(path = strdup (s))
        || !(name = basename (path)))
        goto error;
    if ((cp = strstr (name, ".so")))
        *cp = '\0';
    if (!(cpy = strdup (name)))
        goto error;
    free (path);
    return cpy;
error:
    ERRNO_SAFE_WRAP (free, path);
    return NULL;
}

module_t *module_create (flux_t *h,
                         const char *parent_uuid,
                         const char *name, // may be NULL
                         const char *path,
                         int rank,
                         attr_t *attrs,
                         json_t *args,
                         flux_error_t *error)
{
    module_t *p;
    void *dso;
    const char **mod_namep;
    mod_main_f *mod_main;

    dlerror ();
    if (!(dso = dlopen (path, RTLD_NOW | RTLD_GLOBAL | FLUX_DEEPBIND))) {
        errprintf (error, "%s", dlerror ());
        errno = ENOENT;
        return NULL;
    }
    if (!(mod_main = dlsym (dso, "mod_main"))) {
        errprintf (error, "module does not define mod_main()");
        dlclose (dso);
        errno = EINVAL;
        return NULL;
    }
    if (!(p = calloc (1, sizeof (*p))))
        goto nomem;
    p->main = mod_main;
    p->dso = dso;
    p->rank = rank;
    p->attrs = attrs;
    p->conf = flux_get_conf (h);
    if (!(p->parent_uuid_str = strdup (parent_uuid)))
        goto nomem;
    strncpy (p->uuid_str, parent_uuid, sizeof (p->uuid_str) - 1);
    if (args) {
        size_t index;
        json_t *entry;

        json_array_foreach (args, index, entry) {
            const char *s = json_string_value (entry);
            if (s && (argz_add (&p->argz, &p->argz_len, s) != 0))
                goto nomem;
        }
    }
    if (!(p->path = strdup (path))
        || !(p->rmmod = zlist_new ())
        || !(p->subs = zlist_new ()))
        goto nomem;
    if (name) {
        if (!(p->name = strdup (name)))
            goto nomem;
    }
    else {
        if (!(p->name = module_name_from_path (path)))
            goto nomem;
    }
    /* Handle legacy 'mod_name' symbol - not recommended for new modules
     * but double check that it's sane if present.
     */
    if ((mod_namep = dlsym (p->dso, "mod_name")) && *mod_namep != NULL) {
        if (!streq (*mod_namep, p->name)) {
            errprintf (error, "mod_name %s != name %s", *mod_namep, name);
            errno = EINVAL;
            goto cleanup;
        }
    }
    uuid_generate (p->uuid);
    uuid_unparse (p->uuid, p->uuid_str);

     /* Broker end of PAIR socket is opened here.
     */
    if (!(p->sock = zsock_new_pair (NULL))) {
        errprintf (error, "could not create zsock for %s", p->name);
        goto cleanup;
    }
    zsock_set_unbounded (p->sock);
    zsock_set_linger (p->sock, 5);
    if (zsock_bind (p->sock, "inproc://%s", module_get_uuid (p)) < 0) {
        errprintf (error, "zsock_bind inproc://%s", module_get_uuid (p));
        goto cleanup;
    }
    if (!(p->broker_w = zmqutil_watcher_create (flux_get_reactor (h),
                                                p->sock,
                                                FLUX_POLLIN,
                                                module_cb,
                                                p))) {
        errprintf (error, "could not create %s zsock watcher", p->name);
        goto cleanup;
    }
    /* Set creds for connection.
     * Since this is a point to point connection between broker threads,
     * credentials are always those of the instance owner.
     */
    p->cred.userid = getuid ();
    p->cred.rolemask = FLUX_ROLE_OWNER | FLUX_ROLE_LOCAL;

    return p;
nomem:
    errprintf (error, "out of memory");
    errno = ENOMEM;
cleanup:
    module_destroy (p);
    return NULL;
}

const char *module_get_path (module_t *p)
{
    return p && p->path ? p->path : "unknown";
}

const char *module_get_name (module_t *p)
{
    return p && p->name ? p->name : "unknown";
}

const char *module_get_uuid (module_t *p)
{
    return p ? p->uuid_str : "unknown";
}

double module_get_lastseen (module_t *p)
{
    return p ? p->lastseen : 0;
}

int module_get_status (module_t *p)
{
    return p ? p->status : 0;
}

flux_msg_t *module_recvmsg (module_t *p)
{
    flux_msg_t *msg = NULL;
    int type;
    struct flux_msg_cred cred;

    if (!(msg = zmqutil_msg_recv (p->sock)))
        goto error;
    if (flux_msg_get_type (msg, &type) < 0)
        goto error;
    switch (type) {
        case FLUX_MSGTYPE_RESPONSE:
            if (flux_msg_route_delete_last (msg) < 0)
                goto error;
            break;
        case FLUX_MSGTYPE_REQUEST:
        case FLUX_MSGTYPE_EVENT:
            if (flux_msg_route_push (msg, p->uuid_str) < 0)
                goto error;
            break;
        default:
            break;
    }
    /* All shmem:// connections to the broker have FLUX_ROLE_OWNER
     * and are "authenticated" as the instance owner.
     * Allow modules so endowed to change the userid/rolemask on messages when
     * sending on behalf of other users.  This is necessary for connectors
     * implemented as DSOs.
     */
    assert ((p->cred.rolemask & FLUX_ROLE_OWNER));
    if (flux_msg_get_cred (msg, &cred) < 0)
        goto error;
    if (cred.userid == FLUX_USERID_UNKNOWN)
        cred.userid = p->cred.userid;
    if (cred.rolemask == FLUX_ROLE_NONE)
        cred.rolemask = p->cred.rolemask;
    if (flux_msg_set_cred (msg, cred) < 0)
        goto error;
    return msg;
error:
    flux_msg_destroy (msg);
    return NULL;
}

int module_sendmsg (module_t *p, const flux_msg_t *msg)
{
    flux_msg_t *cpy = NULL;
    int type;
    const char *topic;
    int rc = -1;

    if (!msg)
        return 0;
    if (flux_msg_get_type (msg, &type) < 0
        || flux_msg_get_topic (msg, &topic) < 0)
        return -1;
    /* Muted modules only accept response to broker.module-status
     */
    if (p->muted) {
        if (type != FLUX_MSGTYPE_RESPONSE
            || !streq (topic, "broker.module-status")) {
            errno = ENOSYS;
            return -1;
        }
    }
    switch (type) {
        case FLUX_MSGTYPE_REQUEST: { /* simulate DEALER socket */
            if (!(cpy = flux_msg_copy (msg, true)))
                goto done;
            if (flux_msg_route_push (cpy, p->parent_uuid_str) < 0)
                goto done;
            if (zmqutil_msg_send (p->sock, cpy) < 0)
                goto done;
            break;
        }
        case FLUX_MSGTYPE_RESPONSE: { /* simulate ROUTER socket */
            if (!(cpy = flux_msg_copy (msg, true)))
                goto done;
            if (flux_msg_route_delete_last (cpy) < 0)
                goto done;
            if (zmqutil_msg_send (p->sock, cpy) < 0)
                goto done;
            break;
        }
        default:
            if (zmqutil_msg_send (p->sock, msg) < 0)
                goto done;
            break;
    }
    rc = 0;
done:
    flux_msg_destroy (cpy);
    return rc;
}

int module_disconnect_arm (module_t *p,
                           const flux_msg_t *msg,
                           disconnect_send_f cb,
                           void *arg)
{
    if (!p->disconnect) {
        if (!(p->disconnect = disconnect_create (cb, arg)))
            return -1;
    }
    if (disconnect_arm (p->disconnect, msg) < 0)
        return -1;
    return 0;
}

void module_destroy (module_t *p)
{
    int e;
    void *res;
    int saved_errno = errno;

    if (!p)
        return;

    if (p->t) {
        if ((e = pthread_join (p->t, &res)) != 0)
            log_errn_exit (e, "pthread_cancel");
        if (p->status != FLUX_MODSTATE_EXITED) {
            /* Calls broker.c module_status_cb() => service_remove_byuuid()
             * and releases a reference on 'p'.  Without this, disconnect
             * requests sent when other modules are destroyed can still find
             * this service name and trigger a use-after-free segfault.
             * See also: flux-framework/flux-core#4564.
             */
            module_set_status (p, FLUX_MODSTATE_EXITED);
        }
    }

    /* Send disconnect messages to services used by this module.
     */
    disconnect_destroy (p->disconnect);

    flux_watcher_stop (p->broker_w);
    flux_watcher_destroy (p->broker_w);
    zsock_destroy (&p->sock);

#ifndef __SANITIZE_ADDRESS__
    dlclose (p->dso);
#endif
    free (p->argz);
    free (p->name);
    free (p->path);
    free (p->parent_uuid_str);
    if (p->rmmod) {
        flux_msg_t *msg;
        while ((msg = zlist_pop (p->rmmod)))
            flux_msg_destroy (msg);
    }
    flux_msg_destroy (p->insmod);
    if (p->subs) {
        char *s;
        while ((s = zlist_pop (p->subs)))
            free (s);
        zlist_destroy (&p->subs);
    }
    zlist_destroy (&p->rmmod);
    free (p);
    errno = saved_errno;
}

/* Send shutdown request, broker to module.
 */
int module_stop (module_t *p, flux_t *h)
{
    char *topic = NULL;
    flux_future_t *f = NULL;
    int rc = -1;

    if (asprintf (&topic, "%s.shutdown", p->name) < 0)
        goto done;
    if (!(f = flux_rpc (h,
                        topic,
                        NULL,
                        FLUX_NODEID_ANY,
                        FLUX_RPC_NORESPONSE)))
        goto done;
    rc = 0;
done:
    free (topic);
    flux_future_destroy (f);
    return rc;
}

void module_mute (module_t *p)
{
    p->muted = true;
}

int module_start (module_t *p)
{
    int errnum;
    int rc = -1;

    flux_watcher_start (p->broker_w);
    if ((errnum = pthread_create (&p->t, NULL, module_thread, p))) {
        errno = errnum;
        goto done;
    }
    rc = 0;
done:
    return rc;
}

int module_cancel (module_t *p, flux_error_t *error)
{
    if (p->t) {
        int e;
        if ((e = pthread_cancel (p->t)) != 0 && e != ESRCH) {
            errprintf (error, "pthread_cancel: %s", strerror (e));
            return -1;
        }
    }
    return 0;
}

void module_set_poller_cb (module_t *p, modpoller_cb_f cb, void *arg)
{
    p->poller_cb = cb;
    p->poller_arg = arg;
}

void module_set_status_cb (module_t *p, module_status_cb_f cb, void *arg)
{
    p->status_cb = cb;
    p->status_arg = arg;
}

void module_set_status (module_t *p, int new_status)
{
    assert (new_status != FLUX_MODSTATE_INIT);  /* illegal state transition */
    assert (p->status != FLUX_MODSTATE_EXITED); /* illegal state transition */
    int prev_status = p->status;
    p->status = new_status;
    if (p->status_cb)
        p->status_cb (p, prev_status, p->status_arg);
}

void module_set_errnum (module_t *p, int errnum)
{
    p->errnum = errnum;
}

int module_get_errnum (module_t *p)
{
    return p->errnum;
}

int module_push_rmmod (module_t *p, const flux_msg_t *msg)
{
    flux_msg_t *cpy = flux_msg_copy (msg, false);
    if (!cpy)
        return -1;
    if (zlist_push (p->rmmod, cpy) < 0) {
        errno = ENOMEM;
        return -1;
    }
    return 0;
}

flux_msg_t *module_pop_rmmod (module_t *p)
{
    return zlist_pop (p->rmmod);
}

/* There can be only one.
 */
int module_push_insmod (module_t *p, const flux_msg_t *msg)
{
    flux_msg_t *cpy = flux_msg_copy (msg, false);
    if (!cpy)
        return -1;
    if (p->insmod)
        flux_msg_destroy (p->insmod);
    p->insmod = cpy;
    return 0;
}

flux_msg_t *module_pop_insmod (module_t *p)
{
    flux_msg_t *msg = p->insmod;
    p->insmod = NULL;
    return msg;
}

int module_subscribe (module_t *p, const char *topic)
{
    char *cpy;
    int rc = -1;

    if (!(cpy = strdup (topic)))
        goto done;
    if (zlist_push (p->subs, cpy) < 0) {
        free (cpy);
        errno = ENOMEM;
        goto done;
    }
    rc = 0;
done:
    return rc;
}

void module_unsubscribe (module_t *p, const char *topic)
{
    char *s;

    s = zlist_first (p->subs);
    while (s) {
        if (streq (topic, s)) {
            zlist_remove (p->subs, s);
            free (s);
            break;
        }
        s = zlist_next (p->subs);
    }
}

static bool match_sub (module_t *p, const char *topic)
{
    char *s = zlist_first (p->subs);

    while (s) {
        if (strstarts (topic, s))
            return true;
        s = zlist_next (p->subs);
    }
    return false;
}

int module_event_cast (module_t *p, const flux_msg_t *msg)
{
    const char *topic;

    if (flux_msg_get_topic (msg, &topic) < 0)
        return -1;
    if (match_sub (p, topic)) {
        if (module_sendmsg (p, msg) < 0)
            return -1;
    }
    return 0;
}

/*
 * vi:tabstop=4 shiftwidth=4 expandtab
 */
