/************************************************************\
 * Copyright 2019 Lawrence Livermore National Security, LLC
 * (c.f. AUTHORS, NOTICE.LLNS, COPYING)
 *
 * This file is part of the Flux resource manager framework.
 * For details, see https://github.com/flux-framework.
 *
 * SPDX-License-Identifier: LGPL-3.0
\************************************************************/

/* servhash.c - proxy service registration
 *
 * The broker offers dynamic service registration to direct peers.
 * A router must maintain its own hash of service registrations,
 * manage broker/upstream registrations on behalf of its clients,
 * and route request messages to its clients.  This class provides
 * support for router implementations.
 *
 * Notes
 * - service.add and service.remove requests intercepted from the client
 *   should be directed to servhash_add() and servhash_remove().
 * - servhash_add() and servhash_remove() asynchronously request upstream reg/
 *   unreg, add/remove a servhash->services entry, and respond to the client
 * - servhash_match() can match a request message to a client uuid
 * - when a client disconnects, the router must call servhash_disconnect()
 *   with its uuid so that any services can be unregistered
 * - we have to handle some corner cases like client disconnects with
 *   an add or remove request pending, etc.
 * - when the router shuts down, servhash_destroy() unregisters all services.
 */

#if HAVE_CONFIG_H
#include "config.h"
#endif
#include <stdlib.h>
#include <czmq.h>
#include <flux/core.h>

#include "src/common/libutil/errno_safe.h"

#include "servhash.h"

struct servhash_entry {
    char *name;
    char *uuid;                 // owner
    struct flux_match match;
    char *glob;
    struct servhash *sh;
    const flux_msg_t *add_request;
    const flux_msg_t *remove_request;
    flux_future_t *f_add;
    flux_future_t *f_remove;
    unsigned char live:1;
};

struct servhash {
    flux_t *h;
    zhashx_t *services;         // name => servhash_entry
    respond_f respond_cb;
    void *respond_arg;
};

static bool needs_unregister (struct servhash_entry *entry)
{
    if (!entry->live && entry->f_add && !flux_future_is_ready (entry->f_add))
        return true; // pending service.add request
    if (entry->live && !entry->f_remove)
        return true; // service.add successful, service.remove not sent
    return false;
}

/* Destructor for a service.
 * Be sure that any registered services are cleaned up on the broker
 * by sending an "open loop" unregister request if needed.
 */
static void servhash_entry_destroy (struct servhash_entry *entry)
{
    if (entry) {
        if (needs_unregister (entry)) {
            flux_future_t *f;
            f = flux_service_unregister (entry->sh->h, entry->name);
            flux_future_destroy (f);
        }
        flux_future_destroy (entry->f_add);
        flux_future_destroy (entry->f_remove);
        flux_msg_decref (entry->add_request);
        flux_msg_decref (entry->remove_request);
        ERRNO_SAFE_WRAP (free, entry->name);
        ERRNO_SAFE_WRAP (free, entry->uuid);
        ERRNO_SAFE_WRAP (free, entry->glob);
        ERRNO_SAFE_WRAP (free, entry);
    }
}

// zhashx_destructor_fn footprint (wrapper)
static void servhash_entry_destructor (void **item)
{
    if (item) {
        servhash_entry_destroy (*item);
        *item = NULL;
    }
}

static struct servhash_entry *servhash_entry_create (const char *name,
                                                     const char *uuid)
{
    struct servhash_entry *entry;

    if (!(entry = calloc (1, sizeof (*entry))))
        return NULL;
    if (!(entry->name = strdup (name)) || !(entry->uuid = strdup (uuid)))
        goto error;
    if (asprintf (&entry->glob, "%s.*", name) < 0)
        goto error;
    entry->match = FLUX_MATCH_REQUEST;
    entry->match.topic_glob = entry->glob;
    return entry;
error:
    servhash_entry_destroy (entry);
    return NULL;
}

static void add_continuation (flux_future_t *f, void *arg)
{
    struct servhash_entry *entry = arg;
    struct servhash *sh = entry->sh;
    int errnum = 0;

    if (flux_future_get (f, NULL) < 0) {
        errnum = errno;
        goto done;
    }
    entry->live = 1;
done:
    if (sh->respond_cb)
        sh->respond_cb (entry->add_request,
                        entry->uuid,
                        errnum,
                        sh->respond_arg);
    if (errnum != 0)
        zhashx_delete (sh->services, entry->name);
}

int servhash_add (struct servhash *sh,
                  const char *name,
                  const char *uuid,
                  flux_msg_t *msg)
{
    struct servhash_entry *entry;

    if (!sh || !name || !uuid || !msg) {
        errno = EINVAL;
        return -1;
    }
    if ((entry = zhashx_lookup (sh->services, name))) {
        errno = EEXIST;
        return -1;
    }
    if (!(entry = servhash_entry_create (name, uuid)))
        return -1;
    entry->sh = sh;
    entry->add_request = flux_msg_incref (msg);
    if (!(entry->f_add = flux_service_register (sh->h, name)))
        goto error;
    if (flux_future_then (entry->f_add, -1, add_continuation, entry) < 0)
        goto error;
    zhashx_update (sh->services, name, entry);
    return 0;
error:
    servhash_entry_destroy (entry);
    return -1;
}

static void remove_continuation (flux_future_t *f, void *arg)
{
    struct servhash_entry *entry = arg;
    struct servhash *sh = entry->sh;
    int errnum = 0;

    if (flux_future_get (f, NULL) < 0) {
        errnum = errno;
        goto done;
    }
    entry->live = 0;
done:
    if (sh->respond_cb)
        sh->respond_cb (entry->remove_request,
                        entry->uuid,
                        errnum,
                        sh->respond_arg);
    zhashx_delete (sh->services, entry->name);
}

int servhash_remove (struct servhash *sh,
                     const char *name,
                     const char *uuid,
                     flux_msg_t *msg)
{
    struct servhash_entry *entry;

    if (!sh || !name || !uuid || !msg) {
        errno = EINVAL;
        return -1;
    }
    if (!(entry = zhashx_lookup (sh->services, name))
            || strcmp (entry->uuid, uuid) != 0
            || entry->f_remove != NULL) {
        errno = ENOENT;
        return -1;
    }
    entry->remove_request = flux_msg_incref (msg);
    if (!(entry->f_remove = flux_service_unregister (sh->h, name)))
        return -1;
    if (flux_future_then (entry->f_remove, -1, remove_continuation, entry) < 0)
        goto error;
    return 0;
error:
    ERRNO_SAFE_WRAP (zhashx_delete, sh->services, name);
    return -1;
}

void servhash_disconnect (struct servhash *sh, const char *uuid)
{
    struct servhash_entry *entry;
    zlistx_t *keys;
    char *key;

    if (sh && uuid && (keys = zhashx_keys (sh->services))) {
        key = zlistx_first (keys);
        while (key) {
            entry = zhashx_lookup (sh->services, key);
            if (!strcmp (entry->uuid, uuid))
                zhashx_delete (sh->services, key);
            key = zlistx_next (keys);
        }
        zlistx_destroy (&keys);
    }
}

int servhash_match (struct servhash *sh,
                    const flux_msg_t *msg,
                    const char **uuid)
{
    struct servhash_entry *entry;

    if (!sh || !msg || !uuid) {
        errno = EINVAL;
        return -1;
    }
    entry = zhashx_first (sh->services);
    while ((entry)) {
        if (flux_msg_cmp (msg, entry->match))
            break;
        entry = zhashx_next (sh->services);
    }
    if (!entry) {
        errno = ENOENT;
        return -1;
    }
    *uuid = entry->uuid;
    return 0;
}

void servhash_set_respond (struct servhash *sh, respond_f cb, void *arg)
{
    if (sh) {
        sh->respond_cb = cb;
        sh->respond_arg = arg;
    }
}

void servhash_destroy (struct servhash *sh)
{
    if (sh) {
        ERRNO_SAFE_WRAP (zhashx_destroy, &sh->services);
        ERRNO_SAFE_WRAP (free, sh);
    }
}

struct servhash *servhash_create (flux_t *h)
{
    struct servhash *sh;

    if (!h) {
        errno = EINVAL;
        return NULL;
    }
    if (!(sh = calloc (1, sizeof (*sh))))
        return NULL;
    if (!(sh->services = zhashx_new ()))
        goto error;
    sh->h = h;
    zhashx_set_destructor (sh->services, servhash_entry_destructor);
    return sh;
error:
    servhash_destroy (sh);
    return NULL;
}

/*
 * vi:tabstop=4 shiftwidth=4 expandtab
 */
