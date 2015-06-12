#ifndef _FLUX_COMPAT_REACTOR_H
#define _FLUX_COMPAT_REACTOR_H

#include "src/common/libflux/message.h"
#include "src/common/libflux/handle.h"

/* FluxMsgHandler indicates msg is "consumed" by destroying it.
 * Callbacks return 0 on success, -1 on error and set errno.
 * Error terminates reactor, and flux_reactor_start() returns -1.
 */
typedef int (*FluxMsgHandler)(flux_t h, int typemask, flux_msg_t **msg, void *arg);

typedef struct {
    int typemask;
    const char *pattern;
    FluxMsgHandler cb;
} msghandler_t;

/* Register a FluxMsgHandler callback to be called whenever a message
 * matching typemask and pattern (glob) is received.  The callback is
 * added to the beginning of the msghandler list.
 */
int flux_msghandler_add (flux_t h, int typemask, const char *pattern,
                         FluxMsgHandler cb, void *arg);

/* Register a batch of FluxMsgHandler's
 */
int flux_msghandler_addvec (flux_t h, msghandler_t *handlers, int len,
                            void *arg);

/* Unregister a FluxMsgHandler callback.  Only the first callback with
 * identical typemask and pattern is removed.
 */
void flux_msghandler_remove (flux_t h, int typemask, const char *pattern);

#endif /* !_FLUX_COMPAT_REACTOR_H */

/*
 * vi:tabstop=4 shiftwidth=4 expandtab
 */
