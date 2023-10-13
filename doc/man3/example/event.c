#include <flux/core.h>
#include "die.h"

int main (int argc, char **argv)
{
    flux_t *h;
    flux_msg_t *msg;
    const char *topic;

    if (!(h = flux_open (NULL, 0)))
        die ("could not connect to broker");
    if (flux_event_subscribe (h, "heartbeat.pulse") < 0)
        die ("error subscribing to heartbeat");
    if (!(msg = flux_recv (h, FLUX_MATCH_EVENT, 0)))
        die ("message receive error");
    if (flux_msg_get_topic (msg, &topic) < 0)
        die ("error decoding message");
    printf ("Event: %s\n", topic);

    (void)flux_event_unsubscribe (h, "heartbeat.pulse");
    flux_msg_destroy (msg);
    flux_close (h);
    return (0);
}
