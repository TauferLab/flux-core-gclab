#include <flux/core.h>
#include "src/common/libutil/shortjson.h"

void get_rank (flux_rpc_t *rpc)
{
    const char *json_str;
    JSON o;
    int rank;

    if (flux_rpc_get (rpc, NULL, &json_str) < 0)
        err_exit ("flux_rpc_get");
    if (!(o = Jfromstr (json_str)) || !Jget_int (o, "rank", &rank))
        msg_exit ("response protocol error");
    printf ("rank is %d\n", rank);
    Jput (o);
}

int main (int argc, char **argv)
{
    flux_t h;
    flux_rpc_t *rpc;

    if (!(h = flux_open (NULL, 0)))
        err_exit ("flux_open");
    if (!(rpc = flux_rpc (h, "cmb.info", NULL, FLUX_NODEID_ANY, 0)))
        err_exit ("flux_rpc");
    get_rank (rpc);
    flux_rpc_destroy (rpc);
    flux_close (h);
    return (0);
}
