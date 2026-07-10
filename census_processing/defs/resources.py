import dagster as dg


def _get_logger(context: dg.InitResourceContext) -> dg.DagsterLogManager:
    logger = context.log
    if logger is None:
        err = (
            "Context log is not available. Ensure this function is called "
            "within a Dagster resource or op context."
        )
        raise RuntimeError(err)
    return logger


class PathResource(dg.ConfigurableResource):
    data_path: str
