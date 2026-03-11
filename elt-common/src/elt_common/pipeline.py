from elt_common.constants import SOURCE_DATASET_NAME_PREFIX


def dataset_name(suffix: str) -> str:
    """Given a suffix return the full dataset name"""
    return f"{SOURCE_DATASET_NAME_PREFIX}{suffix}"


def dataset_name_v2(source_domain: str, pipeline_name: str) -> str:
    """Given a domain and pipeline name construct a dataset name"""
    return f"{source_domain}_{pipeline_name}"
