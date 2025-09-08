from elt_common.constants import SOURCE_DATASET_NAME_PREFIX


def dataset_name(suffix: str) -> str:
    """Given a suffix return the full dataset name"""
    return f"{SOURCE_DATASET_NAME_PREFIX}{suffix}"
