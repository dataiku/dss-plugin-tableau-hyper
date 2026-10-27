import os


def get_temporary_directory(plugin_config, operation):
    """
    Resolves temporary storage for an operation without changing process-wide defaults.

    Returns None when the setting is empty so callers retain their default location.
    Otherwise, requires an existing absolute base and creates the operation subdirectory.
    Leaves the base and operation directories available for reuse across runs.
    Raises ValueError if the base is invalid or the subdirectory cannot be created.
    """
    base_directory = (plugin_config or {}).get("temporary_directory")
    if base_directory is None or base_directory == "":
        return None

    if not os.path.isabs(base_directory) or not os.path.isdir(base_directory):
        raise ValueError(f"Temporary directory must be an existing absolute directory: {base_directory}")

    directory = os.path.join(base_directory, operation)
    try:
        os.makedirs(directory, exist_ok=True)
    except OSError as error:
        raise ValueError(f"Cannot create temporary {operation} directory at {directory}: {error}") from error
    return directory
