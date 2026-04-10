"""
ChromaDB unified configuration module.
Compatible with ChromaDB >= 0.4.x through 1.5.x+.
Automatically adapts to the installed version.
"""
import os
import platform
import logging

logger = logging.getLogger(__name__)


def _get_chromadb_version() -> tuple:
    """
    Detect the installed ChromaDB version.

    Returns:
        tuple: (major, minor, patch) version numbers, e.g. (1, 5, 6)
    """
    try:
        import chromadb
        version_str = getattr(chromadb, '__version__', '0.0.0')
        parts = version_str.split('.')
        return tuple(int(p) for p in parts[:3])
    except Exception:
        return (0, 0, 0)


def is_windows_11() -> bool:
    """
    Detect whether the OS is Windows 11.

    Returns:
        bool: True if Windows 11, False otherwise.
    """
    if platform.system() != "Windows":
        return False

    version = platform.version()
    try:
        version_parts = version.split('.')
        if len(version_parts) >= 3:
            build_number = int(version_parts[2])
            return build_number >= 22000
    except (ValueError, IndexError):
        pass

    return False


def _create_ephemeral_client():
    """
    Create an in-memory (ephemeral) ChromaDB client.
    Works with ChromaDB >= 0.4.x.

    Returns:
        ChromaDB client instance
    """
    import chromadb

    # Preferred: EphemeralClient (available since 0.4.x)
    if hasattr(chromadb, 'EphemeralClient'):
        return chromadb.EphemeralClient()

    # Fallback: Client() with minimal settings
    from chromadb.config import Settings
    settings = Settings(
        anonymized_telemetry=False,
        is_persistent=False
    )
    return chromadb.Client(settings)


def _create_client_with_settings():
    """
    Create a ChromaDB client with explicit settings.
    Adapts parameters based on the installed version.

    Returns:
        ChromaDB client instance
    """
    import chromadb
    from chromadb.config import Settings

    version = _get_chromadb_version()

    if version >= (1, 0, 0):
        # ChromaDB >= 1.0: use EphemeralClient, avoid deprecated params
        settings = Settings(
            anonymized_telemetry=False,
            is_persistent=False,
            allow_reset=True
        )
        if hasattr(chromadb, 'EphemeralClient'):
            return chromadb.EphemeralClient(settings=settings)
        return chromadb.Client(settings)

    elif version >= (0, 4, 0):
        # ChromaDB 0.4.x - 0.5.x: EphemeralClient or Client with settings
        settings = Settings(
            anonymized_telemetry=False,
            is_persistent=False,
            allow_reset=True
        )
        if hasattr(chromadb, 'EphemeralClient'):
            return chromadb.EphemeralClient(settings=settings)
        return chromadb.Client(settings)

    else:
        # Very old ChromaDB: try basic Client()
        return chromadb.Client()


def get_win10_chromadb_client():
    """
    Get a ChromaDB client optimized for Windows 10.

    Returns:
        ChromaDB client instance
    """
    return _create_ephemeral_client()


def get_win11_chromadb_client():
    """
    Get a ChromaDB client optimized for Windows 11.

    Returns:
        ChromaDB client instance
    """
    return _create_ephemeral_client()


def get_optimal_chromadb_client():
    """
    Automatically select the optimal ChromaDB configuration
    based on the OS and installed ChromaDB version.

    Returns:
        ChromaDB client instance

    Raises:
        RuntimeError: If all initialization attempts fail.
    """
    import chromadb

    version = _get_chromadb_version()
    system = platform.system()
    logger.info(
        f"📚 [ChromaDB] Initializing: version={'.'.join(str(v) for v in version)}, "
        f"os={system}"
    )

    # Attempt 1: EphemeralClient (best for in-memory usage)
    try:
        if hasattr(chromadb, 'EphemeralClient'):
            client = chromadb.EphemeralClient()
            logger.info("📚 [ChromaDB] EphemeralClient initialized successfully")
            return client
    except Exception as e:
        logger.warning(f"⚠️ [ChromaDB] EphemeralClient failed: {e}")

    # Attempt 2: Client with version-aware settings
    try:
        client = _create_client_with_settings()
        logger.info("📚 [ChromaDB] Client with settings initialized successfully")
        return client
    except Exception as e:
        logger.warning(f"⚠️ [ChromaDB] Client with settings failed: {e}")

    # Attempt 3: Client with minimal settings
    try:
        from chromadb.config import Settings
        settings = Settings(
            anonymized_telemetry=False,
            is_persistent=False
        )
        client = chromadb.Client(settings)
        logger.info("📚 [ChromaDB] Client with minimal settings initialized")
        return client
    except Exception as e:
        logger.warning(f"⚠️ [ChromaDB] Client with minimal settings failed: {e}")

    # Attempt 4: bare Client()
    try:
        client = chromadb.Client()
        logger.warning("⚠️ [ChromaDB] Using bare Client() as last resort")
        return client
    except Exception as e:
        logger.error(f"❌ [ChromaDB] All initialization attempts failed: {e}")
        raise RuntimeError(
            f"ChromaDB initialization failed (version={'.'.join(str(v) for v in version)}). "
            f"Last error: {e}"
        )


# Exported symbols
__all__ = [
    'get_optimal_chromadb_client',
    'get_win10_chromadb_client',
    'get_win11_chromadb_client',
    'is_windows_11'
]
