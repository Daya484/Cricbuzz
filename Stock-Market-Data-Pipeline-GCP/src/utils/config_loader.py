"""
Environment-aware Configuration Loader.

Merges base.yaml with environment-specific config (dv.yaml / qa.yaml / pd.yaml).
Environment is determined by:
  1. Explicit parameter
  2. ENV environment variable
  3. Defaults to "dv"

Usage:
    from src.utils.config_loader import load_config

    config = load_config()                  # uses ENV variable or defaults to "dv"
    config = load_config(env="qa")          # explicit QA
    config = load_config(env="pd")          # explicit Prod
"""

import copy
import logging
import os
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

VALID_ENVS = {"dv", "qa", "pd"}
CONFIG_DIR = Path(__file__).resolve().parent.parent.parent / "config"


def _deep_merge(base: dict, override: dict) -> dict:
    """
    Recursively merge override dict into base dict.
    override values take precedence. Lists are replaced, not appended.
    """
    result = copy.deepcopy(base)
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = copy.deepcopy(value)
    return result


def load_config(env: str | None = None, config_dir: str | None = None) -> dict:
    """
    Load merged configuration for the given environment.

    Args:
        env:        Environment name (dv, qa, pd). If None, reads from
                    ENV environment variable, defaults to "dv".
        config_dir: Override config directory path.

    Returns:
        Merged configuration dict (base + environment).
    """
    # Resolve environment
    if env is None:
        env = os.getenv("ENV", "dv").lower()

    if env not in VALID_ENVS:
        raise ValueError(
            f"Invalid environment '{env}'. Must be one of: {VALID_ENVS}"
        )

    # Resolve config directory
    cfg_dir = Path(config_dir) if config_dir else CONFIG_DIR

    # Load base config
    base_path = cfg_dir / "base.yaml"
    if not base_path.exists():
        raise FileNotFoundError(f"Base config not found: {base_path}")

    with open(base_path, "r") as f:
        base_config = yaml.safe_load(f) or {}

    # Load environment config
    env_path = cfg_dir / f"{env}.yaml"
    if not env_path.exists():
        raise FileNotFoundError(f"Environment config not found: {env_path}")

    with open(env_path, "r") as f:
        env_config = yaml.safe_load(f) or {}

    # Merge: env overrides base
    merged = _deep_merge(base_config, env_config)

    logger.info("Loaded config for environment '%s' from %s", env, cfg_dir)
    return merged


def get_env() -> str:
    """Get the current environment name."""
    return os.getenv("ENV", "dv").lower()
