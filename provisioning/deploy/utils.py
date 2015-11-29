import os


def get_env_value(key, default_value=None):
    """
    Return the value of environment variable KEY if it has been set,
    otherwise, return DEFAULT_VALUE
    """
    value = os.getenv(key)
    if value:
        return value
    else:
        print("Env '%s' is not set, use the default value '%s'" %
              (key, default_value))
        return default_value
