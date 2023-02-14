import os


def application(environ, start_response):
    # Set environmental variables
    # mod_wsgi passes "environmental" variables from SetEnv using `environ` variable
    for key in ["DP3_APP_NAME", "DP3_CONFIG_DIR", "DP3_DP_LOG_FILE"]:
        os.environ[key] = environ.get(key, "")

    from receiver import app as _application

    _application.debug = True

    return _application(environ, start_response)
