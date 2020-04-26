from receiver import app as application, initialize
application.debug = True

# API needs to be initialized before running the web service
initialize()
