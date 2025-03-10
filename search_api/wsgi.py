"""
WSGI config for searchApi project.

It exposes the WSGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/1.8/howto/deployment/wsgi/
"""

import os

from django.core.wsgi import get_wsgi_application
import newrelic.agent

newrelic.agent.initialize('./newRelic.ini')
newrelic.agent.register_application()

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "search_api.settings")
application = get_wsgi_application()
