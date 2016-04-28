"""Error reporting

Sends messages to PagerDuty.
"""
from cax import config
from pagerduty_api import Alert

ALERT = Alert(service_key=config.pagerduty_api_key())

def alarm(description, other_data=None):
    if other_data is None:
        other_data = {}

    ALERT.trigger(description,
                  client='cax',
                  client_url=config.get_hostname(),
                  details=other_data)


