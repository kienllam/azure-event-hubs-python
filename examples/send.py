
#!/usr/bin/env python

from __future__ import print_function

import sys
import logging
from eventhubs import EventHubClient, Sender, EventData

import examples
from satori.rtm.client import make_client, SubscriptionMode

endpoint = "wss://open-data.api.satori.com"
appkey = "f20D70e80c62705318Bc3A2D16af7aaE"
channel = "Twitter-statuses-sample"

def main():

    with make_client(endpoint=endpoint, appkey=appkey) as client:
        print('Connected to Satori RTM!')

        class SubscriptionObserver(object):
            def on_subscription_data(self, data):
                ADDRESS = ("amqps://"
                       "send"
                       ":"
                       "RJQLhw9Uu8qHL0KZQkb0dd+4didTvGqIzJZljtP5m/s="
                       "@"
                       "structstreaming.servicebus.windows.net"
                       "/"
                       "twitter")
                sender = Sender()
                azClient = EventHubClient(ADDRESS if len(sys.argv) == 1 else sys.argv[1]) \
                             .publish(sender) \
                             .run_daemon()

                for message in data['messages']:
                    sender.send(EventData(str(message)))
                    print("Got message:", message)

        subscription_observer = SubscriptionObserver()
        client.subscribe(
            channel,
            SubscriptionMode.SIMPLE,
            subscription_observer)

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            pass


if __name__ == '__main__':
    main()
