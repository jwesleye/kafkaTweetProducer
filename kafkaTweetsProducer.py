
try:
    import json
    import argparse, sys
    from tweepy.streaming import StreamListener
    from tweepy import OAuthHandler
    from tweepy import Stream
    # from kafka import SimpleProducer, SimpleClient
    from time import sleep
    import kafka
    #import daemon
    print("imports are good")
except Exception as e:
    print("IMPORT ERROR", e)


print("launching")

parser=argparse.ArgumentParser()
parser.add_argument('--kafkaBrokerUrl', help='need kafkaBrokerUrl')
parser.add_argument('--accessToken' , help='need accessToken')
parser.add_argument('--accessTokenSecret'   , help='need accessTokenSecret')
parser.add_argument('--consumerKey' , help='need consumerKey')
parser.add_argument('--consumerSecret' , help='need consumerSecret')

#parser.add_argument('--foo', help='Foo the program')
args=parser.parse_args()
print(args)

try:
    producer = kafka.KafkaProducer(bootstrap_servers=[args.kafkaBrokerUrl + ':9092'],api_version=(0,10,1), acks="all")

except Exception as e:
    print("ERROR on kafka setup", e)


class StdOutListener(StreamListener):
    def on_data(self, data):
        producer.send("tweets", json.dumps(data).encode('utf-8'))
        print(data)
        return True

    def on_error(self, status):
        print(status)
        producer.send("errors", status.encode('utf-8'))
        return False


def startStream():
    l = StdOutListener()
    auth = OAuthHandler(args.consumerKey, args.consumerSecret)
    auth.set_access_token(args.accessToken, args.accessTokenSecret)
    stream = Stream(auth, l)
    print("Monitoring for tweets")
    stream.filter(track=["kafka", "scala", "confluent"], async=True)


if producer:
    startStream()
