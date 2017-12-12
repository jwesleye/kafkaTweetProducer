
try:
    import argparse, sys
    from tweepy.streaming import StreamListener
    from tweepy import OAuthHandler
    from tweepy import Stream
    from kafka import SimpleProducer, SimpleClient
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
    #client = SimpleClient("127.0.0.1:9092") #for local testing
    client = SimpleClient(args.kafkaBrokerUrl + ":9092")
    producer = SimpleProducer(client)

except Exception as e:
    print("ERROR on kafka client", e)


class StdOutListener(StreamListener):
    def on_data(self, data):
        print(data)
        producer.send_messages("test", data.encode('utf-8'))
        return True

    def on_error(self, status):
        print(status)
        producer.send_messages("errors", status.encode('utf-8'))
        return False


def startStream():
    l = StdOutListener()
    auth = OAuthHandler(args.consumerKey, args.consumerSecret)
    auth.set_access_token(args.accessToken, args.accessTokenSecret)
    stream = Stream(auth, l)
    print("Monitoring for tweets")
    stream.filter(track=["bitcoin"], async=True)


if producer:
    startStream()
