import json
import tweepy
# Authentication details.
consumer_key = 'Hvq33CfuafOFrbDJOrSAKNkLg'
consumer_secret = '3u37qtXeNQF60C0npQeG4rMeZOCFPlmRwHn97akWFVdk6sabBJ'
access_token = '33020840-hDoKHyIfU4PobFpZifignmJHOXZoY22FxBOG95swf'
access_token_secret = 'smjekhIny8IePpvC1ZQoDp9Bh20lOgeMie255BHy1kxI5'

GEOBOX_WORLD = [-180,-90,180,90]
GEOBOX_USA = [-165.6,12.6,-52.7,66.4]


# This is the listener, resposible for receiving data
class StdOutListener(tweepy.StreamListener):

    def on_data(self, data):
        decoded = json.loads(data.strip())
        text_data = decoded.get('text')
        try:
            with open('output.txt', 'a') as f:
                if text_data is not None:
                    f.write(text_data.encode('ascii','ignore')+'\n')
                    print text_data.encode('ascii','ignore')+'\n'
                #print("%s",decoded.get('text'))
                return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
            time.sleep(5)
        return True

    def on_error(self, status):
        print status

if __name__ == '__main__':
    l = StdOutListener()
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    print "Showing all new tweets"
    stream = tweepy.Stream(auth, l)
    #stream.userstream("with=following")
    stream.filter(locations=GEOBOX_USA, languages=['en'])
    #stream.filter(track=keyword_list, languages=['en'])
