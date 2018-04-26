from pymongo import MongoClient
from ShowAd import overallExpectancy


myClient = MongoClient()
RawData = myClient.AdNetwork.RawData
AdsSummary = myClient.AdNetwork.AdsSummary
UserSummary = myClient.AdNetwork.UserSummary


def fadingCCalculator():
    fadingConstant = {}
    overall = overallExpectancy()
    users = UserSummary.distinct("_id", {"acts": {"$exists": True}})
    # Find users with at least one action

    for action in overall.keys():
        shownAfter = 0
        convertedAfter = 0
        for user in users:
            # Find all the ads, this user has acted on
            actionedAds = RawData.distinct("adId",
                                           {
                                               "userId": user,
                                               "converted": True,
                                               "adAction": action
                                           })
            for ad in actionedAds:
                # Find the time of his/her first action on the ad
                firstAction = RawData.find_one({"userId": user, "adId": ad, "converted": True}, sort=[("timestamp", 1)])
                # List all the events after that action (for the same user and ad)
                seriesAfter = RawData.find({"userId": user, "adId": ad, "timestamp": {"$gt": firstAction["timestamp"]}})
                for event in seriesAfter:
                    # Count actions and impressions for the same ad and the same user, after his/her first action on that ad
                    shownAfter += 1
                    convertedAfter += 1 if event["converted"] else 0

        # Calculate CR
        print(convertedAfter, shownAfter, overall[action], action)
        fadingConstant[action] = convertedAfter / shownAfter / overall[action]

    print(fadingConstant)
    # It gave me:
    # {'install': 0.0, 'click': 0.36053731463248, 'completeView': 0.9749502626134012}

    return fadingConstant


if __name__ == "__main__":
    fadingCCalculator()
