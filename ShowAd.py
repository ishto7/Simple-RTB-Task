from pymongo import MongoClient
from numpy.random import choice
from math import atan
from DataGenerator import adsPro

myClient = MongoClient()
RawData = myClient.AdNetwork.RawData
AdsSummary = myClient.AdNetwork.AdsSummary
UserSummary = myClient.AdNetwork.UserSummary

# A constant to shape the fading curve of expected conversion rate (CR)
# This constant helps to distinguish new ads (in a somehow fuzzy logic)
TRAN_C = 10000

class AdInfo:
    def __init__(self, adId: str, adAction: str, adCost: float):
        self.adId = adId
        self.adAction = adAction
        self.adCost = adCost


def sampleAdInfosGenerator():
    # Using all existing ads as candidates
    AdNetwork = RawData.find().distinct("adId")
    adInfos = []
    for ad in AdNetwork:
        temp = RawData.find_one({"adId": ad})
        adInfos.append(AdInfo(ad, temp["adAction"], temp["advertiserCost"]))

    for ad in adsPro(10, "NewAd-%d"):
        # And also some newcomers
        adInfos.append(AdInfo(ad["id"], ad["action"], ad["cost"]))
    return adInfos


def fadingConstant():
    # Fading Constant is the probability of action on a same ad, for the second time.
    # Should be calculated once in a while [month/year] and stored in cache.
    # It can be calculated from FadingConstant.py
    # (As our data was random and not in a large scale, they may seem meaningless)
    return {
        'install': 0.0,
        'click': 0.36053731463248,
        'completeView': 0.9749502626134012
    }


def overallExpectancy():
    # We want to measure the overall conversion rate of different actions
    # This data can be used for new ads
    # This function can be calculated and stored in cache

    actions = ["install", "click", "completeView"]
    # Categorizing ads
    categorizedAds = [AdsSummary.find({"adAction": action}) for action in actions]
    # Counting impressions
    shown = [sum([ad["shown"] for ad in category]) for category in categorizedAds]
    # Preparing cursor for reuse
    for category in categorizedAds:
        category.rewind()
    # Counting actions
    converted = [sum([ad["converted"] for ad in category]) for category in categorizedAds]
    final = {}
    for action, convert, impression in zip(actions, converted, shown):
        final[action] = convert / impression
    return final


def getWinnerAd(userId: str, adInfos: list, appId: str):
    overallCR = overallExpectancy()  # overallCR is a default expected conversion rate (CR) for every action
    winningDict = {}
    for ad in adInfos:
        # Calculating mean value for every ad with respect to AD HISTORY, (despite of user history)
        winningDict[ad.adId] = expectedValue(ad, overallCR[ad.adAction])

    winningDict = userHistoryEffect(userId, winningDict)  # Considering user action history in mean value
    summation = sum([expected for expected in winningDict.values()])  # Normalizing mean values
    # We use weighted random because we don't want an ad to be shown repeatedly
    return choice([adId for adId in winningDict.keys()], p=[expected / summation for expected in winningDict.values()])


def expectedValue(ad, overallCR):
    adHistory = AdsSummary.find_one({"_id": ad.adId})
    try:
        # If there is a history behind an ad, we shall consider them in our prediction
        adCR = adHistory["converted"] / adHistory["shown"]
        expected = ad.adCost * (
                (adCR) * atan(adHistory["shown"] / TRAN_C)
                + overallCR * atan(TRAN_C / adHistory["shown"])
        )
    except:
        # But if it's a newcomer, just treat it as normal
        expected = ad.adCost * overallCR * 3.1415 / 2
    return expected


def userHistoryEffect(userId, winningDict):
    # Find a list of ads, the user have seen
    actedAds = UserSummary.distinct(
        "acts",
        {"_id": userId}
    )
    for adId in actedAds:
        # For every seen ad:
        theAd = AdsSummary.find_one(
            {"_id": adId},
            projection={"rules": True, "adAction": True}
        )
        try:
            # First we make sure we have considered his/her previous actions
            winningDict[adId] = winningDict[adId] * fadingConstant()[theAd["adAction"]]
        except:
            pass
        try:
            # Some ads have mutual theme, so the same users may like[/dislike] them.
            for rule in theAd["rules"].keys():
                try:
                    winningDict[rule] = winningDict[rule] * theAd["rules"][rule]
                except:
                    pass
        except:
            pass
    return winningDict


def main():
    adInfos = sampleAdInfosGenerator()
    users = ["user-%d" % i for i in range(1000)]
    for user in users:
        winner = getWinnerAd(user, adInfos, "")
    print("Winner is:", winner)


if __name__ == "__main__":
    main()
