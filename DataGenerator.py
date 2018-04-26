from datetime import datetime
from random import randrange, choice, uniform, random
from pymongo import MongoClient, UpdateOne

myClient = MongoClient()
RawData = myClient.AdNetwork.RawData
AdsSummary = myClient.AdNetwork.AdsSummary
UserSummary = myClient.AdNetwork.UserSummary

# Constants:
N_IMP = 10 ** 5  # Number of impressions
N_ADS = 50  # Number of ads
N_USERS = 1000  # Number of users
N_APPS = 100  # Number of apps
N_GENRES = 6  # Number of genres
N_THRESHOLD = 5000  # Bulk size
adSummary = {}  # They help to write to database in bulk
userSummary = []  # They help to write to database in bulk
rawData = []  # They help to write to database in bulk


def appPro(N_GENRES, N_APPS):
    appProfile = []
    availableGenres = ["Genre-%d" % i for i in range(N_GENRES)]
    for app in range(N_APPS):
        appProfile.append({
            "id": "App-%d" % app,
            "genre": choice(availableGenres)
        })
    return appProfile


def adsPro(N_ADS, format="Ad-%d"):
    adsProfile = []
    for ad in range(N_ADS):
        action = choice(["install", "completeView", "click"])
        if action is "install":
            cost = 3000 * uniform(0.7, 1.3)
            conChance = 0.001 * uniform(0.7, 1.3)
        elif action is "click":
            cost = 300 * uniform(0.7, 1.3)
            conChance = 0.01 * uniform(0.7, 1.3)
        else:
            cost = 30 * uniform(0.7, 1.3)
            conChance = 0.1 * uniform(0.7, 1.3)
        adsProfile.append({
            "id": format % ad,
            "action": action,
            "cost": cost,
            "conChance": conChance
        })
    return adsProfile


def percentRand(chance):
    return (random() < chance)


def showAd(N_IMP, adsProfile, appsProfile, N_USERS):
    for _ in range(N_IMP):
        ad = choice(adsProfile)
        app = choice(appsProfile)
        result = {
            "timestamp": datetime.utcnow(),
            "userId": "user-%d" % randrange(N_USERS),
            "appId": app["id"],
            "appGenre": app["genre"],
            "adId": ad["id"],
            "adAction": ad["action"],
            "advertiserCost": ad["cost"],
            "converted": percentRand(ad["conChance"])
        }
        streamAnalysis(result)


def streamAnalysis(result):
    global rawData, adSummary, userSummary
    rawData.append(result)

    # Aggregating every AD data into a dict in a very fast way
    adAnalysis(result)
    # Aggregating every USER data into a dict (but not too fast)
    userAnalysis(result)

    if len(rawData) >= N_THRESHOLD:
        AdsSummary.bulk_write(list(adSummary.values()), ordered=False)
        adSummary = {}
        UserSummary.bulk_write(userSummary, ordered=False)
        userSummary = []
        RawData.insert_many(rawData, ordered=False)
        rawData = []


def adAnalysis(result):
    global adSummary
    try:
        adSummary[result["adId"]]._doc["$inc"]["shown"] += 1
        adSummary[result["adId"]]._doc["$inc"]["converted"] += (1 if result["converted"] else 0)
    except:
        adSummary[result["adId"]] = UpdateOne(
            {"_id": result["adId"]},
            {
                "$inc":
                    {
                        "shown": 1,
                        "converted": (1 if result["converted"] else 0)
                    },
                "$setOnInsert":
                    {
                        "adAction": result["adAction"],
                        "advertiserCost": result["advertiserCost"],
                    }
            },
            upsert=True
        )
    return


def userAnalysis(result):
    global userSummary
    if result["converted"]:
        userSummary.append(UpdateOne(
            {"_id": result["userId"]},
            {
                "$addToSet":
                    {"acts": result["adId"]}
            },
            upsert=True
        ))


def main():
    appsProfile = appPro(N_GENRES, N_APPS)
    adsProfile = adsPro(N_ADS)
    showAd(N_IMP, adsProfile, appsProfile, N_USERS)


if __name__ == "__main__":
    main()
