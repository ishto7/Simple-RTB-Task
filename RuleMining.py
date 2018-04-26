from mlxtend.frequent_patterns import association_rules, apriori
from pymongo import MongoClient
import pandas as pd

myClient = MongoClient()
RawData = myClient.AdNetwork.RawData
UserSummary = myClient.AdNetwork.UserSummary
AdsSummary = myClient.AdNetwork.AdsSummary


def mineRules():
    adsDict = {}
    allActedUsers = UserSummary.find({"acts": {"$exists": True}})  # All users with at least one action
    for ad in AdsSummary.find().sort("_id", 1):
        # One-Hot-Encoding user actions.
        adsDict[ad["_id"]] = [1 if ad["_id"] in user["acts"] else 0 for user in allActedUsers]
        allActedUsers.rewind()  # This is needed for reusing Mongo cursor
    # Now we get ready for the magic
    df = pd.DataFrame(data=adsDict)
    # Abracadabra
    frequents = apriori(df, min_support=0.05, use_colnames=True)
    # We have limited minimum support to 5%. It helps to mine more frequent rules
    rules = association_rules(frequents, metric="lift", min_threshold=1)
    # lift equal to one, means shit. But more or less than that is something meaningful
    # I was thinking we may also use rules with lift < 1. It may help
    print(rules)
    return rules


def useRules(rules):
    # Well, an unused weapon is a useless weapon.
    for i in range(len(rules)):
        antecedant, = rules["antecedants"][i]
        consequent, = rules["consequents"][i]
        AdsSummary.update_one(
            {"_id": antecedant},
            {
                "$set":
                    {
                        "rules.%s" % consequent: rules["lift"][i]
                    }
            }
        )


if __name__ == "__main__":
    rules = mineRules()
    useRules(rules)
