# Simple-RTB-Task
A simple project, trying to simulate a Real Time Bidding system.

# How to start
First of all, run the ```DataGenerator.py``` file to generate a random initial dataset. Keep in mind that you have to have a mongod service running on your device. Add authentication credentials if needed.

Then run the ```FadingConstant.py``` and ```RuleMining.py``` to perform some analysis on the initial database. Although they take heavy computation power to calculate, but they don't change dramatically as the time goes on and can be done in the background once a time.

Finally run ```ShowAd.py``` and let the system decide which advertisement should be shown to a new query.
