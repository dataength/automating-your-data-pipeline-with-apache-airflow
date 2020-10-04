from sklearn.feature_extraction.text import CountVectorizer

vectorizer = CountVectorizer(stop_words='english')

X_train = [
    'Hey Cici sweetheart! Just wanted to let u know I luv u! OH! and will the mixtape drop soon? FANTASY RIDE MAY 5TH!!!!',
    'I heard about that contest! Congrats girl!!',
    'UNC!!! NCAA Champs!! Franklin St.: I WAS THERE!! WILD AND CRAZY!!!!!! Nothing like it...EVER http://tinyurl.com/49955t3',
    'Do you Share More #jokes #quotes #music #photos or #news #articles on #Facebook or #Twitter?',
    'Good night #Twitter and #TheLegionoftheFallen. 5:45am cimes awfully early!',
    'I just finished a 2.66 mi run with a pace of 11\'14"/mi with Nike+ GPS. #nikeplus #makeitcount',
    'Disappointing day. Attended a car boot sale to raise some funds for the sanctuary, made a total of 88p after the entry fee - sigh',
    'no more taking Irish car bombs with strange Australian women who can drink like rockstars...my head hurts.',
    'Just had some bloodwork done. My arm hurts'
]

y_train = [
    'positive',
    'positive',
    'positive',
    'neutral',
    'neutral',
    'neutral',
    'negative',
    'negative',
    'negative'
]

X_train = vectorizer.fit_transform(X_train)

X_test = ['Congrats @ravikiranj, i heard you wrote a new tech post on sentiment analysis']
X_test = vectorizer.transform(X_test)

# import
from sklearn import naive_bayes

# instantiate
clf = naive_bayes.MultinomialNB()

# fit
clf.fit(X_train, y_train)

# predict
result = clf.predict(X_test)
print(result)


X_test = ['I am very bored today.']
X_test = vectorizer.transform(X_test)
result = clf.predict(X_test)
print(result)

X_test = ['Good morning!']
X_test = vectorizer.transform(X_test)
result = clf.predict(X_test)
print(result)

X_test = ['I luv you']
X_test = vectorizer.transform(X_test)
result = clf.predict(X_test)
print(result)
