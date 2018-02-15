# Python standard libraries
import os, sys, re, csv, string, codecs, operator

# Parse HTML charachters, and convert them to unicode
from html import unescape

# Random number generation, and list shuffling
from random import shuffle

# For linear algebra
import numpy as np

# For graph plotting
import matplotlib.pyplot as plt

# Import NLTK Helpers
import nltk
from nltk import SnowballStemmer
from nltk.corpus import stopwords
from nltk.tokenize import TweetTokenizer
from nltk.stem.wordnet import WordNetLemmatizer

# Sklearn for machine learning modules
from sklearn import metrics
from sklearn.naive_bayes import MultinomialNB
from sklearn.model_selection import ShuffleSplit
from sklearn.model_selection import learning_curve
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfTransformer

# Initialize parsers/tokenizer/lemmatizer/stemmer
lemmatizer = WordNetLemmatizer()
stemmer = SnowballStemmer("english")
tokenizer = TweetTokenizer(strip_handles=True, reduce_len=True)

# Configurations
training_data_set_size = "90K"

# Input data files
training_data_file_name = "data/input/training_data_" + training_data_set_size + ".txt"
testing_data_file_name = "data/input/testing_data.txt"
emoticons_sentiment_file_name = "data/input/emoticon_sentiment_lexicon.txt"

# Output data files
processed_training_data_file_name = "data/output/processed_training_data_" + training_data_set_size + ".txt"
freq_unigrams_training_data_file_name = "data/output/freq_unigrams_training_data_" + training_data_set_size + ".txt"
freq_bigrams_training_data_file_name = "data/output/freq_bigrams_training_data_" + training_data_set_size + ".txt"
freq_trigrams_training_data_file_name = "data/output/freq_trigrams_training_data_" + training_data_set_size + ".txt"

# Emoticon sentiment, key: emoticon, value: sentiment tag (/)
emoticons_sentiments = dict()

# Special +/-/% charachters preceding and subsequent to numbers
special_chars = {'+': 'plustag', '-': 'minustag', '%': 'percenttag'}


# Load emoticons sentiment lexicon (Emoticon TAB Sentiment Tag)
def load_emoticons_lexicon():
    with codecs.open(emoticons_sentiment_file_name, "r", encoding='utf-8',
                     errors='ignore') as emoticon_sentiment_lexicon:
        emoticons_sentiments_lst = [tuple(emoticon_sentiment.rstrip().split("\t")) for emoticon_sentiment in
                                    emoticon_sentiment_lexicon]
        for (emoticon, sentiment) in emoticons_sentiments_lst: emoticons_sentiments[emoticon] = sentiment


load_emoticons_lexicon()


# Preprocess original text of stocktwit/tweet
def preprocess_text(original_string):
    # Decode HTML charachters
    processed_string = unescape(original_string)

    # Remove URLs
    processed_string = re.sub(r"http\S+", "", processed_string)

    # Remove cash tags (e.g. $AAPL) to minimize noise and reduce vocabulary size
    processed_string = re.sub(r"\$\S+", "", processed_string)

    # Add spaces before and after numbers to distinguish them from preceding and subsequent +/-/% charachters
    processed_string = " ".join(re.split("(\d+\.\d+|\d+)", processed_string))

    # Remove numbers to minimize noise and reduce vocabulary size
    processed_string = re.sub("\d+\.\d+|\d+", " ", processed_string)

    # Tokenize text using NLTK tweets tokenizer
    processed_string = tokenizer.tokenize(processed_string)

    # Replace emoticons with special tags reflecting their sentiment
    processed_string = [emoticons_sentiments[token] if token in emoticons_sentiments \
                            else token for token in processed_string]

    # Lowering case, and replace +/-/% charachters preceding and subsequent to numbers to give stronger signal to their implication and prevent them from being deleted in cleaning
    processed_string = [special_chars[token] if token in special_chars else token.lower() for token in processed_string]

    # Remove stopwords and punctuation
    processed_string = " ".join([token for token in processed_string \
                                 if len(token) > 1 and token not in string.punctuation \
                                 and token not in stopwords.words('english')])

    # Remove extra continous periods
    processed_string = re.sub(r"\.+", "", processed_string)

    return processed_string


# Preprocess training data file, input format Stocktwit/tweet TAB Label
# Return a list of training data instances, each entry [original_string, processed_string, label]
def preprocess_training_data():
    training_data = []
    # Open training data file as UTF-8 and cleanup and faulty charachters
    with codecs.open(training_data_file_name, "r", encoding='utf-8', errors='ignore') as training_data_file:
        for training_data_line in training_data_file:
            # Read stocktwits entry/tweet and label(Bullish/Bearish)
            [original_string, label] = training_data_line.rstrip().split("\t")
            processed_string = preprocess_text(original_string)
            if len(processed_string) >= 1:
                training_data.append([original_string, processed_string, label])

    return training_data


training_data = preprocess_training_data()

# Create a copy from the training data and shuffle it
training_data_shuffled = training_data.copy()
shuffle(training_data_shuffled)

# Print a sample of 5 training examples (different output each time for better demonstration)
for i in range(0, 5): print(
    str(i + 1) + ". Before:" + training_data_shuffled[i][0] + "\n   After: " + training_data_shuffled[i][
        1] + "\n   Label: " + training_data_shuffled[i][2])


# Save processed training data on disk (processed_training_data_file_name)
# Output file format (original string TAB processed string TAB label)
def save_processed_training_data(training_data):
    with codecs.open(processed_training_data_file_name, "w", encoding='utf-8',
                     errors='ignore') as processed_training_data_file:
        for training_data_entry in training_data:
            processed_training_data_file.write(
                training_data_entry[0] + "\t" + training_data_entry[1] + "\t" + training_data_entry[2] + "\n")


save_processed_training_data(training_data)
