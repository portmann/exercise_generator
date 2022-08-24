import sqlite3

import numpy as np
import pandas as pd
import re

from multiprocessing import Pool, cpu_count


try:
    # connecting to the database
    connection = sqlite3.connect("translation.db")

    def regexp(expr, item):
        reg = re.compile(expr)
        return reg.search(item) is not None

    connection.create_function("REGEXP", 2, regexp)

    # cursor
    crsr = connection.cursor()

    # print statement will execute if there
    # are no errors
    print("Connected to the database")
except:
    print('Error while connecting database')

# taking input
lemma_word = input("Enter Word: ").lower()
min_lemmas = int(input("Enter minimum limit: "))
max_lemmas = int(input("Enter maximum limit: "))



def get_results(query):
    crsr.execute(query)
    return np.array(crsr.fetchall())

print('-----------Matching Word in all Sentences-----------')
lemma_matching_query = f'SELECT * FROM en_sentences WHERE sentence REGEXP "[^a-bA-b0-9]{lemma_word}[^a-bA-z0-9]";'
lemma_matched_sentences = get_results(lemma_matching_query)


def make_query(lemma_ids):
    query = 'SELECT * FROM lemmas WHERE id in ('
    for _, _id in lemma_ids[:-1]:
        query = query + str(_id) +','
    query = query + str(lemma_ids[-1][1])+') ORDER BY id;'
    return query


def get_sentences(idx):
    eid, sentence = lemma_matched_sentences[idx]
    lemma_ids_query = f'SELECT DISTINCT en_sentences_id, lemmas_id FROM en_sentences_lemmas WHERE en_sentences_id = {eid};'
    lemma_ids = get_results(lemma_ids_query)
    if len(lemma_ids) >= min_lemmas and len(lemma_ids) <= max_lemmas:
        highest_frequency_query = make_query(lemma_ids)
        lemmas = get_results(highest_frequency_query)
        if lemmas[0][1] == lemma_word:
            # in_range_sentences.append((eid, sentence))
            # print(idx)
            return (eid, sentence)


print('-----------Matching High Frequency and Range of lemmas-----------')
size = len(lemma_matched_sentences)
cpus = cpu_count()
print(cpus, size)
with Pool(cpus) as pool:
    result = pool.map(get_sentences, range(0,size))

print('-----------Filtering None-----------')
result = list(filter(lambda item: item is not None, result))


def get_german_sentences(idx):
    eid, en_sentence = result[idx]
    german_id_query = f'SELECT * FROM translation WHERE en_id = {eid};'
    german_ids = get_results(german_id_query)
    german_id = german_ids[0][0]

    german_sentence_query = f'SELECT * FROM de_sentences WHERE id = {german_id};'
    german_sentences = get_results(german_sentence_query)
    de_sentence = german_sentences[0][1]

    return en_sentence, de_sentence

print('-----------Extracting German Sentences-----------')
size = len(result)
cpus = size if size<cpus else cpus
with Pool(cpus) as pool:
    result = pool.map(get_german_sentences, range(0,size))

result = np.array(result)

print('-----------Saving File-----------')
df = pd.DataFrame({'english_sentences':result[:,0], 'de_sentences':result[:,1]})
df.to_csv(lemma_word+'.csv', index=False)
print(f'File is saved as {lemma_word}.csv')

# close the connection
connection.close()