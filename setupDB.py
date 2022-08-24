import os
import sqlite3
import spacy
sp = spacy.load('en_core_web_sm')

# Delete DB
file_path = "translation.db"
if os.path.isfile(file_path):
    os.remove(file_path)

# Load all sentences
with open('resources/SentencePairsEN_DE_all.txt') as f:
    sentences = f.read().splitlines()

# Load all lemmas
with open('resources/freqList.txt') as f:
    lemmas = f.read().splitlines()

# Init DB
con = sqlite3.connect('translation.db')
c = con.cursor()

# Create tables
c.execute('''CREATE TABLE de_sentences(id int, sentence text)''')
c.execute('''CREATE TABLE en_sentences(id int, sentence text)''')
c.execute('''CREATE TABLE lemmas (id int, lemma text)''')
c.execute('''CREATE TABLE en_sentences_lemmas(en_sentences_id int, lemmas_id int)''')
c.execute('''CREATE TABLE translation (de_id int, en_id int)''')

# Insert de_sentences
for sentence in sentences:
    c.execute("SELECT count() FROM de_sentences where id=" + sentence.split('\t')[0])
    if c.fetchone()[0] == 0:
        c.execute("insert into de_sentences (id, sentence) values (?, ?)",
                  (sentence.split('\t')[0], sentence.split('\t')[1]))

# Insert en_sentences
for sentence in sentences:
    c.execute("SELECT count() FROM en_sentences where id=" + sentence.split('\t')[2])
    if c.fetchone()[0] == 0:
        c.execute("insert into en_sentences (id, sentence) values (?, ?)",
                  (sentence.split('\t')[2], sentence.split('\t')[3]))

# Insert lemmas
i = 0
while i < len(lemmas):
    c.execute("insert into lemmas (id, lemma) values (?, ?)", (i, lemmas[i].split(';')[0]))
    i = i + 1

# Insert en_sentences lemmas
for sentence in sentences:
    for word in sp(sentence.split('\t')[3]):
        c.execute("SELECT id FROM lemmas where lemma='" + word.lemma_.replace("'", "") + "'")
        row = c.fetchone()
        if row is not None:
            c.execute("insert into en_sentences_lemmas (en_sentences_id, lemmas_id) values (?, ?)",
                      (sentence.split('\t')[2], row[0]))


# Insert translation
for sentence in sentences:
    c.execute("insert into translation (de_id, en_id) values (?, ?)",
              (sentence.split('\t')[0], sentence.split('\t')[2]))

# Save (commit) the changes
con.commit()

# We can also close the connection if we are done with it.
# Just be sure any changes have been committed or they will be lost.
con.close()






