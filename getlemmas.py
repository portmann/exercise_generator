import spacy
sp = spacy.load('en_core_web_sm')

# Load all sentences
with open('resources/test_sentences.txt') as f:
    sentences = f.read().splitlines()

# Insert en_sentences lemmas
i = 0
while i < len(sentences):
    for sentence in sentences[i]:
        for word in sp(sentence.split('\t')[3]):
            print(word.lemma_)
    i = i + 1
