import pandas as pd
import re
import pymorphy2
import math
from wiki_ru_wordnet import WikiWordnet
wikiwordnet = WikiWordnet()
morph = pymorphy2.MorphAnalyzer()


class CategoryPredictor:
    def __init__(self, label='Название', category_columns=['Номинация / Задача', 'категория', 'Иновация'], df=None):
        self.category_columns=category_columns
        self.curent_category = category_columns[0]
        self.label = label
        self.word_clouds = {}
        if type(df) == type(pd.DataFrame()):
            self.df = df

    def read_df(self):
        self.df = pd.read_excel('Датасет. Задача 1.xlsx')

    def calculate_distanses(self, word, cloud):
        scores = 0
        synset1 = wikiwordnet.get_synsets(word)
        if len(synset1) > 0:
            for s1 in synset1:
                for w in cloud:
                        synset2 = wikiwordnet.get_synsets(w)
                        if len(synset2) > 0:
                            for s2 in synset2:
                                common_hypernyms = wikiwordnet.get_common_hypernyms(s1, s2)
                                top_hip = sorted(common_hypernyms, key=lambda x: x[1] + x[2])
                                if len(top_hip) > 0:
                                    top_hip = top_hip[0]
                                    score = top_hip[1] + top_hip[2]
                                    if score != 0:
                                        scores += 1/score
        return scores

    def get_hypernyms(self, words):
        new_cloud = []
        for word in words: 
            lemmas = []
            synsets = wikiwordnet.get_synsets(word)
            for synset in synsets:
                for w in synset.get_words():
                    lemmas.append(w.lemma())
                    for hypernym in wikiwordnet.get_hypernyms(synset):
                        for h in hypernym.get_words():
                            lemmas.append(h.lemma())
                    for hyponym in wikiwordnet.get_hyponyms(synset):
                        for h in hyponym.get_words():
                            lemmas.append(h.lemma())
            lemmas = list(set(lemmas))
            new_cloud.extend(lemmas)
        return new_cloud

    def text_to_words(self, text):
        text = re.sub("[,|-|(|)|.|;|:|1|2|3|4|5|6|7|8|9|0|\n]","", text)
        text = text.replace('\\', ' ')
        text = text.replace('-', ' ')
        text = text.lower()
        words = text.split(' ')
        words = map(morph.normal_forms, words)
        words = list(map(lambda x: x[0], words))
        return words
    
    # create clouds for each category
    def create_clouds(self, category):
        df_c = self.df[self.df[self.curent_category] == category]
        claud = []
        df_c['words'].apply(lambda x: claud.extend(x))
        return claud

    def clear_clouds(self, row):
        category, cloud = row['category'], row['clouds']
        new_cloud = cloud.copy()
        for w in cloud:
            other_clouds = list(self.woc[self.woc['category'] != category]['clouds'])
            for orher_cloud in other_clouds:
                if w in orher_cloud:
                    new_cloud = list(filter(lambda x: x != w, new_cloud))
                    break
        return new_cloud


    def find_categoris(self,
                        text,
                        category=None,
                        K_cloud=0,
                        K_clear_cloud=3,
                        K_big_cloud=1,
                        K_dist=1,
                        K_top=3,
                        ):
        if category != None:
            self.woc = self.word_clouds[category]
        scores = {} # catedory: score
        words = self.text_to_words(text)
        categoris = list(pd.DataFrame(self.woc['category'].value_counts()).index)
        for c in categoris:
            score = 0
            cloud = list(self.woc[self.woc['category'] == c]['clouds'])[0]
            clear_cloud = list(self.woc[self.woc['category'] == c]['clear_clouds'])[0]
            big_cloud = list(self.woc[self.woc['category'] == c]['big_clouds'])[0]
            for w in words:
                if w in cloud:
                    num = cloud.count(w)
                    score += K_cloud * num / math.log(len(cloud))
                
                if w in big_cloud:
                    num = big_cloud.count(w)
                    score += K_clear_cloud * num / math.log(len(clear_cloud))
                
                if w in clear_cloud:
                    num = clear_cloud.count(w)
                    score += K_big_cloud * num / math.log(len(big_cloud))
                if self.Fast == False:
                    try:
                        score += self.calculate_distanses(word=w, cloud=big_cloud) * K_dist / math.log(len(big_cloud))
                    except:
                        print('score 3 error')

            scores[c] = score
        #if '-' in scores.keys():   #   !!!!
        #    scores['-'] *= 10
        sorted_scores = dict(sorted(scores.items(),key=lambda item:item[1])[::-1][0:K_top])
        return sorted_scores
    
    def fit(self, category_column):
        self.curent_category = category_column
        self.df['words'] = self.df[self.label].apply(self.text_to_words)
        woc = pd.DataFrame({'category': pd.DataFrame(self.df[category_column].value_counts()).index})
        self.woc = woc
        woc['clouds'] = woc['category'].apply(self.create_clouds)
        woc['clear_clouds'] = woc.apply(self.clear_clouds, axis=1)
        woc['big_clouds'] = woc['clear_clouds'].apply(self.get_hypernyms)
        self.word_clouds[category_column] = woc

    def predict(self, labels, category, Fast=False): # labels is dataframe
        self.Fast = Fast
        self.woc = self.word_clouds[category]
        labels['predictions'] = labels[self.label].apply(self.find_categoris)
        return labels

    def fit_all(self):
        for category_column in self.category_columns:
            self.fit(category_column)

