import pandas as pd
import re
import pymorphy2
import math
from wiki_ru_wordnet import WikiWordnet
import psycopg2
import time
import os

DB_HOST=os.environ.get('DB_HOST')
DB_NAME=os.environ.get('DB_NAME')
DB_USER=os.environ.get('DB_USER')
DB_PASSWORD=os.environ.get('DB_PASSWORD')

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
                        K_cloud=0.1,
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
                    score += K_cloud * num / (math.log(len(cloud)) + 1)
                
                if w in big_cloud:
                    num = big_cloud.count(w)
                    score += K_clear_cloud * num / (math.log(len(big_cloud)) + 1)
                
                if w in clear_cloud:
                    num = clear_cloud.count(w)
                    score += K_big_cloud * num / (math.log(len(clear_cloud)) + 1)

                if self.Fast == False:
                    try:
                        score += self.calculate_distanses(word=w, cloud=big_cloud) * K_dist / math.log(len(big_cloud))
                    except:
                        print('score 3 error')

            scores[c] = score
        if '-' in scores.keys():
            scores['-'] *= 10
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



class PredictController:
    def connect_db(self):
        self.conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, 
                        password=DB_PASSWORD, host=DB_HOST)
        self.cursor = self.conn.cursor()

    def get_data_db(self):
        q = f'''SELECT {self.categorys}.{self.category_col} AS subsidy, projects.title, {self.categorys}.id,
                    {self.join_table}.accepted FROM {self.join_table}
                    LEFT JOIN {self.categorys} ON ({self.join_table}.{self.category_id} = {self.categorys}.id)
                    LEFT JOIN projects ON ({self.join_table}.project_id = projects.id);'''
        if self.categorys == 'tag':
            print(q)
        df = pd.read_sql(q, self.conn)
        #self.len_sub_project = len(df)
        self.len_sub_project = pd.read_sql(f'SELECT max(id) FROM {self.join_table}', self.conn)['max'][0]

        df_train = df[df['accepted'] == True]

        df_test = pd.read_sql('SELECT * FROM projects', self.conn)
        return df_train, df_test
        
    def write_predictions(self, row):
        q = f'''SELECT * FROM {self.categorys}'''
        df_sub = pd.read_sql(q, self.conn)
        project_id = row['id']
        predictions = row['predictions']
        dsubsidy_project = pd.read_sql(f'SELECT * FROM {self.join_table}', self.conn)
        for subsidy in predictions:
            subsidy_id = df_sub[df_sub[self.category_col] == subsidy]['id'].iloc[0]
            prob = predictions[subsidy]
            print(prob, self.len_sub_project)
            if len(dsubsidy_project[(dsubsidy_project['project_id'] == project_id) & (dsubsidy_project[self.category_id] == subsidy_id)]) == 0:
                
                self.values.append((int(self.len_sub_project + 1), False, float(prob), int(project_id), int(subsidy_id)))
                self.len_sub_project += 1
            
            self.conn.commit()

    def fit_predict(self, categorys='subsidies', join_table='subsidy_project', category_col='title', category_id='subsidies_id'):
        self.categorys = categorys
        self.join_table=join_table
        self.category_col=category_col
        self.category_id=category_id
        df_train, df_test = self.get_data_db()
        Predictor = CategoryPredictor(label='title',    
                              category_columns=['subsidy'], df=df_train)
        Predictor.fit_all()
        df_predicted = Predictor.predict(labels=df_test, category='subsidy')
        self.values = []
        df_predicted.apply(self.write_predictions, axis=1)

        if self.values != []:
            q = f'''INSERT INTO {self.join_table} (id, accepted, probability, project_id, {self.category_id}) VALUES ''' + '{}'
            records_list_template = ','.join(['%s'] * len(self.values))
            insert_query = q.format(records_list_template)
            cursor = self.conn.cursor()
            cursor.execute(insert_query, self.values)
            self.conn.commit()
        
        return df_predicted

    def server(self):
        while True:
            self.connect_db()
            try:
                self.fit_predict(categorys='subsidies', join_table='subsidy_project', category_col='title', category_id='subsidies_id')
            except:
                print('err sybsidy')
            time.sleep(1)
            try:
                self.fit_predict(categorys='tag', join_table='tag_project', category_col='tag_name', category_id='tag_id')
            except:
                print('err tags')
            time.sleep(1)
            try:
                self.fit_predict(categorys='innovation', join_table='innovation_project', category_col='is_innovation', category_id='innovation_id')
            except:
                print('err inovation')
            time.sleep(1)
            self.conn.close()
            time.sleep(10)

Controller = PredictController()
df_pred = Controller.server()
