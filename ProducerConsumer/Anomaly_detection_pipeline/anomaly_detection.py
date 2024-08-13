import pandas as pd
from tensorflow.keras.layers import Layer, LSTM, Dense, Input, Flatten, Attention,Reshape # type: ignore
from tensorflow.keras.models import Model # type: ignore
import tensorflow as tf # type: ignore
import keras
from keras.saving import register_keras_serializable  # type: ignore
import tensorflow.keras.backend as K # type: ignore
from sklearn.feature_extraction.text import TfidfVectorizer,CountVectorizer # type: ignore
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.metrics import pairwise_distances_argmin_min
from sklearn.cluster import KMeans
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from numpy import nan
from sklearn import tree
from sklearn.metrics import accuracy_score
import numpy as np
from sklearn.model_selection import GridSearchCV
from sklearn.neighbors import KNeighborsClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import VotingClassifier
import os.path
import warnings
warnings.filterwarnings('ignore')
import re
import joblib
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import  LabelEncoder
from sklearn.svm import  OneClassSVM

from sklearn.linear_model import LogisticRegression
from sklearn.metrics import precision_recall_fscore_support # type: ignore
import logging 
import nltk
import re
import string
from pprint import pprint
from nltk.sentiment import SentimentIntensityAnalyzer
# Setup logging
logging.basicConfig(level=logging.INFO)


@register_keras_serializable()

class attention(tf.keras.layers.Layer):
    def __init__(self ,selected_features,**kwargs):
        super(Attention, self).__init__(**kwargs)
        self.selected_features=selected_features
    def build(self, input_shape):
        feature_dim=input_shape[1][-1]
        self.attention_weights = self.add_weight(
            shape=(feature_dim,),
            initializer='uniform',
            trainable=True,
            name='attention_weights'
        ) 
        super(attention,self).build(input_shape)
    def call(self, inputs):
        if self.selected_features:
            attention_scores = tf.reduce_sum(
                inputs[:, self.selected_features] * self.attention_weights[self.selected_features], axis=-1
            )
        else:
            attention_scores = tf.reduce_sum(inputs * self.attention_weights, axis=-1)
        
        attention_scores = tf.nn.softmax(attention_scores, axis=1)
        weighted_inputs = inputs * tf.expand_dims(attention_scores, -1)
        return tf.reduce_sum(weighted_inputs, axis=1)
    def compute_output_shape(self, input_shape):
        return (input_shape[0], input_shape[1][0])
    @classmethod
    def get_config(self):
        config = super(Attention, self).get_config()
        return config

@register_keras_serializable()
class MyLSTMLayer(tf.keras.layers.Layer):
    def __init__(self, units, **kwargs):
        super(MyLSTMLayer, self).__init__(**kwargs)
        self.units = units
        self.lstm = tf.keras.layers.LSTM(units, activation='relu', return_sequences=True)

    def call(self, inputs):
        return self.lstm(tf.expand_dims(inputs, axis=1))
    
    
    def get_config(self):
        config = super(MyLSTMLayer, self).get_config()
        config.update({"units": self.units})
        return config
    @classmethod
    def from_config(cls,config):
        return cls(**config)
class AnomalyDetectionPipeline :
    def __init__(self,columns=None,path=None) :
        self.columns  = columns
        self.tf_idf = None
        self.tf_idf_feature_names =[]
        self.label_encoders = {}
        self.isolation_forest = None
        self.scaler=StandardScaler()
        self.isolation_forest = None
        self.svm = None
        self.path = path
        self.autoencoder=None
        self.meta_model=None
        self.final_data=None
        self.knn=None
        self.rf=None
        self.ensemble=None
        self.all_models=self.load()
        self.decision_tree_classifier=None
        self.count_vect = None
        self.vocab = []
        self.sia=SentimentIntensityAnalyzer()
        self.misclassified_data = pd.DataFrame()  # Initialize if not already done
    def  analyze_sentiment(self,text):
            res=self.sia.polarity_scores(text)
            return res
    def createfeatures(self, x:pd.DataFrame) -> pd.DataFrame :
        
        def is_positive(tweet: str) -> bool:
                """True if tweet has positive compound sentiment, False otherwise."""
                return self.sia.polarity_scores(tweet)["compound"] > 0
        try:
            if "datetimeutc" in x.columns :
                x['avg_io_wait_time'] = x.groupby('datetimeutc')['io_wait'].transform(lambda x: (x == 'Y').sum())  # Count of wait times
                x['datetimeutc'] = pd.to_datetime(x['datetimeutc'])
                x['year'] = x['datetimeutc'].dt.year
                x['month'] = x['datetimeutc'].dt.month
                x['day'] = x['datetimeutc'].dt.day
                x['hour'] = x['datetimeutc'].dt.hour
                x['minute'] = x['datetimeutc'].dt.minute
                x['second'] = x['datetimeutc'].dt.second
                x['day_of_week'] = x['datetimeutc'].dt.dayofweek
                x['is_weekend'] = x['datetimeutc'].dt.dayofweek >= 5
                x['is_weekend']=x['is_weekend'].astype(int)

                x.drop('datetimeutc', axis=1, inplace=True)
                x.drop('user', axis=1, inplace=True)
                # Create Performance Anomalies Features
            if "query" in x.columns:
                x['query'] = x['query'].astype(str)
                x['compound'] = x['query'].apply(lambda x: self.analyze_sentiment(x)['compound'])
                x['neg'] = x['query'].apply(lambda x: self.analyze_sentiment(x)['neg'])
                x['neu'] = x['query'].apply(lambda x: self.analyze_sentiment(x)['neu'])
                x['pos'] = x['query'].apply(lambda x: self.analyze_sentiment(x)['pos'])
                x['is_positive'] = x['query'].apply(is_positive).astype(int)
            current_cpu=x['cpu']
            current_memory=x['memory']
            ################################
            x['avg_query_duration'] = x.groupby('query')['duration'].transform('mean')
            x['max_query_duration'] = x.groupby('query')['duration'].transform('max')
            
            #  System Metric
            x['peak_io_wait_time'] = x['io_wait'].apply(lambda y: 1 if y == 'Y' else 0)  # Example peak
            x['historical_cpu_utilization'] = 1
            x['historical_memory_utilization'] = 1
           
            x['cpu_memory_ratio'] = current_cpu/ (current_memory + 1e-6)
            x['read_write_ratio'] = x['read'] / (x['write'] + 1e-6)
            x['cpu_duration_ratio'] = x['cpu'] / (x['duration'] + 1e-6)
            x['memory_duration_ratio'] = x['memory'] / (x['duration'] + 1e-6)
            x['is_long_query'] = x['duration'] > x['duration'].quantile(0.95)
            x['is_high_cpu'] = x['cpu'] > x['cpu'].quantile(0.95)
            x['is_high_memory'] = x['memory'] > x['memory'].quantile(0.95)
            x['is_high_read'] = x['read'] > x['read'].quantile(0.95)
            x['is_high_write'] = x['write'] > x['write'].quantile(0.95)
            x['is_high_cpu_memory_ratio'] = x['cpu_memory_ratio'] > x['cpu_memory_ratio'].quantile(0.95)
            x['is_high_read_write_ratio'] = x['read_write_ratio'] > x['read_write_ratio'].quantile(0.95)
            x['is_high_cpu_duration_ratio'] = x['cpu_duration_ratio'] > x['cpu_duration_ratio'].quantile(0.95)
            x['is_high_memory_duration_ratio'] = x['memory_duration_ratio'] > x['memory_duration_ratio'].quantile(0.95)
            x['is_high_cpu']=x['is_high_cpu'].astype(int)
            x['is_high_memory']=x['is_high_memory'].astype(int)
            x['is_high_read']=x['is_high_read'].astype(int)
            x['is_high_write']=x['is_high_write'].astype(int)
            x['is_high_cpu_memory_ratio']=x['is_high_cpu_memory_ratio'].astype(int)
            x['is_high_read_write_ratio']=x['is_high_read_write_ratio'].astype(int)
            x['is_high_cpu_duration_ratio']=x['is_high_cpu_duration_ratio'].astype(int)
            x['is_high_memory_duration_ratio']=x['is_high_memory_duration_ratio'].astype(int)
            x['is_peak_hour'] = x['hour'].between(9, 17).astype(int)
            x['query_length'] = x['query'].apply(len)
            x['has_join'] = x['query'].str.contains('JOIN', case=False,regex=True).astype(int)
            x['has_subselect'] = x['query'].str.contains('SELECT.*SELECT', case=False, regex=True).astype(int)
            x['has_union'] = x['query'].str.contains('UNION', case=False).astype(int)
            x['has_sleep'] = x['query'].str.contains('SLEEP', case=False).astype(int)
            x['has_truncate'] = x['query'].str.contains('TRUNCATE', case=False).astype(int)
            x['has_drop'] = x['query'].str.contains('DROP', case=False).astype(int)
            x['has_delete'] = x['query'].str.contains('DELETE', case=False).astype(int)
            x['has_alter'] = x['query'].str.contains('ALTER', case=False).astype(int)
            x['has_insert'] = x['query'].str.contains('INSERT', case=False).astype(int)
            x['has_update'] = x['query'].str.contains('UPDATE', case=False).astype(int)
            x['system_load'] = (x['cpu'] + x['memory']) / 2 
            x['is_high_system_load'] = x['system_load'] > x['system_load'].quantile(0.95)
            x['is_high_system_load'] = x['is_high_system_load'].astype(int)
            x['is_high_io_wait'] = x['io_wait'] == 'Y'
            x['is_high_io_wait'] = x['is_high_io_wait'].astype(int)
            x['num_joins'] = x['query'].str.count('JOIN')
            x['query_complexity_score'] = x['has_join'] * 2 + x['has_subselect'] * 3 + x['has_union'] * 2  # Example scoring
            x.fillna(method='bfill', inplace=True)
        except Exception as e:
            logging.error(f"Error occurred while creating features: {str(e)}")
        return x
    def extract_text_features(self, x: pd.DataFrame) -> pd.DataFrame:
        if 'tf_idf'  in self.all_models:
            self.tf_idf = self.all_models['tf_idf']
        if 'count_vect' in self.all_models:
            self.count_vect = self.all_models['count_vect']
        try:
            self.normalize_query(x)
            query_features=None
            if (self.tf_idf is None or self.count_vect is None) :
                if len(self.vocab)>0 :
                    self.count_vect=CountVectorizer(vocabulary=self.vocab)
                else :
                    self.count_vect=CountVectorizer()
                query_features_count=self.count_vect.fit_transform(x['query'])

                self.tf_idf = TfidfTransformer()
                query_features = self.tf_idf.fit_transform(query_features_count)
                self.vocab = list(self.count_vect.vocabulary_.keys())
            else :
                query_features_count = self.count_vect.transform(x['query'])
                query_features = self.tf_idf.transform(query_features_count)
                # Update vocabulary with new terms if necessary
                new_vocab = set(self.count_vect.get_feature_names_out()) - set(self.vocab)
                if new_vocab:
                    self.vocab.extend(new_vocab)
                    self.count_vect = CountVectorizer(vocabulary=self.vocab)
                    query_features_count = self.count_vect.fit_transform(x['query'])
                    query_features = self.tf_idf.fit_transform(query_features_count)
                    self.tf_idf_feature_names = self.count_vect.get_feature_names_out()
                else:
                    self.tf_idf_feature_names = self.count_vect.get_feature_names_out()

            query_data = pd.DataFrame(query_features.todense(), columns = self.count_vect.get_feature_names_out())
            x = pd.concat([x, query_data], axis=1)
            x.drop('query', axis=1, inplace=True)
        except Exception as e:
            logging.error(f"Error occurred while extracting text features: {str(e)}")
        return x

    def normalize_query(self, x):
        x['query'] = x['query'].apply(lambda x: re.sub(r'[^a-zA-Z0-9\s]', '', x))
        x['query'] = x['query'].apply(lambda x: x.lower())
        x['query'] = x['query'].apply(lambda x: re.sub(r'\s+', ' ', x))
        x['query'] = x['query'].apply(lambda x: x.strip())
        x['query'] = x['query'].apply(lambda x: re.sub(r'\d+', 'NUM', x))
        x['query'] = x['query'].apply(lambda x: re.sub(r'\b\w{1,2}\b', '', x))
        x['query'] = x['query'].apply(lambda x: re.sub(r'\s+', ' ', x))
        x['query'] = x['query'].apply(lambda x: x.strip())
    def encode_column(self, x: pd.DataFrame, col: str) -> pd.DataFrame:
        if 'label_encoders' in self.all_models:
            self.label_encoders = self.all_models['label_encoders']

        if isinstance(x[col], pd.DataFrame):
            x[col] = x[col].stack().reset_index(drop=True)
        unique_values = x[col].unique()
        if len(unique_values) == 1:
            x[col] = 0
        else:
            if x[col].dtypes == 'object':
                if col not in self.label_encoders:
                    le = LabelEncoder()
                    le.fit(unique_values)
                    x[col] = le.transform(x[col])
                    self.label_encoders[col] = le
                else:
                    le = self.label_encoders[col]
                    new_labels = set(unique_values) - set(le.classes_)
                    if new_labels:
                        le.classes_ = list(set(le.classes_) | new_labels)
                        x[col]=le.fit_transform(x[col])
                        self.label_encoders[col] = le
                    else:
                        x[col] = le.transform(x[col])
        return x
    
    def categorize(self, x: pd.DataFrame) -> pd.DataFrame:
        categorical_columns = list(set(x.select_dtypes(include=['object']).columns))
        if self.columns is not None:
            for col in categorical_columns:
                x = self.encode_column(x, col)
        return x
    def assign_label(self, x: pd.DataFrame) -> pd.DataFrame:
        # here maybe we need to use deep learning instead of using normal key like model lstm
        def assign_anomalie(row):
            anomalies = []
            if row['is_high_cpu'] == 1:
                anomalies.append('High CPU Usage')
            if row['is_high_memory'] == 1:
                anomalies.append('High Memory Usage')
            if row['is_high_read'] == 1:
                anomalies.append('High Disk Read')
            if row['is_high_write'] == 1:
                anomalies.append('High Disk Write')
            if row['is_high_cpu_memory_ratio'] == 1:
                anomalies.append('High CPU to Memory Ratio')
            if row['is_high_read_write_ratio'] == 1:
                anomalies.append('High Read to Write Ratio')
            if row['is_high_cpu_duration_ratio'] == 1:
                anomalies.append('High CPU to Duration Ratio')
            if row['is_high_memory_duration_ratio'] == 1:
                anomalies.append('High Memory to Duration Ratio')
            if row['is_long_query'] == 1:
                anomalies.append('Long Running Query')
            if row['state'] == 'active' and row['duration'] > 0.1:
                anomalies.append('Stuck in Active State')
            if row['wait'] == 'Y' and row['is_long_query'] == 1:
                anomalies.append('Potential Deadlock')
            if row['is_high_cpu'] and row['is_high_memory']:
                anomalies.append('High Resource Utilization')
            if row['is_high_system_load'] == 1:
                anomalies.append('High System Load')
            if row['is_high_io_wait'] == 1:
                anomalies.append('High IO Wait')
            if row['has_join'] == 1:
                anomalies.append('Join Query Needs Optimization')
            if row['avg_io_wait_time'] > 10:
                anomalies.append('High IO Wait Time')
            
            if len(anomalies) == 0:
                return 'Normal'
            else:
                return ", ".join(anomalies)

            
        x['type_anomalie'] = x.apply(assign_anomalie, axis=1)
        x['type_anomalie_pos']=x['type_anomalie'].apply(lambda x: self.analyze_sentiment(x)['pos'] )
        x['type_anomalie_neg']=x['type_anomalie'].apply(lambda x: self.analyze_sentiment(x)['neg'] )
        x['type_anomalie_neu']=x['type_anomalie'].apply(lambda x: self.analyze_sentiment(x)['neu'] )
        x['type_anomalie_compound']=x['type_anomalie'].apply(lambda x: self.analyze_sentiment(x)['compound'] )
        x['type_anomalie_is_pos']=x['type_anomalie'].apply(lambda x: self.analyze_sentiment(x)['compound']>0 )


        
    def transform_for_model(self, x: pd.DataFrame) -> pd.DataFrame:
        x = self.createfeatures(x)
        x = self.extract_text_features(x)
        x = self.categorize(x)
        return x
    def train_classifier(self, x: pd.DataFrame) -> pd.DataFrame:
        try:
            ###
            self.final_data=self.detect_anomalies(x)
            self.assign_label(self.final_data) 
            self.misclassified_data = self.final_data[(self.final_data['anomaly_scores'] == 0) & (self.final_data['type_anomalie'] != 'Normal')]
            logging.info(f"Number of misclassified data: {len(self.misclassified_data)/len(self.final_data):.2f}")
            ###
            data=self.classification_trainer(self.final_data)
            return data
        except Exception as e:
            logging.error(f"Error occurred while fitting and transforming data: {str(e)}")
            return pd.DataFrame()
    
    
    def prepare_data_for_prediction(self, x: pd.DataFrame) -> pd.DataFrame:
        x = self.transform_for_model(x)
        self.scaler=self.all_models['scaler']
        expected_features = self.scaler.feature_names_in_
        for feature in expected_features:
            if feature not in x.columns:
               x[feature] = 0
        x_train = self.scaler.transform(x[expected_features])
        x_tensor = tf.convert_to_tensor(x_train, dtype=tf.float32)
        self.autoencoder=self.all_models['autoencoder']
        
        reconstruction = self.autoencoder.predict(x)
        print(reconstruction)
        mse = tf.reduce_mean(tf.square(x_tensor - reconstruction), axis=1)
        autoencoder_scores = mse.numpy()
        self.isolation_forest=self.all_models['isolation_forest']
        isolation_features=self.isolation_forest.feature_names_in_
        isolation_forest_scores = -self.isolation_forest.decision_function(x[isolation_features])
        self.svm=self.all_models['one_class_svm']
        one_class_svm_features=self.svm.feature_names_in_
        one_class_svm_scores = -self.svm.decision_function(x[one_class_svm_features])
        combined_scores = pd.DataFrame({
                'autoencoder': autoencoder_scores,
                'isolation_forest': isolation_forest_scores,
                'one_class_svm': one_class_svm_scores
            })
        self.ensemble=self.all_models['ensemble']
        final_scores = self.ensemble.predict(combined_scores)
        anomaly_scores = pd.Series(final_scores, name='anomaly_scores')
        anomaly_scores.index = x.index
        mean_score = anomaly_scores.mean()
        std_score = anomaly_scores.std()
        threshold = mean_score + 3 * std_score
        anomalous = anomaly_scores >= threshold
        binary_anomalies = anomalous.astype(int)
        final_data = pd.concat([x,  binary_anomalies], axis=1)
        return final_data
    def detect_anomalies(self, x):
        x = self.transform_for_model(x)# we use the same function to transform the data
        x = x.fillna(method='bfill')  # Backward fill
        x_train = self.scaler.fit_transform(x) # when prediction we need to use the same scaler
            # Convert to tensor
        x_tensor = tf.convert_to_tensor(x_train, dtype=tf.float32)
        # Define model architecture
        input_dim = x_train.shape[1]
        encoding_dim = 64
        
        input_layer = Input(shape=(input_dim,))
        flatten = Flatten()(input_layer)
        lstm = MyLSTMLayer(20)(flatten)
        encoder = Dense(encoding_dim, activation='relu')(lstm)
        features_tfidf=self.tf_idf.feature_names_in_
        
        attention_layer = Attention(selected_features=list(features_tfidf))([encoder,encoder])
        decoder = Dense(input_dim, activation='sigmoid')(attention_layer)
        decoder = Reshape((input_dim,))(decoder)
        
        self.autoencoder = Model(inputs=input_layer, outputs=decoder)
        self.autoencoder.compile(optimizer='adam', loss='mse')
        early_stopping = tf.keras.callbacks.EarlyStopping(monitor='loss', patience=3, restore_best_weights=True)
        self.autoencoder.fit(x_tensor, x_tensor, epochs=100, batch_size=32, callbacks=[early_stopping]) #  we load this model in prediction face
        reconstruction = self.autoencoder.predict(x)
        mse = tf.reduce_mean(tf.square(x_tensor - reconstruction), axis=1)
        autoencoder_scores = mse.numpy()
        self.isolation_forest = IsolationForest(contamination='auto', random_state=42)
        self.svm = OneClassSVM(nu=0.05, kernel="rbf", gamma=0.1)
        self.isolation_forest.fit(x) # we need to load this model
        self.svm.fit(x) # we need to load this model
        one_class_svm_scores = -self.svm.decision_function(x)
        isolation_forest_scores = -self.isolation_forest.decision_function(x)
        combined_scores = pd.DataFrame({
                'autoencoder': autoencoder_scores,
                'isolation_forest': isolation_forest_scores,
                'one_class_svm': one_class_svm_scores
            })
        logging.info(combined_scores)
        kmeans = KMeans(n_clusters=2, random_state=42).fit(combined_scores)
        centroids = kmeans.cluster_centers_
        closest, _ = pairwise_distances_argmin_min(combined_scores, centroids)
        distances = np.linalg.norm(combined_scores - centroids[closest], axis=1)
        threshold_distance = np.percentile(distances, 95)
        anomaly_labels = distances > threshold_distance
        logging.info(anomaly_labels)
        self.meta_model = LogisticRegression(random_state=42)
        params_knn = {'n_neighbors': np.arange(1, 25)}
        knn=KNeighborsClassifier()
        knn_gs = GridSearchCV(knn, params_knn, cv=2)
        rf=RandomForestClassifier(random_state=42)
        params_rf = {'n_estimators': [100, 200, 300, 400, 500]}
        rf_gs = GridSearchCV(rf, params_rf, cv=2)
        x_train, x_test_meta, y_train_meta, y_test_meta = train_test_split(combined_scores, anomaly_labels.astype(int), test_size=0.2, random_state=42)
        knn_gs.fit(x_train, y_train_meta)
        rf_gs.fit(x_train, y_train_meta)
        self.knn=knn_gs.best_estimator_
        self.rf=rf_gs.best_estimator_
        self.meta_model.fit(x_train, y_train_meta)
        print('knn: {}'.format(self.knn.score(x_test_meta, y_test_meta)))
        print('rf: {}'.format(self.rf.score(x_test_meta, y_test_meta)))
        print('log_reg: {}'.format(self.meta_model.score(x_test_meta, y_test_meta)))
            #create a dictionary of our models
        estimators=[('knn', self.knn), ('rf', self.rf), ('log_reg', self.meta_model)]
            #create our voting classifier, inputting our models
        self.ensemble = VotingClassifier(estimators, voting='hard') # we need to load this model
        self.ensemble.fit(x_train, y_train_meta)
        print('ensemble: {}'.format(self.ensemble.score(x_test_meta, y_test_meta)))
        final_scores = self.ensemble.predict(combined_scores)
        anomaly_scores = pd.Series(final_scores, name='anomaly_scores')
        anomaly_scores.index = x.index
        mean_score = anomaly_scores.mean()
        std_score = anomaly_scores.std()
        threshold = mean_score + 3 * std_score
        anomalous = anomaly_scores >= threshold
        binary_anomalies = anomalous.astype(int)
        precision, recall, f1_score, _ = precision_recall_fscore_support(binary_anomalies, anomalous, average='binary')
        logging.info(f'Precision: {precision:.2f}')
        logging.info(f'Recall: {recall:.2f}')
        logging.info(f'F1 Score: {f1_score:.2f}')
            # Evaluate the meta-model on test data
        test_scores = self.meta_model.predict_proba(x_test_meta)[:, 1]
        test_anomalous = test_scores >= threshold
        test_binary_anomalies = test_anomalous.astype(int)
        test_precision, test_recall, test_f1_score, _ = precision_recall_fscore_support(y_test_meta, test_binary_anomalies, average='binary')
        logging.info(f'Test Precision: {test_precision:.2f}')
        logging.info(f'Test Recall: {test_recall:.2f}')
        logging.info(f'Test F1 Score: {test_f1_score:.2f}')
        self.final_data = pd.concat([x,  anomaly_scores], axis=1)
        return self.final_data
    def classification_trainer(self, x: pd.DataFrame) -> pd.DataFrame:
        try:
            y=x['type_anomalie']
            le = LabelEncoder()
            y_numeric = le.fit_transform(y)
            category_list = le.classes_
            x.drop('type_anomalie', axis=1, inplace=True)
            x_train, x_test, y_train, y_test = train_test_split(x, y_numeric, test_size=0.2, random_state=42)
            self.decision_tree_classifier = tree.DecisionTreeClassifier(random_state=42)
            self.decision_tree_classifier.fit(x_train, y_train)
            y_eva= self.decision_tree_classifier.predict(x_test)
            y_pred = self.decision_tree_classifier.predict(x)
            accuracy = accuracy_score(y_test, y_eva)
            logging.info(f"Accuracy of the Decision Tree Classifier: {accuracy:.2f}")
            y_pred_labels = [category_list[i] for i in y_pred]
            # Prepare results DataFrame
            predictions_df = pd.DataFrame({'predicted_label': y_pred_labels})
            results = pd.concat([x.reset_index(drop=True), predictions_df], axis=1)
            all_models = {
                'tf_idf': self.tf_idf,
                'count_vect': self.count_vect,
                'label_encoders': self.label_encoders,
                'scaler': self.scaler,
                'autoencoder': self.autoencoder,
                'isolation_forest': self.isolation_forest,
                'one_class_svm': self.svm,
                'meta_model': self.meta_model,
                'knn': self.knn,
                'rf': self.rf,
                'ensemble': self.ensemble,
                'treeClassifier': self.decision_tree_classifier,
                'le_classes': le.classes_
            }
            self.save( all_models)
            return results
            
        except Exception as e:
            logging.error(f"Error occurred while training the classification model: {str(e)}")
    # HERE WHEN DO THE PREDECTION we need to give him the inti row 
    def final_predection(self, x: pd.DataFrame) -> pd.DataFrame:
        try:
            self.decision_tree_classifier=self.all_models['treeClassifier']
            features_tree = self.decision_tree_classifier.feature_names_in_
            x=self.prepare_data_for_prediction(x)
            for feature in features_tree:
                if feature not in x.columns:
                    x[feature] = 0
            category_list = self.all_models['le_classes']
            y_pred = self.decision_tree_classifier.predict(x[features_tree])
            y_pred_labels = [category_list[i] for i in y_pred]
            predictions_df = pd.DataFrame({'predicted_label': y_pred_labels})
            logging.info(predictions_df['predicted_label'])
            return pd.concat([x.reset_index(drop=True), predictions_df], axis=1)
        except Exception as e:
            logging.error(f"Error occurred while transforming data: {str(e)}")
    def evaluate_model(self, x: pd.DataFrame, y_true: pd.Series) -> None:
            y_pred = self.decision_tree_classifier.predict(x)
            precision, recall, f1_score, _ = precision_recall_fscore_support(y_true, y_pred, average='weighted')
            logging.info(f'Precision: {precision:.2f}')
            logging.info(f'Recall: {recall:.2f}')
            logging.info(f'F1 Score: {f1_score:.2f}')
    def log_model_summary(self) -> None:
            logging.info("Model Summary:")
            logging.info(f"TF-IDF Vectorizer: {self.tf_idf}")
            logging.info(f"Count Vectorizer: {self.count_vect}")
            logging.info(f"Decision Tree Classifier: {self.decision_tree_classifier}")
            if self.ensemble:
                logging.info("Ensemble Classifier Models: ")
                for name, model in self.ensemble.named_estimators_.items():
                    logging.info(f"{name}: {model}")
                    logging.info(f"{name}: {model}")

    def save(self, data):
        if os.path.exists(self.path):
            logging.warning(f"Model directory {self.path} already exists. Please delete or rename the directory before saving.")        
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        with open(self.path, 'wb') as f:
            joblib.dump(data, f)
    def load(self):
        if (os.path.exists(self.path)):
             return keras.models.load_model(
                 self.path,
                 custom_objects={
                     'tf': tf,
                     "attention":attention,
                     "MyLSTMLayer": MyLSTMLayer
                 }
             )
        else :
            return {}
        
if __name__ == "__main__":
    activities=pd.read_csv('ProducerConsumer/Pg_activity_Data/activities.csv',sep=';')
    anomaly_detection_pipeline = AnomalyDetectionPipeline(columns=activities.columns,path='ProducerConsumer/Anomaly_detection_pipeline/models/xprocessor.h5')
    train_data=anomaly_detection_pipeline.train_classifier(activities.iloc[0:1000])
    one_row = activities.iloc[0:1]
    logging.info(one_row)
    final=anomaly_detection_pipeline.final_predection(one_row.copy())
    logging.info(final)
    logging.info(f' anomalie predicted before class:: {final['anomaly_scores'][0]}')
    if (final['anomaly_scores'].values[0]==1):
        logging.info(f' anomalie predicted after class:: {final["predicted_label"]}')
    if (final['anomaly_scores'].values[0]==0 and final['predicted_label'].values[0]!='Normal'):
         while(final['anomaly_scores'][0]==0 and final['predicted_label'][0]!='Normal'):
            final_train=anomaly_detection_pipeline.train_classifier(activities.iloc[0:1000])
            final=anomaly_detection_pipeline.final_predection(one_row)
            logging.info(f' anomalie predicted before class:: {final["anomaly_scores"].values}')
            if (final['anomaly_scores'].values[0]==1):
                logging.info(f' anomalie predicted after class:: {final["predicted_label"]}')
            if (final['anomaly_scores'].values[0]==0 and final['predicted_label'].values[0]=='Normal'):
                break
        
        
