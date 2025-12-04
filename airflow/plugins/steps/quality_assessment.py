import pandas as pd
import numpy as np
import logging

class QualityAssessment():
    def __init__(
            self,
            top_rec,
            events_test,
            events_train,
            top_popular,
            als_recommendations,
            k,
    ):
        self.K = k
        self.events_test = events_test.head(1000)
        self.events_train = events_train.head(1000)
        self.als_recommendations = als_recommendations
        self.top_popular = top_popular

        self.top_K_items = self.create_top_popular()
        self.final_recommendations = self.create_final_recomendation()
        self.gt_dict = events_test.groupby('visitorid')['itemid'].apply(list).to_dict()
        self.all_items = events_test['itemid'].unique()

        self.recs_df = self.top_K_items if top_rec else self.final_recommendations

    def create_top_popular(self):
        all_users = self.events_test['visitorid'].unique()
        top_popular_recs = pd.DataFrame({
            'visitorid': np.repeat(all_users, self.K),
            'itemid': np.tile(self.top_popular.head(self.K)['itemid'], len(all_users))
        })
        return top_popular_recs

    def create_final_recomendation(self):
        # Определяем пользователей, которых нет в трейне
        all_test_users = set(self.events_test.visitorid.unique())
        als_users = set(self.events_train.visitorid.unique())

        # Пользователи, которых ALS не знает (cold-start)
        cold_start_users = list(all_test_users - als_users)
        hot_start_users = list(als_users - all_test_users)

        list_new_from_test = {
            item: self.top_K_items
            for item in cold_start_users
        }

        df_top_rec = pd.DataFrame({
            "visitorid": list(list_new_from_test.keys()),
            "itemid": list(list_new_from_test.values())
        }).explode("itemid")
        df_top_rec["score"] = 0

        df = self.als_recommendations.query("visitorid in @hot_start_users")

        # Объединяем ALS и TOP-20 в один общий словарь рекомендаций
        final_recommendations = pd.concat([df_top_rec, df])
        return final_recommendations

    def precision_at_k(self) -> float:
        """Средняя точность рекомендаций по пользователям (Precision@K)."""
        user_precisions = []

        for user_id, user_recs in self.recs_df.groupby('visitorid'):
            if user_id not in self.gt_dict:
                continue

            recommended = user_recs.head(self.K)['itemid'].tolist()
            actual = self.gt_dict[user_id]
            hits = len(set(recommended) & set(actual))
            user_precisions.append(hits / self.K)

        return np.mean(user_precisions) if user_precisions else 0.0


    def recall_at_k(self) -> float:
        """Средняя полнота рекомендаций по пользователям (Recall@K)."""
        user_recalls = []

        for user_id, user_recs in self.recs_df.groupby('visitorid'):
            if user_id not in self.gt_dict:
                continue

            recommended = user_recs.head(self.K)['itemid'].tolist()
            actual = self.gt_dict[user_id]
            hits = len(set(recommended) & set(actual))
            user_recalls.append(hits / len(actual))

        return np.mean(user_recalls) if user_recalls else 0.0


    def coverage_at_k(self) -> float:
        """Доля всех возможных треков, попавших в топ-K рекомендаций хотя бы одному пользователю."""
        recommended_items = self.recs_df.groupby('visitorid').head(self.K)['itemid'].unique()
        return len(recommended_items) / len(self.all_items)


    def novelty_at_k(self) -> float:
        """Средняя новизна рекомендаций (доля новых треков, которых не было у пользователя в train)."""
        train_user_items = self.events_test.groupby('visitorid')['itemid'].apply(set).to_dict()
        user_novelty_scores = []

        for user_id, user_recs in self.recs_df.groupby('visitorid'):
            seen_items = train_user_items.get(user_id, set())
            recommended = user_recs.head(self.K)['itemid'].tolist()
            unseen_count = len(set(recommended) - seen_items)
            user_novelty_scores.append(unseen_count / self.K)

        return np.mean(user_novelty_scores) if user_novelty_scores else 0.0
