import pandas as pd
from sklearn.preprocessing import StandardScaler, LabelEncoder

def load_and_preprocess_data(filepath):
    # Load the dataset
    df = pd.read_csv(filepath)
    df.dropna(inplace=True)

    # Drop unnecessary columns
    columns_to_drop = [
        'id', 'avg_rating', 'url', 'created', 'num_published_lectures',
        'discount_price__currency', 'discount_price__amount', 'published_time',
        'is_paid', 'discount_price__price_string',
        'price_detail__currency', 'price_detail__price_string', 'avg_rating_recent'
    ]
    df.drop(columns=columns_to_drop, axis=1, inplace=True)
    
    rating = df['rating']
    df.drop(columns=['rating'], axis=1, inplace=True)
    df['rating'] = rating

    # encoding
    le_title = LabelEncoder()
    df['title'] = le_title.fit_transform(df['title'])

    le_wishlist = LabelEncoder()
    df['is_wishlisted'] = le_wishlist.fit_transform(df['is_wishlisted'])

    X = df.drop('rating', axis=1)
    y = df['rating']

    # scaling
    scaler = StandardScaler()
    X[['num_subscribers', 'num_reviews', 'price_detail__amount']] = scaler.fit_transform(
        X[['num_subscribers', 'num_reviews', 'price_detail__amount']]
    )

    return df, X, y, le_title, le_wishlist, scaler
