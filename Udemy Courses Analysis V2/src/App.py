import streamlit as st
import pandas as pd
from data_preprocessing import load_and_preprocess_data
from model import train_model

def load_data(): 
    df, X, y, le_title, le_wishlist, scaler = load_and_preprocess_data(r'path\to\dataset\dataset.csv')
    return df, X, y, le_title, le_wishlist, scaler

def main():
    st.title('Udemy Course Rating Prediction')

    # Load and preprocess data
    df, X, y, le_title, le_wishlist, scaler = load_data()

    rf, X_test, y_test, y_pred, mse, mae, r2 = train_model(X, y)

    # Sidebar for navigation
    page = st.sidebar.selectbox(
        'Navigate',
        ['Prediction', 'Model Performance', 'Dataset Insights']
    )
    
    if page == 'Prediction': # Predict course rating based on user input data
        st.header('Course Rating Prediction')

        input_data = {}

        # Dropdown menu for Title
        available_titles = list(le_title.classes_)
        selected_title = st.selectbox('Select Course Title', available_titles)
        input_data['title'] = selected_title

        input_data['num_subscribers'] = st.number_input('Number of Subscribers', min_value=0, value=100)
        input_data['num_reviews'] = st.number_input('Number of Reviews', min_value=0, value=10)
        input_data['num_published_practice_tests'] = st.number_input('Number of Practice Tests', min_value=0, value=0)
        input_data['price_detail__amount'] = st.number_input('Course Price', min_value=0.0, value=10.0, step=0.1)

        wishlist_status = st.selectbox('Is Wishlisted', [False, True])
        input_data['is_wishlisted'] = wishlist_status

        input_df = pd.DataFrame([input_data])

        input_df['title'] = le_title.transform([input_df['title'][0]])
        input_df['is_wishlisted'] = int(input_df['is_wishlisted'][0])

        input_df = input_df[X.columns]

        numerical_cols = ['num_subscribers', 'num_reviews', 'price_detail__amount']
        input_df[numerical_cols] = scaler.transform(input_df[numerical_cols])

    
        if st.button('Predict Rating'):
            prediction = rf.predict(input_df)
            st.success(f'Predicted Course Rating: {prediction[0]:.2f}')

    elif page == 'Model Performance': # Model performance metrics and feature importance
        st.header('Model Performance Metrics')
        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric('Mean Squared Error', f'{mse:.4f}')
        with col2:
            st.metric('Mean Absolute Error', f'{mae:.4f}')
        with col3:
            st.metric('R-squared', f'{r2:.4f}')

        st.subheader('Feature Importance') 
        feature_importance = pd.DataFrame({
            'feature': X.columns,
            'importance': rf.feature_importances_
        }).sort_values('importance', ascending=False)

        st.bar_chart(feature_importance.set_index('feature'))

    
    elif page == 'Dataset Insights': # Dataset overview to understand the data
        st.header('Dataset Overview')

        st.subheader('Dataset Statistics')
        st.write(X.describe())

        st.subheader('Feature Correlation')
        corr_matrix = X.corr()
        st.dataframe(corr_matrix)

# Run the app
if __name__ == '__main__':
    main()
