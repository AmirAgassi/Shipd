import streamlit as st
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from data_preprocessing import load_and_preprocess_data
from model import train_model

def load_data(): 
    df, X, y, le_title, le_wishlist, scaler = load_and_preprocess_data('dataset/dataset.csv')
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
    
    if page == 'Prediction':
        st.header('Course Rating Prediction')
        
        try:
            input_data = {}

            # Dropdown menu for Title with error handling
            available_titles = list(le_title.classes_)
            selected_title = st.selectbox('Select Course Title', available_titles)
            input_data['title'] = selected_title

            # Numerical inputs with validation
            input_data['num_subscribers'] = st.number_input('Number of Subscribers', min_value=0, value=100)
            input_data['num_reviews'] = st.number_input('Number of Reviews', min_value=0, value=10)
            input_data['num_published_practice_tests'] = st.number_input('Number of Practice Tests', min_value=0, value=0)
            input_data['price_detail__amount'] = st.number_input('Course Price', min_value=0.0, value=10.0, step=0.1)

            wishlist_status = st.selectbox('Is Wishlisted', [False, True])
            input_data['is_wishlisted'] = int(wishlist_status)  # Convert boolean to int directly

            if st.button('Predict Rating'):
                # Create DataFrame with user input
                input_df = pd.DataFrame([input_data])
                
                # Transform categorical variables
                input_df['title'] = le_title.transform([input_df['title'][0]])
                # No need to transform is_wishlisted as it's already an int
                
                # Ensure column order matches training data
                input_df = input_df[X.columns]
                
                # Scale numerical features using the same scaler
                numerical_cols = ['num_subscribers', 'num_reviews', 'price_detail__amount']
                input_df[numerical_cols] = scaler.transform(input_df[numerical_cols])
                
                # Make prediction
                prediction = rf.predict(input_df)
                
                # Display prediction with confidence interval
                st.success(f'Predicted Course Rating: {prediction[0]:.2f} ⭐')
                st.info('Note: This prediction is based on historical data and may vary from actual ratings.')

        except Exception as e:
            st.error(f'An error occurred: {str(e)}. Please check your inputs and try again.')
    
    elif page == 'Model Performance':
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
    
    elif page == 'Dataset Insights':
        st.header('Dataset Overview')

        # Dataset Statistics
        st.subheader('Dataset Statistics')
        st.write(df.describe())

        # Correlation Heatmap
        st.subheader('Feature Correlation')
        plt.figure(figsize=(10, 8))
        correlation = df.corr()
        sns.heatmap(correlation, annot=True, cmap='coolwarm', center=0)
        st.pyplot(plt.gcf())
        plt.close()

        # Distribution of Ratings
        st.subheader('Rating Distribution')
        plt.figure(figsize=(10, 6))
        sns.histplot(data=df, x='rating', bins=30, kde=True)
        plt.title('Distribution of Course Ratings')
        st.pyplot(plt.gcf())
        plt.close()

        # Price vs Rating Scatter Plot
        st.subheader('Price vs Rating Relationship')
        plt.figure(figsize=(10, 6))
        sns.scatterplot(data=df, x='price_detail__amount', y='rating', alpha=0.5)
        plt.title('Course Price vs Rating')
        st.pyplot(plt.gcf())
        plt.close()

        # Subscribers vs Rating Box Plot
        st.subheader('Subscribers vs Rating')
        plt.figure(figsize=(12, 6))
        df['subscriber_range'] = pd.qcut(df['num_subscribers'], q=5, labels=['Very Low', 'Low', 'Medium', 'High', 'Very High'])
        sns.boxplot(data=df, x='subscriber_range', y='rating')
        plt.title('Rating Distribution by Subscriber Range')
        plt.xticks(rotation=45)
        st.pyplot(plt.gcf())
        plt.close()

# Run the app
if __name__ == '__main__':
    main()
