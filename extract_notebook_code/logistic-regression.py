import pandas as pd
from dotenv import dotenv_values
from sqlalchemy import create_engine, text

config = dotenv_values()

DB_NAME = config.get('DB_NAME')
DB_USER = config.get('DB_USER')
DB_HOST = config.get('DB_HOST')
DB_PASSWORD = config.get('DB_PASSWORD')
DB_PORT = config.get('DB_PORT')

engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

def get_data(season):
    query = f"""
        SELECT
            d.id AS driver_id,
            r.round AS round,
            d.name AS driver_name,
            fp1.position_number AS fp1_result,
            fp2.position_number AS fp2_result,
            fp3.position_number AS fp3_result,
            q.position_number AS q_result,
            rr.grid_position_number AS grid_position,
            rr.position_number AS finish_position
        FROM 
            drivers d
        JOIN 
            races_fp1_results fp1 ON d.id = fp1.driver_id
        JOIN 
            races_fp2_results fp2 ON d.id = fp2.driver_id
        JOIN 
            races_fp3_results fp3 ON d.id = fp3.driver_id
        JOIN 
            races_qualifying_results q ON d.id = q.driver_id
        JOIN 
            races_race_results rr ON d.id = rr.driver_id
        JOIN 
            races r ON fp1.race_id = r.id AND fp2.race_id = r.id AND fp3.race_id = r.id AND q.race_id = r.id AND rr.race_id = r.id
        JOIN
            seasons s ON s.id = r.season_id
        WHERE 
            s.year = :season
    """
    
    with engine.connect() as connection:
        df = pd.read_sql(text(query), connection, params={'season': season})
    
    return df

df = get_data(2019)
df.head()

import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix

df = df.dropna()
features = ['fp1_result', 'fp2_result', 'fp3_result', 'q_result', 'grid_position']
X = df[features]
y = df['finish_position']

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)
X_test_scaled = scaler.transform(X_test)

lr = LogisticRegression(random_state=42)
lr.fit(X_train_scaled, y_train)

y_pred = lr.predict(X_test_scaled)

accuracy = accuracy_score(y_test, y_pred)
print(f"Accuracy: {accuracy:.2f}")

print("Classification Report:")
print(classification_report(y_test, y_pred))

print("Confusion Matrix:")
print(confusion_matrix(y_test, y_pred))

comparison_df = pd.DataFrame({'Actual': y_test, 'Predicted': y_pred})
comparison_df.head(10)

import matplotlib.pyplot as plt

plt.figure(figsize=(10, 6))
plt.scatter(comparison_df['Actual'], comparison_df['Predicted'], color='blue', alpha=0.5)
plt.plot([min(comparison_df['Actual']), max(comparison_df['Actual'])], 
         [min(comparison_df['Actual']), max(comparison_df['Actual'])], 
         color='red', linestyle='--')
plt.title('Actual vs Predicted Positions')
plt.xlabel('Actual Position')
plt.ylabel('Predicted Position')
plt.grid(True)
plt.show()

driver_names = df.loc[X_test.index]['driver_name'].tolist()
rounds = df.loc[X_test.index]['round'].tolist()

visualization_df = pd.DataFrame({'Driver Name': driver_names,
                                 'Round': rounds,
                                 'Actual Position': y_test.values,
                                 'Predicted Position': y_pred})

round = 1
filtered_df = visualization_df[visualization_df['Round'] == round]
sorted_df = filtered_df.sort_values(by='Actual Position')
sorted_df.head(20)

