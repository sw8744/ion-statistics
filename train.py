import psycopg2
import pandas as pd
import dotenv
import os

dotenv_file = dotenv.find_dotenv()
dotenv.load_dotenv(dotenv_file)

connection = psycopg2.connect(
    host="ionya.cc",
    database=os.environ.get("DB_NAME"),
    port=int(os.environ.get("DB_PORT")),
    user=os.environ.get("DB_USER"),
    password=os.environ.get("DB_PASSWORD")
    )
print("DB_Connected")

cur = connection.cursor()
cur.execute("SELECT * FROM " + os.environ.get("TABLE_NAME"))
ns_arr = cur.fetchall()

weekday_Mon, weekday_Tue, weekday_Wed, weekday_Thu, weekday_Fri = [], [], [], [], []

for ns in ns_arr:
    weekday = ns[1].weekday()
    temp = [ns[1], ns[2], ns[3], ns[4], ns[5]]
    if weekday == 0:
        weekday_Mon.append(temp)
    elif weekday == 1:
        weekday_Tue.append(temp)
    elif weekday == 2:
        weekday_Wed.append(temp)
    elif weekday == 3:
        weekday_Thu.append(temp)
    elif weekday == 4:
        weekday_Fri.append(temp)


df_Mon = pd.DataFrame(weekday_Mon, columns=['at_date', 'at_time', 'seat', 'grade', 'uuid'])
df_Tue = pd.DataFrame(weekday_Tue, columns=['at_date', 'at_time', 'seat', 'grade', 'uuid'])
df_Wed = pd.DataFrame(weekday_Wed, columns=['at_date', 'at_time', 'seat', 'grade', 'uuid'])
df_Thu = pd.DataFrame(weekday_Thu, columns=['at_date', 'at_time', 'seat', 'grade', 'uuid'])
df_Fri = pd.DataFrame(weekday_Fri, columns=['at_date', 'at_time', 'seat', 'grade', 'uuid'])
print("Fetch Data Success")

df_list = [df_Mon, df_Tue, df_Wed, df_Thu, df_Fri]
for i in range(len(df_list)):
    df = df_list[i]
    df_final = pd.DataFrame(columns=['at_date', 'at_time', 'grade', 'seat_info'])
    at_date = df['at_date'].unique()
    at_time = df['at_time'].unique()
    grade = df['grade'].unique()
    seat = ['A1', 'A2', 'A3', 'A4', 'A5', 'A6', 'B1', 'B2', 'B3', 'B4', 'B5', 'B6', 'C1', 'C2', 'C3', 'C4', 'C5', 'C6', 'D1', 'D2', 'D3', 'D4', 'D5', 'D6', 'E1', 'E2', 'E3', 'E4', 'E5', 'E6', 'F1', 'F2', 'F3', 'F4', 'F5', 'F6']
    for date in at_date:
        for g in grade:
            for time in at_time:
                seat_info = {}
                for s in seat:
                    temp = df[(df['at_date'] == date) & (df['at_time'] == time) & (df['seat'] == s)]['uuid'].values
                    if temp.size == 0:
                        temp = [-1]
                    seat_info[s] = temp[0]
                new_data = [date, time, g, seat_info]
                new_df = pd.DataFrame([new_data], columns=['at_date', 'at_time', 'grade', 'seat_info'])
                df_final = pd.concat([df_final, new_df])
    df_list[i] = df_final
df_Mon, df_Tue, df_Wed, df_Thu, df_Fri = df_list[0], df_list[1], df_list[2], df_list[3], df_list[4]
print("Data Preprocessing Success")

cur.close()
connection.close()
print("DB_Disconnected")

