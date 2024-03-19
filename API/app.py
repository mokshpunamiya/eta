import csv
import math
import os
import warnings
import pandas as pd
from flask import Flask, request, jsonify
from datetime import datetime, timedelta
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
warnings.filterwarnings('ignore')

app = Flask(__name__)

def get_route_id(input_string, csv_filename):
    with open(csv_filename, 'r') as csvfile:
        csv_reader = csv.DictReader(csvfile)
        for row in csv_reader:
            if row['route_name'] == input_string:
                return int(row['route_id'])

def get_driver_id(input_string, csv_filename):
    if not os.path.exists(csv_filename):
        with open(csv_filename, 'w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            csv_writer.writerow(['driver_id', 'driver_license_id'])

    last_id = 0
    with open(csv_filename, 'r') as csvfile:
        csv_reader = csv.DictReader(csvfile)
        for row in csv_reader:
            last_id = max(last_id, int(row['driver_id']))

    with open(csv_filename, 'r') as csvfile:
        csv_reader = csv.DictReader(csvfile)
        for row in csv_reader:
            if row['driver_license_id'] == input_string:
                return int(row['driver_id'])

    new_id = last_id + 1
    with open(csv_filename, 'a', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow([new_id, input_string])

    return new_id

def get_hour_of_the_day(start_time):
    try:
        start_time_obj = datetime.strptime(start_time, "%H:%M")
        hour_of_the_day = start_time_obj.hour
        return hour_of_the_day
    except ValueError:
        print("Invalid start time format. Please use HH:MM format.")
        return None
    
def calculate_deviation(coord1, coord2):
    try:
        lat1, lon1 = map(float, coord1.split(','))
        lat2, lon2 = map(float, coord2.split(','))

        deviation = math.sqrt((lat2 - lat1)*2 + (lon2 - lon1)*2)
        return deviation
    except (ValueError, TypeError):
        return float('inf')
    
def find_minimum_deviation_coordinate(column):
    min_deviation = float('inf')
    min_coordinate = None

    for i in range(len(column)):
        coordinate = column[i]

        deviation_sum = 0
        for j in range(len(column)):
            if i != j:
                deviation_sum += calculate_deviation(coordinate, column[j])

        if deviation_sum < min_deviation:
            min_deviation = deviation_sum
            min_coordinate = coordinate

    return min_coordinate

def get_ideal_coordinates(filled_data):
    dataset = filled_data.values

    header = filled_data.columns
    dataset = dataset[2:]

    coordinates = {}

    for i in range(2, len(header)):
        column = [row[i] for row in dataset]
        min_coordinate = find_minimum_deviation_coordinate(column)
        coordinates[header[i]] = min_coordinate
        
    return coordinates

def count_parked_and_driving_hours(data, ideal_status_list, hour_of_the_day, required_driving_hours):
    driving_count = 0
    parking_count = 0
    coordinates_index_list = []

    while (True):
        for i in range(len(data)):
            if data[i] == 'driving':
                driving_count += 1
                hour_of_the_day = (hour_of_the_day + 1)%24
                if(driving_count == required_driving_hours):
                    coordinates_index_list.append(-1)
                    return parking_count, driving_count, coordinates_index_list
                if hour_of_the_day == 23:
                    coordinates_index_list.append(driving_count)
            else:
                parking_count += 1
                hour_of_the_day = (hour_of_the_day + 1)%24
                if hour_of_the_day == 23:
                    coordinates_index_list.append(driving_count)
        
        while (True):
            for i in range(len(ideal_status_list)):
                if ideal_status_list[i] == 'driving':
                    driving_count += 1
                    hour_of_the_day = (hour_of_the_day + 1)%24
                    if(driving_count == required_driving_hours):
                        coordinates_index_list.append(-1)
                        return parking_count, driving_count, coordinates_index_list
                    if hour_of_the_day == 23:
                        coordinates_index_list.append(driving_count)
                else:
                    parking_count += 1
                    hour_of_the_day = (hour_of_the_day + 1)%24
                    if hour_of_the_day == 23:
                        coordinates_index_list.append(driving_count)
                        
def process_coordinates_data(coordinates_file_path, desired_route_id):
    coordinates_file = coordinates_file_path

    coordinates_data = pd.read_csv(coordinates_file, header=None, skiprows=1, names=range(200))
    coordinates_data = coordinates_data.dropna(axis=1, how='all')

    average_lengths = coordinates_data.groupby(0).apply(lambda group: int(group.count(axis=1).mean()))
    tolerance = 2

    filtered_data = pd.DataFrame(columns=coordinates_data.columns)

    for route_id, average_length in average_lengths.items():
        if route_id != int(desired_route_id):
            continue
            
        route_data = coordinates_data[coordinates_data[0] == route_id]
        filtered_route_data = route_data[route_data.apply(lambda row: row.count() >= average_length - tolerance and row.count() <= average_length + tolerance, axis=1)]
        filtered_route_data.loc[:, :average_length + tolerance] = filtered_route_data.loc[:, :average_length + tolerance].fillna(method='ffill', axis=1)
        filtered_data = pd.concat([filtered_data, filtered_route_data])

    filtered_data = filtered_data.dropna(axis=1, how='all')
    max_row_length = filtered_data.apply(len, axis=1).max()
    header = ['route_id', 'driver_id'] + [i for i in range(max_row_length - 2)]
    filled_data = filtered_data.copy()
    filled_data.columns = header

    return filled_data

@app.route('/calculate_route', methods=['POST'])
def calculate_route():
    data = request.get_json()

    route_name = data.get('route_name')
    driver_license_id = data.get('driver_license_id')
    start_date_str = data.get('start_date')
    start_time_str = data.get('start_time')

    route_id = str(get_route_id(route_name, r'C:\Users\TechnoPurple\Documents\ETA SN\route_id_data.csv'))
    driver_id = str(get_driver_id(driver_license_id, fr"C:\Users\TechnoPurple\Desktop\NG Trips\Trip_{route_id}\Output Files\driver_id_data.csv"))

    dataset = pd.read_csv(fr"C:\Users\TechnoPurple\Desktop\NG Trips\Trip_{route_id}\Output Files\VehicleStatus.csv")
    coordinates_file = fr"C:\Users\TechnoPurple\Desktop\NG Trips\Trip_{route_id}\Output Files\VehicleCoordinates.csv"

    hour_of_the_day = get_hour_of_the_day(start_time_str)
    input_route_id = int(route_id)
    input_driver_id = int(driver_id)
    input_start_time = int(hour_of_the_day)

    input_for_ideal_coords = process_coordinates_data(coordinates_file, route_id)
    ideal_coords = get_ideal_coordinates(input_for_ideal_coords)
    required_driving_hours = int(len(ideal_coords)-3)

    matching_rows = dataset[
        (dataset['route_id'] == input_route_id) &
        (dataset['driver_id'] == input_driver_id)
    ]

    if matching_rows.empty:
        matching_rows = dataset[
            (dataset['route_id'] == input_route_id)
        ]

    matching_rows = matching_rows.dropna(axis=1,how='all')
    matching_rows.loc[:,'time_diff'] = 0

    for index, row in matching_rows.iterrows():
        start_time_value = row['start_time']
        time_difference = min(abs(start_time_value - input_start_time), (min(start_time_value, input_start_time) + 24) - max(start_time_value, input_start_time))
        matching_rows.loc[index, 'time_diff'] = time_difference

    nearest_start_time = matching_rows['time_diff'].min()

    selected_indices = matching_rows[matching_rows['time_diff'] == nearest_start_time].index
    selected_rows = matching_rows.loc[selected_indices]
    column_to_move = selected_rows.pop('time_diff')
    selected_rows.insert(3, 'time_diff', column_to_move)
    selected_rows = selected_rows.dropna(axis=1,how='all')
    
    X = []
    y = []

    for i in range(4, len(selected_rows.columns)):
        column_name = selected_rows.columns[i]
        driving_count = selected_rows[column_name].eq('driving').sum()
        parked_count = selected_rows[column_name].eq('parked').sum()
        X.append([driving_count, parked_count])
        if driving_count > parked_count:
            y.append('driving')
        else:
            y.append('parked')

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    clf = RandomForestClassifier(random_state=42)
    clf.fit(X_train, y_train)

    y_pred = clf.predict(X_test)

    accuracy = accuracy_score(y_test, y_pred)

    predicted_status_list = []
    ideal_status_list = ['parked','parked','parked','parked','parked','parked','parked','driving','driving','driving','driving','driving','driving','parked','parked','driving','driving','driving','driving','driving','driving','driving','driving','parked']

    selected_rows_start_time = matching_rows['start_time'][selected_indices[0]]
    min_time = min(input_start_time, selected_rows_start_time)
    max_time = max(input_start_time, selected_rows_start_time)
    time_diff = min(abs(selected_rows_start_time-input_start_time), (min_time+24)-max_time)
    if selected_rows_start_time-input_start_time < 0 and time_diff == abs(selected_rows_start_time-input_start_time):
        time_diff = selected_rows_start_time - input_start_time

    if time_diff == 0:
        for i in range(len(X)):
            prediction = clf.predict([X[i]])
            column_name = dataset.columns[i + 3]
            predicted_status_list.append(prediction[0])

    elif time_diff > 0:
        if(input_start_time > abs(input_start_time - selected_rows_start_time)):
            for i in range(input_start_time, 24):
                predicted_status_list.append(ideal_status_list[i])
            for i in range(0,selected_rows_start_time):
                predicted_status_list.append(ideal_status_list[i])
        else:
            for i in range(input_start_time, selected_rows_start_time):
                predicted_status_list.append(ideal_status_list[i])

        for i in range(len(X)):
            prediction = clf.predict([X[i]])
            column_name = dataset.columns[i + 3]
            predicted_status_list.append(prediction[0])

    else:
        diff = abs(time_diff)
        for i in range(diff,len(X)):
            prediction = clf.predict([X[i]])
            column_name = dataset.columns[i + 3]
            predicted_status_list.append(prediction[0])

    parking_count, driving_count, coordinates_index_list= count_parked_and_driving_hours(predicted_status_list, ideal_status_list, hour_of_the_day, required_driving_hours)
    total_duration = parking_count + driving_count

    route_coordinates = [(float(lat), float(lon)) for coord in ideal_coords.values() for lat, lon in [coord.split(',')]]
    total_no_of_days = len(coordinates_index_list) - 1

    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    start_time = datetime.strptime(start_time_str, "%H:%M")
    end_time = start_time + timedelta(hours=total_duration)
    end_date = start_date + timedelta(days=total_no_of_days)

    result = {
        "end_date": end_date.strftime("%Y-%m-%d"),
        "end_time": end_time.strftime("%H:%M"),
        "total_duration": total_duration,
        "coordinates_per_day": {
            f"Day {i}": route_coordinates[coordinates_index_list[i]] for i in range(len(coordinates_index_list))
        }
    }

    return jsonify(result)

if __name__ == '__main__':
    app.run(debug=True)
