import requests

# Take input from the user
route_name = input("Enter route name: ")
driver_license_id = input("Enter driver's license ID: ")
start_date = input("Enter start date (YYYY-MM-DD): ")
start_time = input("Enter start time (HH:MM): ")

# Create the input_data JSON
input_data = {
    "route_name": route_name,
    "driver_license_id": driver_license_id,
    "start_date": start_date,
    "start_time": start_time
}

response = requests.post('http://localhost:5000/calculate_route', json=input_data)

if response.status_code == 200:
    result = response.json()
    print("End Date:", result["end_date"])
    print("End Time:", result["end_time"])
    print("Total Duration:", result["total_duration"])
    print("Coordinates per Day:")
    for day, coordinates in result["coordinates_per_day"].items():
        print(f"Day {day}: {coordinates}")
else:
    print("Error:", response.status_code)