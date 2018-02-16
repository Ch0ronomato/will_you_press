import requests

text = requests.get("http://will_you_press_etl:5000/8893")
print(text.text)