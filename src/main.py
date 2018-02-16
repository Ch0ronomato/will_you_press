import requests

for i in range(1,5):
    text = requests.get("http://willyoupress_node{0}.will_you_press_etl:5000/8893".format(i))
    print("node {0} test: {1}".format(i, text.text))

# setup a data base
# has the master give the data base the results
