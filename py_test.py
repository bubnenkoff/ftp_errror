import requests

url = "http://127.0.0.1:5002/start-job"

data = {
	"fz": ["fz44"],
	"sections": ["notifications", "protocols"],
	"start_year": 2016,
	"atLeastNotProcessedXML": 150000,
	"onlyLastMonth" : False
}

resp = requests.post(url, json=data)
