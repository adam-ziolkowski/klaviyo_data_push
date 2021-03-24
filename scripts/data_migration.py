import json
import requests
import base64
import datetime
import pandas

import secrets

# line separated json entries file
data_file = '../data/bq-results-20201209-121858-unm2a3z1b4di.json'

# csv file with polish names
names_file = '../data/lista_imion.csv'

# kliavo endpoint
base_url = 'https://a.klaviyo.com/api/track'

# load list of polish names
colnames = ['IMIĘ_PIERWSZE' ,'LICZBA_WYSTĄPIEŃ']
names_data = pandas.read_csv(names_file, names=colnames)


def fix_name(input_name):
	# convert string to a list of words (split on space) and capitalize to
	# match names in names csv
	name_parts = [s.capitalize() for s in input_name.split()]

	# test weird values
	# if len(name_parts) > 2:
	# 	print (name_parts)
	# return input_name

	if name_parts[0] in names_data.IMIĘ_PIERWSZE.values:
		name = name_parts[0]
		last_name = name_parts[1]
	else:
		name = name_parts[1]
		last_name = name_parts[0]

	# fixed name format
	output_name = '{} {}'.format(name, last_name)

	# print('{} {} \t {}'.format(name, last_name, name_parts)

	return output_name


# example request: https://www.klaviyo.com/docs
# {
#   "token" : "PUBLIC_API_KEY",
#   "event" : "Elected President",
#   "customer_properties" : {
#     "$email" : "thomas.jefferson@klaviyo.com"
#   },
#   "properties" : {
#     "PreviouslyVicePresident" : true,
#     "YearElected" : 1801,
#     "VicePresidents" : ["Aaron Burr", "George Clinton"]
#   },
#   "time" : 1607513547
# }


def process_json(js_in):
	js_out = {}

	token = secrets.KLAVIYO_PUBLIC_TOKEN
	event_name = "Placed Order"

	# keep field names here in case they ever change
	customer_properties_field = "customer_properties"
	properties_field = "properties"

	# fix names to be in name last_name format
	full_name = fix_name(js_in["Nazwa"]).split()
	name = full_name[0]
	last_name = full_name[1]

	# authentication token
	js_out["token"] = token
	# event name
	js_out["event"] = event_name

	# profile properties
	js_out[customer_properties_field] = {}
	js_out[customer_properties_field]["$email"] = js_in["Email"]
	js_out[customer_properties_field]["$first_name"] = name
	js_out[customer_properties_field]["$last_name"] = last_name
	js_out[customer_properties_field]["$phone_number"] = js_in["Telefon"]
	# custom profile properties
	js_out[customer_properties_field]["customer_id"] = js_in["ID_Klienta"]

	# event properties
	js_out[properties_field] = {}
	js_out[properties_field]["Nazwa"] = js_in["Nazwa"]
	js_out[properties_field]["IdKlienta"] = js_in["ID_Klienta"]
	js_out[properties_field]["Telefon"] = js_in["Telefon"]
	js_out[properties_field]["Dieta"] = js_in["Dieta"]
	js_out[properties_field]["Kalorie"] = js_in["Kalorie"]
	js_out[properties_field]["Zaplacono"] = js_in["Zap__acono"]
	js_out[properties_field]["LiczbaDni"] = js_in["Zam__dni"]
	js_out[properties_field]["Dodatki"] = js_in["Op__aty_dodatkowe"]
	# todo: check date format
	js_out[properties_field]["PoczatekDiety"] =  datetime.datetime.strptime(js_in["Data_od"], '%Y-%m-%d').strftime("%d/%m/%Y")
	js_out[properties_field]["KoniecDiety"] = datetime.datetime.strptime(js_in["Data_do"], '%Y-%m-%d').strftime("%d/%m/%Y")
	# parse date to unix timestamp
	unix_timestamp = int(datetime.datetime.strptime(js_in["Data_dodania"], '%Y-%m-%d').timestamp())
	js_out["time"] = unix_timestamp

	return js_out


def klaviyo_send_request(js):
	# encode json in base64
	b64 = base64.b64encode(json.dumps(js).encode('utf-8'))

	# insert the encoded json as data parameter
	params = {}
	params['data'] = b64

	# make the request
	r = requests.get('{}'.format(base_url), params=params)

	# print the request result
	print(r.status_code)
	# todo: error handling


# main script

counter = 0
with open(data_file) as json_file:
	for line in json_file:
		# json.load(json_file)
		js_in = json.loads(line)

		# skip records which are not payed yet
		if js_in['Zap__acono'] == 0:
			continue

		js_out = process_json(js_in)
		counter += 1
		# print(js_out)

		# todo: uncomment this to place request
		klaviyo_send_request(js_out)

		# todo: remove this to process all entries
		# exit()

print('inserted {} records'.format(counter))
