import flask
from flask import jsonify, request
import sqlite3


app = flask.Flask(__name__)
app.config["DEBUG"] = True


#Converts the sql row select from tuple to dictionary
def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


def normalize_query_param(value):
    """
    Given a non-flattened query parameter value,
    and if the value is a list containing 1 item, 
    flatten the value to a scalar.
    :param value: a value from a query parameter
    "return: a normalized query paramter value"
    """
    return value if len(value) > 1 else value[0]


def normalize_query(params):
    """
    Converts query parameters from only containing one value
    to include parameters with multiple values as lists.
    :param params: a flask query parameters data structure
    :return: a dict of normalized query parameters
    """
    params_non_flat = params.to_dict(flat=False)
    return {k: normalize_query_param(v) for k, v in params_non_flat.items()}



# File Location of the database
db_file = 'data/countries.db'

#Set the homepage to some basic descriptive html
@app.route('/', methods =['GET'])
def home():
    return """<h1>Country Statistics</h1>
                <p>This site is an API for accessing country and continent statistics</p>"""

#Page that returns the complete countries table as JSON
@app.route('/api/v1/resources/countries/all', methods = ['GET'])
def api_all():
    conn = sqlite3.connect(db_file)
    conn.row_factory = dict_factory
    cur = conn.cursor()

    sql_select_all = "SELECT * FROM countries"
    all_books = cur.execute(sql_select_all).fetchall()

    conn.close()
    return jsonify(all_books)

#Returns each country that is requested as well as all countries 
# within continents that are requested as JSON
# Specific countries can be accessed by specifying the name/value pair
# in the query string, ex. '...?Country=Algeria&Country=Morocco...' or
# '...?Continent=Africa...'
@app.route('/api/v1/resources/countries', methods = ['GET'])
@app.route('/api/v1/resources/Countries', methods = ['GET'])
def api_by_country():
    #get normalized paramaters from URL query
    normalized_params = normalize_query(request.args)

    #Establish the db connection
    conn = sqlite3.connect(db_file)
    #Import rows as dictionaries
    conn.row_factory = dict_factory
    cur = conn.cursor()
    
    #Check if country was specified
    if(normalized_params.get('Country')):
        requested_countries = normalized_params['Country']
        #check if multiple values for country
        if(type(requested_countries) == list):
            country_sql_select = "SELECT * FROM countries WHERE Country IN " 
            country_sql_params = "("+"?, "*(len(requested_countries)-1) +"?)" 
            country_sql_select += country_sql_params
            requested_countries = tuple(requested_countries)

        #Single value is a string
        else:
            country_sql_select = "SELECT * FROM countries WHERE Country = (?)"
            requested_countries = (requested_countries,)
    # No country requested
    else:
        country_sql_select = ""
    #Check if continent was specified
    if(normalized_params.get('Continent')):
        requested_continents = normalized_params['Continent']
        #check if multiple values for country
        if(type(requested_continents) == list):
            continent_sql_select = "SELECT * FROM countries WHERE Region IN " 
            continent_sql_params = "("+"?, "*(len(requested_continents)-1) +"?)" 
            continent_sql_select += continent_sql_params
            requested_continents = tuple(requested_continents)
        #Single value is a string
        else:
            continent_sql_select = "SELECT * FROM countries WHERE Region = (?)"
            requested_continents =(requested_continents,)
    # No country requested
    else:
        continent_sql_select = ""    
        requested_continents = ()
    

    country_sql_select += ";"    
    continent_sql_select += ";"
    #requested_country_stats = cur.execute(params_sql_select).fetchall()

    #Sanitize the sql statement to prevent SQL injection
    requested_country_stats = cur.execute(country_sql_select, requested_countries).fetchall()
    requested_continent_stats = cur.execute(continent_sql_select, requested_continents).fetchall()

    requested_country_stats = requested_country_stats + requested_continent_stats


    conn.close()
    return(jsonify(requested_country_stats))


#Returns data aggregated by each continent that is requested as JSON
# Specific countries can be accessed by specifying the name/value pair
# in the query string, ex. '...?Continent=Africa...'
@app.route('/api/v1/resources/continents', methods=['GET'])
@app.route('/api/v1/resources/Continents', methods=['GET'])
def api_by_continent():
    #get paramaters from URL query
    normalized_params = normalize_query(request.args)

    #Establish the db connection
    conn = sqlite3.connect(db_file)
    #Import rows as dictionaries
    conn.row_factory = dict_factory
    cur = conn.cursor()


    if(normalized_params.get('Continent')):
        requested_continents = normalized_params['Continent']
        #check if multiple values for continent
        if(type(requested_continents) == list):
            requested_continents = tuple(requested_continents)

            continent_sql_select_start  = "SELECT Region, SUM(Population), SUM(Area), AVG(Pop_Density), AVG(Coastline_Ratio), SUM(Net_Migration), "
            continent_sql_select_middle1 = "AVG(Infant_Mortality), AVG(GDP_Per_Capita), AVG(Literacy), AVG(Phones), AVG(Arable), AVG(Crops), AVG(Other), "
            continent_sql_select_middle2 = "AVG(Climate), AVG(Birthrate), AVG(Deathrate), AVG(Agriculture), AVG(Industry), AVG(Service) "
            continent_sql_select_end = "FROM countries WHERE Region IN " + "("+("?, ")*(len(requested_continents)-1) + "?) GROUP BY Region "
            continent_sql_select = continent_sql_select_start + continent_sql_select_middle1 + continent_sql_select_middle2 + continent_sql_select_end
        #requested_continents is string
        else:
            requested_continents = (requested_continents,)
            #= "SELECT * FROM countries WHERE Region = " + requested_continent
            continent_sql_select_start  = "SELECT Region, SUM(Population), SUM(Area), AVG(Pop_Density), AVG(Coastline_Ratio), SUM(Net_Migration), "
            continent_sql_select_middle1 = "AVG(Infant_Mortality), AVG(GDP_Per_Capita), AVG(Literacy), AVG(Phones), AVG(Arable), AVG(Crops), AVG(Other), "
            continent_sql_select_middle2 = "AVG(Climate), AVG(Birthrate), AVG(Deathrate), AVG(Agriculture), AVG(Industry), AVG(Service) "
            continent_sql_select_end = "FROM countries WHERE Region = (?) GROUP BY Region "
            continent_sql_select = continent_sql_select_start + continent_sql_select_middle1 + continent_sql_select_middle2 + continent_sql_select_end
    # No country requested
    else:
        continent_sql_select = ""


    continent_sql_select += ";"
    requested_continent_stats = cur.execute(continent_sql_select, requested_continents).fetchall() 
    
    #Close the SQL database
    conn.close()
    return(jsonify(requested_continent_stats))


#404 resource not found error page
@app.errorhandler(404)
def page_not_found(e):
    return "<h1>404</h1><p>The resource could not be found</p>", 404


app.run()
