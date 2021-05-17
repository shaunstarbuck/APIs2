import pandas as pd


def clean_dataframe(countries_df):
    '''
    : params: raw dataframe from pd.read_csv
    : return: countries_df ready for insertion into SQL table 
    '''
    #Rename df columns to align with the sqlite database
    countries_df = countries_df.rename(columns={ 'Area (sq. mi.)': 'Area', 
                                 'Pop. Density (per sq. mi.)':'Pop_Density',
                                'Coastline (coast/area ratio)': 'Coastline_Ratio',
                                'Net migration': 'Net_Migration',
                                'Infant mortality (per 1000 births)': 'Infant_Mortality',
                                'GDP ($ per capita)': 'GDP_Per_Capita',
                                'Literacy (%)': 'Literacy',
                                'Phones (per 1000)': "Phones",
                                'Arable (%)': 'Arable',
                                'Crops (%)': 'Crops', 
                                'Other (%)': "Other"})




    def cast_to_float(string):
        '''Dont convert floats, specifically Nans
        :params: string representing a number with commas or Nan (float) 
        :return: string converted to float or NaN if already Nan
        '''
        if(type(string) == str):
            string = float(string.replace(',', '.'))
        return string


    # Remove white space from end of country name
    df_size = len(countries_df.index)

    for i in range(0, df_size):
        # remove trailing whitespace from country names
        countries_df.loc[i,"Country"] = countries_df.loc[i]["Country"].strip()
        # convert Pop_Density, Agriculture, Net_Migration, Infant_Mortality, Literacy, Phones, Arable, Crops,
        # Other, Climate, Birthrate, Deathrate, Agriculture, Industry, Service
        # columns from mangled string to float
        countries_df.loc[i,"Pop_Density"] = cast_to_float(countries_df.loc[i,"Pop_Density"])
        countries_df.loc[i,"Coastline_Ratio"] = cast_to_float(countries_df.loc[i,"Coastline_Ratio"])
        countries_df.loc[i,"Net_Migration"] = cast_to_float(countries_df.loc[i,"Net_Migration"])
        countries_df.loc[i,"Infant_Mortality"] = cast_to_float(countries_df.loc[i,"Infant_Mortality"])
        countries_df.loc[i,"Literacy"] = cast_to_float(countries_df.loc[i,"Literacy"])
        countries_df.loc[i,"Phones"] = cast_to_float(countries_df.loc[i,"Phones"])
        countries_df.loc[i,"Arable"] = cast_to_float(countries_df.loc[i,"Arable"])
        countries_df.loc[i,"Crops"] = cast_to_float(countries_df.loc[i,"Crops"])
        countries_df.loc[i,"Other"] = cast_to_float(countries_df.loc[i,"Other"])
        countries_df.loc[i,"Climate"] = cast_to_float(countries_df.loc[i,"Climate"])
        countries_df.loc[i,"Birthrate"] = cast_to_float(countries_df.loc[i,"Birthrate"])
        countries_df.loc[i,"Deathrate"] = cast_to_float(countries_df.loc[i,"Deathrate"])
        countries_df.loc[i,"Agriculture"] = cast_to_float(countries_df.loc[i,"Agriculture"])
        countries_df.loc[i,"Industry"] = cast_to_float(countries_df.loc[i,"Industry"])
        countries_df.loc[i,"Service"] = cast_to_float(countries_df.loc[i,"Service"])

    #Move Mexico from Latin America to North America
    countries_df.loc[countries_df["Country"] == "Mexico", "Region"] = "North America"


    #Replace regions with continents
    countries_df = countries_df.replace(['ASIA (EX. NEAR EAST)         ', 
                          'NEAR EAST                          '],
                        'Asia')

    countries_df = countries_df.replace(['SUB-SAHARAN AFRICA                 ',
                                        'NORTHERN AFRICA                    ',
                                        'C.W. OF IND. STATES '],
                                       'Africa')

    countries_df = countries_df.replace('NORTHERN AMERICA                   ', 'North America')
        
    countries_df = countries_df.replace(['EASTERN EUROPE                     ',
                                         'WESTERN EUROPE                     ',
                                        'BALTICS                            '],
                                        "Europe")
        
    countries_df = countries_df.replace('LATIN AMER. & CARIB    ', "South America")


    countries_df = countries_df.replace('OCEANIA                            ', 'Oceania')


    return countries_df


def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)

    return conn


def create_country(conn, country):
    """
    Create a new country in the countries table
    :param conn: a sqlite3 connection
    :param country:
    :return: row id of inserted country
    """
    sql = """ INSERT INTO countries('Country', 'Region', 'Population', 'Area', 'Pop_Density',
       'Coastline_Ratio', 'Net_Migration', 'Infant_Mortality',
       'GDP_Per_Capita', 'Literacy', 'Phones', 'Arable', 'Crops', 'Other',
       'Climate', 'Birthrate', 'Deathrate', 'Agriculture', 'Industry',
       'Service') VALUES (?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?)
            """
    cur = conn.cursor()
    cur.execute(sql, country)
    conn.commit()
    return cur.lastrowid


def load_database(countries_df):
    '''
    : param : countries_df a cleaned dataframe of countries to be inserted
    into the SQL table countries
    Loads the database with each row of countries_df 
    Throws an error if a country already exists
    '''
    db_file = r"data\countries.db"
    conn = create_connection(db_file)

    #Convert the dataframe to a list of tuples
    countries_tuples = countries_df.to_records(index= False)

    #Add each tuple(row) to the database
    for i in range(0, countries_df.shape[0]):
        id = create_country(conn, countries_tuples[i])
        #print(id)

    conn.close()


