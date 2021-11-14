import numpy as np
from numpy import e
import psycopg2
from datetime import datetime
import sys, pandas as pd
import xml.etree.cElementTree as ET
import pandas as pd
import csv
from sqlalchemy import create_engine
    
if __name__ == '__main__':
    date = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
    print(date,'program starting')
    print(date, 'try connecting...')


    try:
        # paths to files
        print(date, "Getting path files...")
        path_wiki = "path to xml file""
        path_metadata = " path to movies_metadata.csv"
        print(date, "Correctly got {} and {} path files".format(path_wiki, path_metadata))

        #Creating Pandas Dataframe for movies
        print(date, "Creating pandas Dataframe for {} file".format(path_metadata))
        df_metadata = pd.read_csv(path_metadata,low_memory= False)
        print(date, "Successfully created Dataframe for {} file".format(path_metadata))
        #Creating column for ratio budget/revenue
        print(date, "Creating column budget/revenue ratio for {} file".format(path_metadata))
        df_metadata['revenue'] = df_metadata['revenue'].astype(float)
        df_metadata['budget'] = df_metadata['budget'].astype(float)
        df_metadata['ratio'] = df_metadata['budget']/df_metadata['revenue']
        print(date, "Successfully created column budget/revenue ratio for {} file".format(path_metadata))


        print(date, "Exctracting Year from column release_date from df_meatada")
        df_metadata['release_date'] = df_metadata['release_date'].fillna('1999/01/01')
        df_metadata['release_date'] = pd.to_datetime(df_metadata['release_date'],errors='coerce')
        df_metadata['year'] = df_metadata['release_date'].dt.year
        print(date, "Successfully extracted year from column release_date from df_meatada")

        print(date, "Removing spaces and seeting title to lowercase in df_metadata")
        df_movies = df_metadata[['title', 'budget','year', 'revenue', 'vote_average', 'ratio', 'production_companies']]
        df_movies['key'] = df_movies['title'].astype(str)
        df_movies['key'] = df_movies['key'].str.replace(" ", "")
        df_movies['key'] = df_movies['key'].str.lower()
        print(date, "Successfully removed spaces and set title to lowercase in df_metadata")
        
        #Getting company names from production_companies column. Tried to extract the companies from this function.
        for key, value in df_movies['production_companies'].iteritems():
            a = str(value)
            if len(a) > 2:
                b =a.split(',')
                for x in range(len(b)):
                    if x%2 !=0:
                        pass
                    else:
                        company = b[x]
            else:
                company =""
        df_movies['company'] = company


        #Parsing XML file, I decided to parse this file and create a new csv file only with the info that is necessary.
        print("Parsing XML {} file".format(path_wiki))
        film=""
        tree = ET.parse(path_wiki)
        root = tree.getroot()
        try:
            for x in root.iter("doc"):
                if 'film' in str(x[3][0][0].text) or 'Film' in str(x[3][0][0].text):
                    title = str(x[0].text).split(":")[1]
                    abstract = str(x[2].text)
                    if x[3][0][1]:
                        link = str(x[3][0][1].text)
                    else:
                        link = ""
                    film = film + title + "~" + abstract + "~" + link +"\n"
                    fname  =r"csvfile.csv"
                    with open(fname,"w", encoding="utf-8") as file:
                        writer = csv.writer(file)
                        file.write(film)

        except Exception as e:
            print ("Errot: ",e)
            pass
        path_csv =sys.path[0]+"\csvfile.csv"
        print("Successfully parsed and created CSV file {}".format(fname))  

        print(date, "Creating dataframe from parsed XML file ")
        df_wiki = pd.read_csv(path_csv,low_memory= False,sep="~")
        df_wiki['key'] = df_wiki['title'].astype(str)
        df_wiki['key'] = df_wiki['key'].str.replace(" ", "")
        df_wiki['key'] = df_wiki['key'].str.lower()
        df_final = df_movies.merge(df_wiki, on='key', how='left')

        df_final = df_final.sort_values(by='ratio', ascending=False)
        df_final.replace(np.inf,0, inplace=True)
        df_final = df_final.head(1000)
        df_final['budget'].astype(int)
        df_final.to_csv('df_final.csv',sep="~")


        try:
            print(date, "trying to connect to POSTGRE")
            conn_string = "host='localhost' dbname='postgres' user='postgres' password='pp228250'"
            conn = psycopg2.connect(conn_string)
            print(date, "connected to POSTGRE SQL ")
            cursor = conn.cursor()
            #cursor.execute(query)
            print("Creating table based on dt_final...")
            engine = create_engine('postgresql://postgres:pp228250@localhost:5432/postgres')
            df_final.to_sql('true_layer_assignment', engine)
            #pd.io.sql.write_frame(df_final, 'table_name', conn, flavor='postgresql')
            cursor.execute("select * from public.true_layer_assignment")
            print("Fetching query results...")
            print (cursor.fetchall())
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: ", error)

        



    except Exception as e:
        print('error:',e)
