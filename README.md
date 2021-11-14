True Layer Assignment:
1. Please refer to requirements.txt for viewing the libraries installed in the venv.
2. For running truelayer_movies.py in venv please "path to cloned github repository" + \TrueLayer\venv\Scripts\python.exe  "path to cloned github repository" + \TrueLayer\truelayer_movies.py"

The file true_layer movies identifies the code necessary for the assignment. The only thhing to do for being able to rund the code is specify the path for the csv file containing movie metadata (line 20 or path_metadata) and the xml file (line 19 or path wiki) inside true_layer_movies.py 

Steps:
1. create a movie_metadata dataframe
2. calculate ratio Budget/revenue
3. Extract year from date field, in this case all the missing fields were filled with "1900/01/01"
4.I tried to parse the production_companies fields but did not have much time to correcly parse it given all the messy details that the column presents. So, I decided to focus on the other parts of the assignment.
5. Parse the xml file using Element tree parser, taking field title, url, and abstracts of only the records thaat had an anchor tag = 'Film' or 'film'. 
6. Create a CSV file from XML file only with the fields that are from our interest, namely Title, Abstract, and URL
7. Create a dataframe from Csv file 
8.  Created a key from   title field to be able to join two dataframes, removing the spaces and turning all the characters to lowercase in the title field.
9. In some cases,for some  the revenue generated is equal to 0, making the ratio value infinite. I did not considered these cases becasuse it does not provide a insighful information about the movie's performance, which I think is one of the main goal of the assignment
10.  Created connector with POSTGRESQL database using psycopg2 and sql alchemy for pushing the final dataframe (df movies joined with df_wiki) to my POSTGRESQL instance.
10.  Then queried my newly created table to make sure that it was populated
