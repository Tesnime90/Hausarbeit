import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy import MetaData
from sklearn.metrics import mean_squared_error
import matplotlib.pyplot as plt
from math import sqrt


class Database:
    def __init__(self, database, url):
        self.database = database
        self.url = url
        # Create an Engine to connect the database
        self.engine = create_engine(self.url)
        # Establish the connection to Mysql database
        self.sql_conn = self.engine.connect()


class LoadData(Database):
    """
    to load the Data from Mysql it's demanded to provide:
    Database name = database
    URL link = url
    the name of the frame to load = frame.
    """

    def __init__(self, database, url):
        super().__init__(database, url)

    def load(self, frame):
        """  load the required frame """
        frame = pd.read_sql('select * from ' + frame, self.sql_conn)
        return frame


class CalculateBestFit:
    """ Calculate the best fit function for a given Training one. for every
    ideal function and a Training function, an RMSE factor is calculated and
    at the end, the little factor is representing the best fit function.
    ytr is the Training function Dataframe
    tab_ideal is the ideal functions Data Frame
    K is the List with all RMSE calculated """

    def __init__(self, ytr, tab_ideal):
        self.ytr = ytr
        self.tab_ideal = tab_ideal
        self.k = []

    def best_fit_index(self):
        """ this function calculates the factor RMSE and put it in the List K
        then return the Whole list to can find the index of the best fit
        function """

        for key, value in self.tab_ideal.iloc[:, 1:].items():
            rmse = sqrt(mean_squared_error(self.ytr, value))
            self.k.append(rmse)
        return self.k


class CalculateDistance:
    """
            df1 = Dataframe 1 that contains function y1
            df2 = Dataframe 2 that contains function y2
            y1 and y2 = name of the functions (string)
            returns a List with that represents the
            distance between y1 and y2
            """

    def __init__(self, df1, y1, df2, y2):
        self.df1 = df1
        self.y1 = y1
        self.df2 = df2
        self.y2 = y2
        # create a new Dataframe as an inner join of df1 and df2
        self.join_data = pd.merge(self.df1[['x', self.y1]],
                                  self.df2.loc[:, ['x', y2]],
                                  how='inner', on=['x'])
        self.delta = []

    def distance(self):
        for Index, Row in self.join_data.iterrows():
            d = abs(Row[self.y1] - Row[self.y2])
            self.delta.append(d)
        return self.join_data.assign(delta=self.delta)


class SaveData(Database):
    """
    save the final Tables of results
    """

    def __init__(self, database, url, dataframe, name):
        super().__init__(database, url)
        self.dataframe = dataframe
        self.url = url
        self.name = name
        dataframe.to_sql(name=name, con=self.engine, if_exists='replace',
                         index=False)


# Create Metadata
hausarbeit = MetaData()
# Create URL of the Database
url_object = URL.create('mysql+pymysql', username='root',
                        password='Tesnimoez90Python', host='localhost',
                        database='hausarbeit')
# Create an Objekt Sql_data of Database class to use it for loading the tables
Sql_data = LoadData(hausarbeit, url_object)
# read the Test Table from CSV File and save it as Dataframe
Test = pd.read_csv(r"C:\Users\tesni\OneDrive\Desktop\Hausarbeit\test.csv")
# load the Training Table
Training = Sql_data.load('training')
# load the Ideal table
Ideal = Sql_data.load('ideal')
# define multiple figures for multiple plots
# 4 figures
# Ech figure shows a Training function with its best fit one and its test points
fig, ax = plt.subplots(nrows=2, ncols=2, figsize=(12, 7))
# x Axis for all plots
x = Training['x']
# Print the best fit functions for all 4 Trainings functions
# search the best points that corresponds each Ideal function
# save the results in Tables in Mysql
# Draw all results
# Using for Loop
for i in range(1, 5):
    # y is the best fit function name: string
    # N is an Objekt of CalculateBestFit class
    N = CalculateBestFit(Training.iloc[:, i], Ideal)
    N = N.best_fit_index()
    y = 'y' + str(N.index(min(N)) + 1)
    # The highest delta between Ideal and Training functions is N_max
    N_max = min(N)
    # M is a Dataframe that contains the difference between y test points and y
    # Ideal points
    M = CalculateDistance(Test, 'y', Ideal, y)
    M = M.distance()
    # this for Loop is used to search only the test points that satisfy our
    # criteria then create and save a new Dataframe with these points.
    for index, row in M.iterrows():
        if row['delta'] > N_max * sqrt(2):
            M.drop(index, inplace=True)
    s = SaveData(hausarbeit, url_object, M, 'test_ideal_{}'.format(i))
    if i >= 3:
        ax[1, i - 3].scatter(x, Training['y{}'.format(i)], s=2,
                             label='y{}'.format(i))
        ax[1, i - 3].plot(x, Ideal[y], label='fit_{}'.format(i),
                          color='orange', linewidth=1)
        ax[1, i - 3].scatter(M['x'], M['y'],
                             label='Test_Ideal_Points', color='k', s=15)
        ax[1, i - 3].legend()
    else:
        ax[0, i - 1].scatter(x, Training['y{}'.format(i)], s=2,
                             label='y{}'.format(i))
        ax[0, i - 1].plot(x, Ideal[y], label='fit_{}'.format(i),
                          color='orange', linewidth=1)
        ax[0, i - 1].scatter(M['x'], M['y'],
                             label='Test_Ideal_Points', color='k', s=15)
        ax[0, i - 1].legend()
    print(
        'The best fit of the training function y{}'.format(i), 'is ' + y)
print('\n all 4 Tables of test_ideal_i results are saved in Mysql Database')
Sql_data_1 = LoadData(hausarbeit, url_object)
test_ideal_1 = Sql_data_1.load('test_ideal_1')
print('\n test_ideal_1 Table: \n', test_ideal_1)
test_ideal_2 = Sql_data_1.load('test_ideal_2')
print('\n test_ideal_2 Table: \n', test_ideal_2)
test_ideal_3 = Sql_data_1.load('test_ideal_3')
print('\n test_ideal_3 Table: \n', test_ideal_3)
test_ideal_4 = Sql_data_1.load('test_ideal_4')
print('\n test_ideal_4 Table: \n', test_ideal_4)
plt.show()
