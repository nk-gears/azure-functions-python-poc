import sqlalchemy as db
import pandas as pd
import json


class ODBC:

    # Establish the DB Connection
    def createDBConnector(self, resourceList, resourceName):
        try:
            dbConnectionString = self.generateDBConnectionString(
                resourceList, resourceName)
            engine = db.create_engine(
                dbConnectionString, encoding='utf-8', echo=False)
            return engine
        except Exception as e:
            # log the exception
            # raise exception.StopETLException
            pass

    # Read the given csv file and push it to the database
    def pushToTarget(self, resourceList, resourceName, tableName, dateframe):
        try:
            engine = self.createDBConnector(resourceList, resourceName)
            df = pd.read_csv(dateframe)
            self.getTableMetadata(engine, tableName, df)
            df.to_sql(name=tableName, if_exists='append',
                      con=engine, index=False)
        except Exception as e:
            # log the exception
            # raise exception.StopETLException
            pass
        finally:
            engine.dispose()

    # Read the database table and write to the csv file
    def readFromSource(self, resourceList, resourceName, tableName, fileTargetLocation):
        try:
            engine = self.createDBConnector(resourceList, resourceName)
            connection = engine.connect()
            metadata = db.MetaData(engine, reflect=True)
            table = db.Table(tableName, metadata, autoload=True,
                             autoload_with=engine)
            query = db.select([table])
            resultSet = connection.execute(query).fetchall()
            df = pd.DataFrame(resultSet)
            print(df.head(10))
            df = df.head(50)
            df.to_csv(fileTargetLocation, index=False,
                      encoding='utf-8', header=False)
            return df
        except Exception as e:
            # log the exception
            # raise exception.StopETLException
            pass
        finally:
            engine.dispose()

    # Validate the table metadata with the csv metadata
    def getTableMetadata(self, engine, tableName, df):
        try:
            metadata = db.MetaData(engine, reflect=True)
            # Check for the Table existence
            if not tableName in metadata.tables:
                print("Table is not found")
                sys.exit(0)
            table = db.Table(tableName, metadata, autoload=True,
                             autoload_with=engine)
            # Check for the table column metadata
            for columnName in table.columns.keys():
                if not columnName in df.columns:
                    print("Table column name is mismatched")
                    sys.exit(1)
        except Exception as e:
            # log the exception
            # raise exception.StopETLException
            pass
        # Need to check table column data type

    # Get the DB dialect uri based on the db providers
    def getDBDialectString(self, dbEngineProvider):
        try:
            with open('./site-served-poc/odbc/odbctypes.json') as odbcTypes:
                data = json.load(odbcTypes)
                return data[dbEngineProvider.upper()]
        except Exception as e:
            # log the exception
            # raise exception.StopETLException
            pass

    def generateDBConnectionString(self, resourceDetailsList, resourceName):
        # On live, resource details list will be passed as parameter to the function. For now reading it from config.json
        with open('./site-served-poc/odbc/config.json') as odbcTypes:
            data = json.load(odbcTypes)
            resourceList = data['resources']
        for index in range(len(resourceList)):
            if resourceName in resourceList[index]['name']:
                sourceList = resourceList[index]
                dialectString = self.getDBDialectString(
                    sourceList['databaseProvider'])
                dbConnectionString = dialectString + sourceList['credentials']['username'] + \
                    ":" + sourceList['credentials']['password'] + "@"+sourceList['credentials']['hostname'] + \
                    ":" + sourceList['credentials']['port'] + \
                    "/"+sourceList['dbName']
                return dbConnectionString


obj = ODBC()
# SQLite Set up a db before execute
# obj.readFromSource("Sqlite", "demo", "testing", "host",
#                    "3306", "demo", "COMPANY", "./site-served-poc/odbc/fromdb.csv")
# obj.pushToTarget("sqlite:///C:\\sqlite\\demo.db", "demo", "testing", "host",
#                  "3306", "demo", "COMPANY", './site-served-poc/odbc/todb.csv')

# My SQL
obj.readFromSource("", "demo_mysql_odbc", "employee",
                 "./site-served-poc/odbc/fromdb.csv")
obj.pushToTarget("", "demo_mysql_odbc", "employee",
                 "./site-served-poc/odbc/todb.csv")