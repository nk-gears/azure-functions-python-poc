import sqlalchemy as db
import pandas as pd
import json


class ODBC:

    # Establish the DB Connection
    def createDBConnector(self, dbType, username, password, hostName, portNo, dbName):
        try:
            dbConnectionString = self.getDBConnectionString(dbType)
            dbConnectionString = dbConnectionString + username + \
                ":" + password + "@"+hostName + ":" + portNo+"/"+dbName
            engine = db.create_engine(
                dbConnectionString, encoding='utf-8', echo=False)
            return engine
        except Exception as e:
            # log the exception
            # raise exception.StopETLException
            pass

    # Read the given csv file and push it to the database
    def pushToTarget(self, dbType, username, password, hostName, portNo, dbName, tableName, fileLocation):
        try:
            engine = self.createDBConnector(
                dbType, username, password, hostName, portNo, dbName)
            df = pd.read_csv(fileLocation)
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
    def readFromSource(self, dbType, username, password, hostName, portNo, dbName, tableName, fileTargetLocation):
        try:
            engine = self.createDBConnector(
            dbType, username, password, hostName, portNo, dbName)
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
    def getDBConnectionString(self, dbEngineProvider):
        try:
            with open('./odbc/odbctypes.json') as odbcTypes:
                data = json.load(odbcTypes)
                return data[dbEngineProvider.upper()]
        except Exception as e:
            # log the exception
            # raise exception.StopETLException
            pass


obj = odbc()
# SQLite Set up a db before execute
# obj.readFromSource("Sqlite", "demo", "testing", "host",
#                    "3306", "demo", "COMPANY", "./odbc/fromdb.csv")
# obj.pushToTarget("sqlite:///C:\\sqlite\\demo.db", "demo", "testing", "host",
#                  "3306", "demo", "COMPANY", './odbc/todb.csv')


# My SQL
obj.readFromSource("mysql", "manisoft", "Password1!", "db4free.net",
                   "3306", "siteservedpoc", "employee", "./odbc/fromdb.csv")
obj.pushToTarget("mysql", "manisoft", "Password1!", "db4free.net",
                 "3306", "siteservedpoc", "employee", "./odbc/todb.csv")

# Postgres SQL
# obj.readFromSource("POSTGRESQL", "ckgwdcet", "Lcn_FsBNiyq3xgzp35UH5y4-sY4SwzbH", "raja.db.elephantsql.com",
#                    "5432", "ckgwdcet", "COMPANY", "./odbc/fromdb.csv")
# obj.pushToTarget("POSTGRESQL", "ckgwdcet", "Lcn_FsBNiyq3xgzp35UH5y4-sY4SwzbH", "raja.db.elephantsql.com",
#                  "5432", "ckgwdcet", "COMPANY", "./odbc/todb.csv")
