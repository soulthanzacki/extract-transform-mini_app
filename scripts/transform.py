import pandas as pd
import os
import threading
import argparse

class Base:
    def __init__(self, year):
        self.year = year
        scriptDir = os.path.dirname(os.path.realpath(__file__))
        self.inputDirectory = os.path.join(scriptDir, '..', fr'input/{year}')
        self.outputDirectory = os.path.join(scriptDir, '..', fr'output/{year}')
        if not os.path.exists(self.outputDirectory):
            os.makedirs(self.outputDirectory)

class FileLoader(Base):
    def __init__(self, year):
        super().__init__(year)
    
    def check(self):
        month = 1
        while month <= 12:
            sourceDate = f"{self.year}-{str(month).zfill(2)}"
            existedSource = os.path.exists(fr"{self.inputDirectory}/green_tripdata_{sourceDate}.parquet") and os.path.exists(fr"{self.inputDirectory}/yellow_tripdata_{sourceDate}.parquet")
            existedOutput = os.path.exists(fr"{self.outputDirectory}/processed_green_tripdata_{sourceDate}.csv") and os.path.exists(fr"{self.outputDirectory}/processed_yellow_tripdata_{sourceDate}.csv")
            if existedSource and not existedOutput:
                return [fr"{self.inputDirectory}/yellow_tripdata_{sourceDate}.parquet", fr"{self.inputDirectory}/green_tripdata_{sourceDate}.parquet"]
            else:
                month += 1
                continue
                
    
    def load(self, source):
        if os.path.exists(source):
            df = pd.read_parquet(source, engine="pyarrow")
            pickup_datetime = 'lpep_pickup_datetime' if 'lpep_pickup_datetime' in df.columns else 'tpep_pickup_datetime'
            dropoff_datetime = 'lpep_dropoff_datetime' if 'lpep_dropoff_datetime' in df.columns else 'tpep_dropoff_datetime'
            df = df.rename(columns={pickup_datetime: 'pickup_datetime', dropoff_datetime: 'dropoff_datetime'})

            return df[['VendorID', 'pickup_datetime', 'dropoff_datetime', 'passenger_count' ,'PULocationID', 'DOLocationID', 'trip_distance', 'fare_amount', 'tip_amount']]

class Transformer(Base):
    def __init__(self, year):
        super().__init__(year)
        self.loader = FileLoader(year)
    
    def prepare(self, source):
        if os.path.exists(source):
            df = self.loader.load(source)
            df = df.drop_duplicates().reset_index(drop=True)

            df['passenger_count'] = df['passenger_count'].fillna(1).astype(int)
            df = df[(df['trip_distance'] != 0) & (df['trip_distance'] <= 100)]

            df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
            df['dropoff_datetime'] = pd.to_datetime(df['dropoff_datetime'])

            df['trip_duration'] = (df['dropoff_datetime'] - df['pickup_datetime']).dt.total_seconds() / 60
            df['trip_duration'] = df['trip_duration'].astype(int)

            return df
    
    def transform(self, source):
        fileName = f"{os.path.basename(source).split('.')[0]}"
        if os.path.exists(source):
            print(f"Transforming Data '{fileName}'...")
            df = self.prepare(source)
            dfAgg = df.groupby('VendorID').agg({
                                        'pickup_datetime' : 'count',
                                        'passenger_count' : 'sum',
                                        'trip_duration' : 'sum',
                                        'PULocationID' : lambda x: x.mode().iloc[0],
                                        'DOLocationID' : lambda x: x.mode().iloc[0],
                                        'trip_distance' : 'sum',
                                        'fare_amount' : 'sum',
                                        'tip_amount' : 'mean'
                                        }).rename(columns={'passenger_count' : 'sum_passenger',
                                                        'trip_distance' : 'sum_distance (km)',
                                                        'fare_amount': 'sum_fare_amount',
                                                        'tip_amount' : 'mean_tipAmount',
                                                        'PULocationID' : 'mode_PULocationID',
                                                        'DOLocationID' : 'mode_DOLocationID',
                                                        'trip_duration' : 'total_trip_duration (min)',
                                                        'pickup_datetime' : 'trip_count'
                                                        })

            outputFile = f"processed_{os.path.basename(source).split('.')[0]}.csv"
            dfAgg.to_csv(os.path.join(self.outputDirectory, outputFile))
            print(f"Data '{fileName}' Transformed.")
        else:
            print(f'Source file "{fileName}.parquet" is not exist.')

    def procces(self):
        sourceFile = FileLoader(self.year).check()
        if sourceFile:
            threads = []
            for source in sourceFile:
                t = threading.Thread(target=self.transform, args=(source,))
                threads.append(t)
                t.start()

            for t in threads:
                t.join()
        else:
            print("No more data to be Processed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("year", type=int, help="The year of trip data to download (Example: 2023).")
    args = parser.parse_args()

    transform = Transformer(args.year)
    transform.procces()