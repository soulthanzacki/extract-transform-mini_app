import requests as rq
import os
import threading
import argparse

class Base:
    def __init__(self, year):
        self.year = year
        scriptDir = os.path.dirname(os.path.realpath(__file__))
        self.inputDirectory = os.path.join(scriptDir, '..', fr'input/{year}')
        if not os.path.exists(self.inputDirectory):
            os.makedirs(self.inputDirectory)

class SourceGenerator(Base):
    def __init__(self, year, baseURL="https://d37ci6vzurychx.cloudfront.net/trip-data/"):
        super().__init__(year)
        self.baseURL = baseURL
    
    def generateSource(self):
        month = 1
        while month <= 12:
            sourceDate = f"{self.year}-{str(month).zfill(2)}"
            if os.path.exists(fr"{self.inputDirectory}/green_tripdata_{sourceDate}.parquet") and os.path.exists(fr"{self.inputDirectory}/yellow_tripdata_{sourceDate}.parquet"):
                month += 1
                continue
            else:
                return [f"{self.baseURL}yellow_tripdata_{sourceDate}.parquet", f"{self.baseURL}green_tripdata_{sourceDate}.parquet"]

class Extractor(Base):
    def __init__(self, year):
        super().__init__(year)
        self.source = SourceGenerator(year)

    def fileDownload(self, source):
        fileName = os.path.basename(source)
        filePath = os.path.join(self.inputDirectory, fileName)

        if not os.path.exists(filePath):
            print(f"Downloading '{fileName}'...")
            try:
                response = rq.get(source)
                response.raise_for_status()

                with open(filePath, "wb") as file:
                    file.write(response.content)
                print(f"'{fileName}' downloaded.")

            except rq.exceptions.RequestException as e:
                print(f"An error happened while downloading file: '{e}'")

            except Exception as e:
                print(f"Error: '{e}'")
        else:
            print(f"Source file '{fileName}' already exists.")

    def extract(self):
        sourceURL = self.source.generateSource()
        if sourceURL:
            threads = []
            for source in sourceURL:
                t = threading.Thread(target=self.fileDownload, args=(source,))
                threads.append(t)
                t.start()

            for t in threads:
                t.join()
        else:
            print("No more data to be Downloaded.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("year", type=int, help="The year of trip data to download (Example: 2023).")
    args = parser.parse_args()

    extract = Extractor(args.year)
    extract.extract()