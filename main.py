from src.data_ingestor.data_ingestion import StockDataIngestionPipeline

def main():
    print("Hello from automated-stockmarket-datapipeline!")
    pipeline = StockDataIngestionPipeline()
    
    # Run for all configured tickers
    # ticker = ['TCS.NS']
    pipeline.run()





if __name__ == "__main__":
    main()
