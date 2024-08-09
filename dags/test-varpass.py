from airflow.decorators import dag, task


@dag(schedule=None)
def taskflow():
    @task(task_id="extract", retries=2)
    def extract_bitcoin_price():
        print("first")
        return "hello"
        
    @task(multiple_outputs=True)
    def process_data(word):
        print("received"+word)
 
    process_data(extract_bitcoin_price())
    


taskflow()