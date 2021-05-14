from load_to_table import LoadData
def load_thread_function(data):
    conn = data['engine'].connect()
    load_data = LoadData(data['county'],data['data_to_load'],conn)
    load_data.load_data_to_table()
