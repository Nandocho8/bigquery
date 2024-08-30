from google.cloud import storage
from google.cloud import bigquery

# Funcion para subir un archivo a un bucket, 
#recibe el <nombre del bucket>, el nombre del <archivo local> y el nombre del <archivo en el bucket>
def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    # variable que contiene el cliente de storage con las credenciales del archivo json
    storage_client = storage.Client.from_service_account_json('c:/BIGDATA/bigmama-421901-37423eb748fa.json')
    # variable que contiene el bucket al que se va a subir el archivo usando el <nombre del bucket>
    bucket = storage_client.bucket(bucket_name)
    # variable que contiene el blob que se va a subir al bucket con el nombre del <archivo en el bucket>
    blob = bucket.blob(destination_blob_name)

    # sube el archivo al bucket desde el <archivo local>
    blob.upload_from_filename(source_file_name)

    # imprime un mensaje de que el archivo se subio correctamente
    print(f"File {source_file_name} uploaded to {destination_blob_name}.")

# Funcion para cargar datos a bigquery desde un bucket,
# recibe el <nombre del dataset>, el <nombre de la tabla>, <bucket de origen> y el <nombre del archivo en el bucket>
def load_data_from_bucket(dataset_id, table_id, source_bucket, source_file_name):
    
    # variable que contiene el cliente de bigquery con las credenciales del archivo json y el proyecto
    bigquery_client = bigquery.Client.from_service_account_json('c:/BIGDATA/bigmama-421901-37423eb748fa.json', project='bigmama-421901')
    
    # verificar si existe el dataset y la tabla
    try:
        # verifica si existe el dataset
        bigquery_client.get_dataset(dataset_id)
        print(f"Dataset {dataset_id} already exists")
    except Exception as e:
        # si no existe el dataset imprime un mensaje y finaliza la funcion
        print(f"Dataset {dataset_id} is not found")
        return
    try:
        # verifica si existe la tabla
        bigquery_client.get_table(table_id)
        print(f"Table {table_id} already exists")
    except Exception as e:
        # si no existe la tabla imprime un mensaje y finaliza la funcion
        print(f"Table {table_id} is not found")
        return
    
    # verifica si existe el archivo en el bucket
    try:
        # variable que contiene el cliente de storage con las credenciales del archivo json
        storage_client = storage.Client.from_service_account_json('c:/BIGDATA/bigmama-421901-37423eb748fa.json')
        # variable que contiene el bucket al que se subio el archivo usando el <nombre del bucket>
        bucket = storage_client.bucket(source_bucket)
        # variable que contiene el blob que se subio al bucket con el nombre del <archivo en el bucket>
        blob = bucket.blob(source_file_name)

    except Exception as e:
        # si no existe el archivo imprime un mensaje y finaliza la funcion
        print(f"Blob {source_file_name} is not found")
        return

    # imprime un mensaje con el nombre del bucket, el nombre del archivo en el bucket
    print(f'Bucket {bucket} contains blob file {blob}')


    # configuracion del job para cargar los datos a bigquery    
    job_config = bigquery.LoadJobConfig()
    # establece formato del archivo
    job_config.source_format = bigquery.SourceFormat.CSV
    # establece el numero de filas a saltar
    job_config.skip_leading_rows = 1
    # autodetecta el esquema de la tabla
    job_config.autodetect = True

    # abre el archivo en modo lectura binario
    with blob.open("rb") as source_file:
        # variable que contiene el job para cargar los datos a bigquery desde el archivo en el bucket a la tabla con la configuracion del job 
        job = bigquery_client.load_table_from_file(
            source_file, table_id, job_config=job_config
        )

    # espera a que el job se complete
    job.result()  # Waits for the job to complete

    print(f"Loaded {job.output_rows} rows into {dataset_id}:{table_id}.")

# Funcion principal
def main():
    # variables con los nombres del bucket, el archivo local, el archivo en el bucket, el dataset y la tabla en bigquery y el archivo en el bucket con el formato gs://<nombre del bucket>/<nombre del archivo en el bucket>
    bucket_name = "bigpokemon"
    source_file_name = "C:/BIGDATA/pokemon_test.csv"
    destination_blob_name = "poketest.csv"
    dataset_id = 'bigmama-421901.pokemon'
    table_id = "bigmama-421901.pokemon.Test"


    print("Subiendo archivo a bucket y cargando datos a bigquery") 
    upload_blob(bucket_name, source_file_name, destination_blob_name)
    load_data_from_bucket(dataset_id, table_id,bucket_name,destination_blob_name)
    print('ejemplo')

    for i in range(1, 10):
        print(i)
        print(i)
        print(i)
        print(i)
        print(i)
        print(i)
        print(i)

    print('ojala de un merge')
    print('ojala de un merge')
    print('ojala de un merge2')
    print('ojala de un merge3')
    print('ojala de un merge4')

    if __name__ == "__main__":
        main()