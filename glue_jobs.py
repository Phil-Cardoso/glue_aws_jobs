import os
import boto3

# =====================================================================================
# parametros do job

profile_aws = 'datalake-development'
time_out = 120
number_of_workers = 10
worker_type = 'G.1X'
glue_version = '2.0'
max_retries = 0
role = 'arn:aws:iam::144117821426:role/AWSGlueServiceRole'
max_concurrent_runs = 2


# bucket aonde o arquivo deve ser salvo
my_bucket = ''

# pasta do bucket aonde o arquivo deve ser salvo
# (caso fique em branco sera salvo na raiz)

my_bucket_path = ''

# =====================================================================================
# DefaultArguments

# link para additional_python_modules https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-python-libraries.html
additional_python_modules = 'psycopg2-binary==2.8.6'


conf = 'spark.sql.legacy.parquet.int96RebaseModeInRead=CORRECTED --conf spark.sql.legacy.parquet.int96RebaseModeInWrite=CORRECTED --conf spark.sql.legacy.parquet.datetimeRebaseModeInRead=CORRECTED --conf spark.sql.legacy.parquet.datetimeRebaseModeInWrite=CORRECTED'

# pasta com as libs de python ou def
extra_py_files = 's3://bucket/lib/location_s3.py,s3://bucket/lib/glue_python_libs-0.1-py3-none-any.whl'

# =====================================================================================

# iniciando boto3

session = boto3.Session(profile_name=f'{profile_aws}')

# =====================================================================================

extensao = '.py'

current_file = os.path.basename(__file__)

file_path = ''

# Listando arquivos da pasta

if file_path == '':
    file_path = os.getcwd()
    file_path = file_path + '/'

files_current_dir = os.listdir(path=file_path)

files_py = []

for file in files_current_dir:
    if extensao in file and file != current_file:
        files_py.append(file)
# =====================================================================================

# Listando jobs do glue

aws = session.client('glue')
response = aws.list_jobs(MaxResults=1000)
all_jobs = response['JobNames']
list_jobs = []

for x in all_jobs:
    list_jobs.append(x.lower())

print('Jobs listados')

# list_jobs = [] # caso precise reescrever os jobs descomentar essa linha


# =====================================================================================
s3 = session.resource('s3')
glue = session.client('glue')
for file in files_py:

    if str(file).replace('.py', '') not in 'upload' or str(file).replace('.py', '') not in 'Excel':
        print(file)
        s3.meta.client.upload_file(
            f'{file_path}{file}', my_bucket_path+my_bucket, file)

        # Criando novo job caso nao exista
        if str(file).replace('.py', '') not in list_jobs:

            # Apagando job para subir um novo
            job_name = str(file).replace('.py', '')

            # # Apagando job para subir um novo caso precise reescrever os jobs descomentar essa linha
            # job_name = str(file).replace('.py', '')
            # response = glue.delete_job(JobName=job_name)

            response = glue.create_job(
                Name=job_name,
                Role=role,
                ExecutionProperty={
                    'MaxConcurrentRuns': max_concurrent_runs
                },
                Command={
                    'Name': 'glueetl',
                    'ScriptLocation': f's3://{my_bucket_path}{my_bucket}/{file}',
                    'PythonVersion': '3',
                },
                DefaultArguments={
                    '--additional-python-modules': additional_python_modules,
                    '--conf': conf,
                    '--extra-py-files': extra_py_files
                },
                GlueVersion=glue_version,
                MaxRetries=max_retries,
                Tags={
                    'glue_etl': f'{job_name}'
                },
                Timeout=time_out,
                NumberOfWorkers=number_of_workers,
                WorkerType=worker_type
            )
            print(f'job {file} criado')
