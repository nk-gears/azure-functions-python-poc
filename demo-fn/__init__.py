import logging
from stopwatch import StopWatch
import azure.functions as func
import pandas as pd
#ls=["name","age"]
#df=pd.DataFrame(ls)
from azure.storage.blob import BlockBlobService
accountey="MkCyzS20s5n8LP5ukutsmNjdnenV6iVaAAJEDXc9xb3xWidAcfCpvhFiovAcdYCOReLkQjSzxb7w7D3W5wXq2Q=="
accountName="scifunctionapp"
containerName="democontainer"

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')


    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if name:
        with StopWatch() as sw:
            try:
                df =  pd.read_csv(pd.compat.StringIO(((BlockBlobService(account_name=accountName,account_key=accountey)).get_blob_to_text(containerName,"demodata.csv")).content),encoding='utf-8',error_bad_lines=False)
                x=df.head()
                tiime=("[*] Elapsed: {0:.2f}s ".format(sw.elapsed_s))
            except Exception as e:
                return func.HttpResponse(f"Hello  The error is {e}!")
            else:
                return func.HttpResponse(f"Hello {x} THE TIME TAKEN IS {tiime}!")


    else:
        return func.HttpResponse(
             "Please pass a name on the query string or in the request body",
             status_code=400
        )
