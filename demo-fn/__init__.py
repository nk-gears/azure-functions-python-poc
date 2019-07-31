import logging

import azure.functions as func
import pandas as pd
#ls=["name","age"]
#df=pd.DataFrame(ls)
from azure.storage.blob import BlockBlobService
accountey="MkCyzS20s5n8LP5ukutsmNjdnenV6iVaAAJEDXc9xb3xWidAcfCpvhFiovAcdYCOReLkQjSzxb7w7D3W5wXq2Q=="
accountName="scifunctionapp"
containerName="democontainer"
df =  pd.read_csv(pd.compat.StringIO(((BlockBlobService(account_name=accountName,account_key=accountey)).get_blob_to_text(containerName,"creditcard.csv")).content),encoding='utf-8',error_bad_lines=False)
x=df.head()


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
        return func.HttpResponse(f"Hello {name} {x}!")
    else:
        return func.HttpResponse(
             "Please pass a name on the query string or in the request body",
             status_code=400
        )
