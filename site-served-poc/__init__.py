import logging

import azure.functions as func

import pandas as pd
#import os
from azure.storage.blob import BlockBlobService
accountey="MkCyzS20s5n8LP5ukutsmNjdnenV6iVaAAJEDXc9xb3xWidAcfCpvhFiovAcdYCOReLkQjSzxb7w7D3W5wXq2Q=="
accountName="scifunctionapp"
containerName="democontainer"


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('execute')     #query parameter
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if (name=="true"):
        df =  pd.read_csv(pd.compat.StringIO(((BlockBlobService(account_name=accountName,account_key=accountey)).get_blob_to_text(containerName,"demodata.csv")).content),encoding='utf-8',error_bad_lines=False)
        x=df['Region'].head()
        #df.to_csv('df.txt',index = False, encoding='utf-8')
        #final_file = os.path.join(os.getcwd(),'df.txt')
        #BlockBlobService(account_name=accountName,account_key=accountey).create_blob_from_path(containerName,"df.csv",final_file)
        #print(final_file)
        return func.HttpResponse(f"Hello {name}! Program Executed !!Data=={x}")
    else:
        return func.HttpResponse(
             "Please pass a name on the query string or in the request body",
             status_code=400
        )
