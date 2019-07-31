
import logging

import azure.functions as func
import pandas as pd
ls=["name","age"]
df=pd.DataFrame(ls)


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
        try:
            from .. SharedCode import StopWatch
            with StopWatch() as sw:
                t=("[*] Elapsed: {0:.2f}s".format(sw.elapsed_s))
                
        except Exception as ex:
            return func.HttpResponse(f"Hello {name} {ex}")
        else:
            return func.HttpResponse(f"Hello {name} ")
    else:
        return func.HttpResponse(
             "Please pass a name on the query string or in the request body",
             status_code=400
        )

