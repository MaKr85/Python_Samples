########### Python Form Recognizer Async Analyze #############
import json
import time
from requests import get, post



def analyzeForm(model_id,endpoint,apim_key,file_type,data_bytes):

    params = {
    "includeTextDetails": True
}

    headers = {
    # Request headers
    'Content-Type': file_type,
    'Ocp-Apim-Subscription-Key': apim_key,
}

    post_url = endpoint + "/formrecognizer/v2.1/custom/models/"+model_id+"/analyze"


    try:
        resp = post(url = post_url, data = data_bytes, headers = headers, params = params)
        if resp.status_code != 202:
            print("POST analyze failed:\n%s" % json.dumps(resp.json()))
            quit()
        #print("POST analyze succeeded:\n%s" % resp.headers)
        get_url = resp.headers["operation-location"]
    except Exception as e:
        print("POST analyze failed:\n%s" % str(e))
        quit()



    n_tries = 15
    n_try = 0
    wait_sec = 5
    max_wait_sec = 60
    while n_try < n_tries:
        try:
            resp = get(url = get_url, headers = {"Ocp-Apim-Subscription-Key": apim_key})
            resp_json = resp.json()
            if resp.status_code != 200:
                print("GET analyze results failed:\n%s" % json.dumps(resp_json))
                quit()
            status = resp_json["status"]
            if status == "succeeded":
                #print("Analysis succeeded:\n%s" % json.dumps(resp_json))
                #quit()
                n_try=15
            if status == "failed":
                print("Analysis failed:\n%s" % json.dumps(resp_json))
                quit()
            # Analysis still running. Wait and retry.
            print("Analysis still running. Wait and retry. n_try="+str(n_try))
            time.sleep(wait_sec)
            n_try += 1
            wait_sec = min(2*wait_sec, max_wait_sec)
        except Exception as e:
            msg = "GET analyze results failed:\n%s" % str(e)
            print(msg)
            quit()
    #print("Analyze operation did not complete within the allocated time.")

    return resp_json